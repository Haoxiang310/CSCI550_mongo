/**
 *    Copyright (C) 2022-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/db/commands/cqf/cqf_aggregate.h"

#include "mongo/db/curop.h"
#include "mongo/db/exec/sbe/abt/abt_lower.h"
#include "mongo/db/pipeline/abt/abt_document_source_visitor.h"
#include "mongo/db/pipeline/abt/match_expression_visitor.h"
#include "mongo/db/query/ce/ce_sampling.h"
#include "mongo/db/query/optimizer/cascades/ce_heuristic.h"
#include "mongo/db/query/optimizer/cascades/cost_derivation.h"
#include "mongo/db/query/optimizer/explain.h"
#include "mongo/db/query/optimizer/node.h"
#include "mongo/db/query/optimizer/opt_phase_manager.h"
#include "mongo/db/query/plan_executor_factory.h"
#include "mongo/db/query/query_knobs_gen.h"
#include "mongo/db/query/query_planner_params.h"
#include "mongo/db/query/sbe_stage_builder.h"
#include "mongo/db/query/yield_policy_callbacks_impl.h"

namespace mongo {

using namespace optimizer;

static opt::unordered_map<std::string, optimizer::IndexDefinition> buildIndexSpecsOptimizer(
    boost::intrusive_ptr<ExpressionContext> expCtx,
    OperationContext* opCtx,
    const CollectionPtr& collection,
    const optimizer::ProjectionName& scanProjName,
    const DisableIndexOptions disableIndexOptions) {
    using namespace optimizer;

    if (disableIndexOptions == DisableIndexOptions::DisableAll) {
        return {};
    }

    const IndexCatalog& indexCatalog = *collection->getIndexCatalog();
    opt::unordered_map<std::string, IndexDefinition> result;
    auto indexIterator =
        indexCatalog.getIndexIterator(opCtx, IndexCatalog::InclusionPolicy::kReady);

    while (indexIterator->more()) {
        const IndexCatalogEntry& catalogEntry = *indexIterator->next();

        const bool isMultiKey = catalogEntry.isMultikey(opCtx, collection);
        const MultikeyPaths& multiKeyPaths = catalogEntry.getMultikeyPaths(opCtx, collection);
        uassert(6624251, "Multikey paths cannot be empty.", !multiKeyPaths.empty());

        const IndexDescriptor& descriptor = *catalogEntry.descriptor();
        if (descriptor.hidden() || descriptor.isSparse() ||
            descriptor.getIndexType() != IndexType::INDEX_BTREE) {
            // Not supported for now.
            continue;
        }

        // SBE version is base 0.
        const int64_t version = static_cast<int>(descriptor.version()) - 1;

        uint32_t orderingBits = 0;
        {
            const Ordering ordering = catalogEntry.ordering();
            for (int i = 0; i < descriptor.getNumFields(); i++) {
                if ((ordering.get(i) == -1)) {
                    orderingBits |= (1ull << i);
                }
            }
        }

        IndexCollationSpec indexCollationSpec;
        bool useIndex = true;
        size_t elementIdx = 0;
        for (const auto& element : descriptor.keyPattern()) {
            FieldPathType fieldPath;
            FieldPath path(element.fieldName());

            for (size_t i = 0; i < path.getPathLength(); i++) {
                const std::string& fieldName = path.getFieldName(i).toString();
                if (fieldName == "$**") {
                    // TODO: For now disallow wildcard indexes.
                    useIndex = false;
                    break;
                }
                fieldPath.emplace_back(fieldName);
            }
            if (!useIndex) {
                break;
            }

            const int direction = element.numberInt();
            if (direction != -1 && direction != 1) {
                // Invalid value?
                useIndex = false;
                break;
            }

            const CollationOp collationOp =
                (direction == 1) ? CollationOp::Ascending : CollationOp::Descending;

            // Construct an ABT path for each index component (field path).
            const MultikeyComponents& elementMultiKeyInfo = multiKeyPaths[elementIdx];
            ABT abtPath = make<PathIdentity>();
            for (size_t i = fieldPath.size(); i-- > 0;) {
                if (isMultiKey && elementMultiKeyInfo.find(i) != elementMultiKeyInfo.cend()) {
                    // This is a multikey element of the path.
                    abtPath = make<PathTraverse>(std::move(abtPath));
                }
                abtPath = make<PathGet>(fieldPath.at(i), std::move(abtPath));
            }
            indexCollationSpec.emplace_back(std::move(abtPath), collationOp);
            ++elementIdx;
        }
        if (!useIndex) {
            continue;
        }

        PartialSchemaRequirements partialIndexReqMap;
        if (descriptor.isPartial() &&
            disableIndexOptions != DisableIndexOptions::DisablePartialOnly) {
            auto expr = MatchExpressionParser::parseAndNormalize(
                descriptor.partialFilterExpression(),
                expCtx,
                ExtensionsCallbackNoop(),
                MatchExpressionParser::kBanAllSpecialFeatures);

            ABT exprABT = generateMatchExpression(expr.get(), false /*allowAggExpression*/, "", "");
            exprABT = make<EvalFilter>(std::move(exprABT), make<Variable>(scanProjName));

            // TODO: simplify expression.

            PartialSchemaReqConversion conversion = convertExprToPartialSchemaReq(exprABT);
            if (!conversion._success || conversion._hasEmptyInterval) {
                // Unsatisfiable partial index filter?
                continue;
            }
            partialIndexReqMap = std::move(conversion._reqMap);
        }

        // For now we assume distribution is Centralized.
        result.emplace(descriptor.indexName(),
                       IndexDefinition(std::move(indexCollationSpec),
                                       version,
                                       orderingBits,
                                       isMultiKey,
                                       DistributionType::Centralized,
                                       std::move(partialIndexReqMap)));
    }

    return result;
}

static QueryHints getHintsFromQueryKnobs() {
    QueryHints hints;

    hints._disableScan = internalCascadesOptimizerDisableScan.load();
    hints._disableIndexes = internalCascadesOptimizerDisableIndexes.load()
        ? DisableIndexOptions::DisableAll
        : DisableIndexOptions::Enabled;
    hints._disableHashJoinRIDIntersect =
        internalCascadesOptimizerDisableHashJoinRIDIntersect.load();
    hints._disableMergeJoinRIDIntersect =
        internalCascadesOptimizerDisableMergeJoinRIDIntersect.load();
    hints._disableGroupByAndUnionRIDIntersect =
        internalCascadesOptimizerDisableGroupByAndUnionRIDIntersect.load();
    hints._keepRejectedPlans = internalCascadesOptimizerKeepRejectedPlans.load();
    hints._disableBranchAndBound = internalCascadesOptimizerDisableBranchAndBound.load();

    return hints;
}

static std::unique_ptr<PlanExecutor, PlanExecutor::Deleter> optimizeAndCreateExecutor(
    OptPhaseManager& phaseManager,
    ABT abtTree,
    OperationContext* opCtx,
    boost::intrusive_ptr<ExpressionContext> expCtx,
    const NamespaceString& nss,
    const CollectionPtr& collection) {

    const bool optimizationResult = phaseManager.optimize(abtTree);
    uassert(6624252, "Optimization failed", optimizationResult);

    // TODO: SERVER-62648. std::cerr is used for debugging. Consider structured logging.
    std::cerr << "********* Optimizer Stats *********\n";
    {
        const auto& memo = phaseManager.getMemo();
        std::cerr << "Memo groups: " << memo.getGroupCount() << "\n";
        std::cerr << "Memo logical nodes: " << memo.getLogicalNodeCount() << "\n";
        std::cerr << "Memo phys. nodes: " << memo.getPhysicalNodeCount() << "\n";

        const auto& memoStats = memo.getStats();
        std::cerr << "Memo integrations: " << memoStats._numIntegrations << "\n";
        std::cerr << "Phys. plans explored: " << memoStats._physPlanExplorationCount << "\n";
        std::cerr << "Phys. memo checks: " << memoStats._physMemoCheckCount << "\n";
    }
    std::cerr << "********* Optimizer Stats *********\n";

    std::cerr << "********* Optimized ABT *********\n";
    std::cerr << ExplainGenerator::explainV2(
        make<MemoPhysicalDelegatorNode>(phaseManager.getPhysicalNodeId()),
        true /*displayPhysicalProperties*/,
        &phaseManager.getMemo());
    std::cerr << "********* Optimized ABT *********\n";

    auto env = VariableEnvironment::build(abtTree);
    SlotVarMap slotMap;
    sbe::value::SlotIdGenerator ids;
    SBENodeLowering g{env,
                      slotMap,
                      ids,
                      phaseManager.getMetadata(),
                      phaseManager.getNodeToGroupPropsMap(),
                      phaseManager.getRIDProjections()};
    auto sbePlan = g.optimize(abtTree);

    uassert(6624253, "Lowering failed: did not produce a plan.", sbePlan != nullptr);
    uassert(6624254, "Lowering failed: did not produce any output slots.", !slotMap.empty());

    {
        std::cerr << "********* SBE *********\n";
        sbe::DebugPrinter p;
        std::cerr << p.print(*sbePlan.get()) << "\n";
        std::cerr << "********* SBE *********\n";
    }

    stage_builder::PlanStageData data{std::make_unique<sbe::RuntimeEnvironment>()};
    data.outputs.set(stage_builder::PlanStageSlots::kResult, slotMap.begin()->second);

    sbePlan->attachToOperationContext(opCtx);
    if (expCtx->explain || expCtx->mayDbProfile) {
        sbePlan->markShouldCollectTimingInfo();
    }

    auto yieldPolicy =
        std::make_unique<PlanYieldPolicySBE>(opCtx,
                                             PlanYieldPolicy::YieldPolicy::YIELD_AUTO,
                                             opCtx->getServiceContext()->getFastClockSource(),
                                             internalQueryExecYieldIterations.load(),
                                             Milliseconds{internalQueryExecYieldPeriodMS.load()},
                                             nullptr,
                                             std::make_unique<YieldPolicyCallbacksImpl>(nss),
                                             false /*useExperimentalCommitTxnBehavior*/);

    sbePlan->prepare(data.ctx);
    auto planExec = uassertStatusOK(plan_executor_factory::make(
        opCtx,
        nullptr /*cq*/,
        nullptr /*solution*/,
        {std::move(sbePlan), std::move(data)},
        std::make_unique<ABTPrinter>(std::move(abtTree), phaseManager.getNodeToGroupPropsMap()),
        MultipleCollectionAccessor(collection),
        QueryPlannerParams::Options::DEFAULT,
        nss,
        std::move(yieldPolicy)));
    return planExec;
}

static void populateAdditionalScanDefs(OperationContext* opCtx,
                                       boost::intrusive_ptr<ExpressionContext> expCtx,
                                       const Pipeline& pipeline,
                                       const size_t numberOfPartitions,
                                       PrefixId& prefixId,
                                       opt::unordered_map<std::string, ScanDefinition>& scanDefs,
                                       const DisableIndexOptions disableIndexOptions) {
    for (const auto& involvedNss : pipeline.getInvolvedCollections()) {
        // TODO handle views?
        AutoGetCollectionForReadCommandMaybeLockFree ctx(
            opCtx, involvedNss, AutoGetCollectionViewMode::kViewsForbidden);
        const CollectionPtr& collection = ctx ? ctx.getCollection() : CollectionPtr::null;
        const bool collectionExists = collection != nullptr;
        const std::string uuidStr =
            collectionExists ? collection->uuid().toString() : "<missing_uuid>";

        const std::string collNameStr = involvedNss.coll().toString();
        // TODO: We cannot add the uuidStr suffix because the pipeline translation does not have
        // access to the metadata so it generates a scan over just the collection name.
        const std::string scanDefName = collNameStr;

        opt::unordered_map<std::string, optimizer::IndexDefinition> indexDefs;
        const ProjectionName& scanProjName = prefixId.getNextId("scan");
        if (collectionExists) {
            // TODO: add locks on used indexes?
            indexDefs = buildIndexSpecsOptimizer(
                expCtx, opCtx, collection, scanProjName, disableIndexOptions);
        }

        // For now handle only local parallelism (no over-the-network exchanges).
        DistributionAndPaths distribution{(numberOfPartitions == 1)
                                              ? DistributionType::Centralized
                                              : DistributionType::UnknownPartitioning};

        const CEType collectionCE = collectionExists ? collection->numRecords(opCtx) : -1.0;
        scanDefs[scanDefName] =
            ScanDefinition({{"type", "mongod"},
                            {"database", involvedNss.db().toString()},
                            {"uuid", uuidStr},
                            {ScanNode::kDefaultCollectionNameSpec, collNameStr}},
                           std::move(indexDefs),
                           std::move(distribution),
                           collectionExists,
                           collectionCE);
    }
}

std::unique_ptr<PlanExecutor, PlanExecutor::Deleter> getSBEExecutorViaCascadesOptimizer(
    OperationContext* opCtx,
    boost::intrusive_ptr<ExpressionContext> expCtx,
    const NamespaceString& nss,
    const CollectionPtr& collection,
    const Pipeline& pipeline) {
    const bool collectionExists = collection != nullptr;
    const std::string uuidStr = collectionExists ? collection->uuid().toString() : "<missing_uuid>";
    const std::string collNameStr = nss.coll().toString();
    const std::string scanDefName = collNameStr + "_" + uuidStr;

    auto curOp = CurOp::get(opCtx);
    curOp->debug().cqfUsed = true;

    QueryHints queryHints = getHintsFromQueryKnobs();

    PrefixId prefixId;
    const ProjectionName& scanProjName = prefixId.getNextId("scan");

    // Add the base collection metadata.
    opt::unordered_map<std::string, optimizer::IndexDefinition> indexDefs;
    if (collectionExists) {
        // TODO: add locks on used indexes?
        indexDefs = buildIndexSpecsOptimizer(
            expCtx, opCtx, collection, scanProjName, queryHints._disableIndexes);
    }

    const size_t numberOfPartitions = internalQueryDefaultDOP.load();
    // For now handle only local parallelism (no over-the-network exchanges).
    DistributionAndPaths distribution{(numberOfPartitions == 1)
                                          ? DistributionType::Centralized
                                          : DistributionType::UnknownPartitioning};

    opt::unordered_map<std::string, ScanDefinition> scanDefs;
    const int64_t numRecords = collectionExists ? collection->numRecords(opCtx) : -1;
    scanDefs.emplace(scanDefName,
                     ScanDefinition({{"type", "mongod"},
                                     {"database", nss.db().toString()},
                                     {"uuid", uuidStr},
                                     {ScanNode::kDefaultCollectionNameSpec, collNameStr}},
                                    std::move(indexDefs),
                                    std::move(distribution),
                                    collectionExists,
                                    static_cast<CEType>(numRecords)));

    // Add a scan definition for all involved collections. Note that the base namespace has already
    // been accounted for above and isn't included here.
    populateAdditionalScanDefs(opCtx,
                               expCtx,
                               pipeline,
                               numberOfPartitions,
                               prefixId,
                               scanDefs,
                               queryHints._disableIndexes);

    Metadata metadata(std::move(scanDefs), numberOfPartitions);

    ABT abtTree = collectionExists ? make<ScanNode>(scanProjName, scanDefName)
                                   : make<ValueScanNode>(ProjectionNameVector{scanProjName});
    abtTree =
        translatePipelineToABT(metadata, pipeline, scanProjName, std::move(abtTree), prefixId);

    std::cerr << "******* Translated ABT **********\n";
    std::cerr << ExplainGenerator::explainV2(abtTree) << std::endl;
    std::cerr << "******* Translated ABT **********\n";

    if (collectionExists && numRecords > 0 &&
        internalQueryEnableSamplingCardinalityEstimator.load()) {
        Metadata metadataForSampling = metadata;
        // Do not use indexes for sampling.
        for (auto& entry : metadataForSampling._scanDefs) {
            entry.second.getIndexDefs().clear();
        }

        // TODO: consider a limited rewrite set.
        OptPhaseManager phaseManagerForSampling(OptPhaseManager::getAllRewritesSet(),
                                                prefixId,
                                                false /*requireRID*/,
                                                std::move(metadataForSampling),
                                                std::make_unique<HeuristicCE>(),
                                                std::make_unique<DefaultCosting>(),
                                                DebugInfo::kDefaultForProd);

        OptPhaseManager phaseManager{
            OptPhaseManager::getAllRewritesSet(),
            prefixId,
            false /*requireRID*/,
            std::move(metadata),
            std::make_unique<CESamplingTransport>(opCtx, phaseManagerForSampling, numRecords),
            std::make_unique<DefaultCosting>(),
            DebugInfo::kDefaultForProd};
        phaseManager.getHints() = queryHints;

        return optimizeAndCreateExecutor(
            phaseManager, std::move(abtTree), opCtx, expCtx, nss, collection);
    }

    // Use heuristics.
    OptPhaseManager phaseManager{OptPhaseManager::getAllRewritesSet(),
                                 prefixId,
                                 std::move(metadata),
                                 DebugInfo::kDefaultForProd};
    phaseManager.getHints() = queryHints;

    return optimizeAndCreateExecutor(
        phaseManager, std::move(abtTree), opCtx, expCtx, nss, collection);
}

}  // namespace mongo
