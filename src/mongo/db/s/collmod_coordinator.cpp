/**
 *    Copyright (C) 2021-present MongoDB, Inc.
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

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kSharding

#include "mongo/db/s/collmod_coordinator.h"

#include "mongo/db/catalog/coll_mod.h"
#include "mongo/db/catalog/collection_catalog.h"
#include "mongo/db/catalog/collection_uuid_mismatch.h"
#include "mongo/db/catalog/database_holder.h"
#include "mongo/db/coll_mod_gen.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/ops/insert.h"
#include "mongo/db/s/participant_block_gen.h"
#include "mongo/db/s/sharded_collmod_gen.h"
#include "mongo/db/s/sharding_ddl_coordinator_gen.h"
#include "mongo/db/s/sharding_ddl_util.h"
#include "mongo/db/s/sharding_state.h"
#include "mongo/db/timeseries/catalog_helper.h"
#include "mongo/db/timeseries/timeseries_collmod.h"
#include "mongo/db/timeseries/timeseries_options.h"
#include "mongo/idl/idl_parser.h"
#include "mongo/logv2/log.h"
#include "mongo/s/cluster_commands_helpers.h"
#include "mongo/s/grid.h"
#include "mongo/util/fail_point.h"

namespace mongo {

MONGO_FAIL_POINT_DEFINE(collModBeforeConfigServerUpdate);

namespace {

bool isShardedColl(OperationContext* opCtx, const NamespaceString& nss) {
    try {
        auto coll = Grid::get(opCtx)->catalogClient()->getCollection(opCtx, nss);
        return true;
    } catch (const ExceptionFor<ErrorCodes::NamespaceNotFound>&) {
        // The collection is not sharded or doesn't exist.
        return false;
    }
}

bool hasTimeSeriesGranularityUpdate(const CollModRequest& request) {
    return request.getTimeseries() && request.getTimeseries()->getGranularity();
}

}  // namespace

CollModCoordinator::CollModCoordinator(ShardingDDLCoordinatorService* service,
                                       const BSONObj& initialState)
    : ShardingDDLCoordinator(service, initialState),
      _initialState{initialState.getOwned()},
      _doc{CollModCoordinatorDocument::parse(IDLParserErrorContext("CollModCoordinatorDocument"),
                                             _initialState)},
      _request{_doc.getCollModRequest()} {}

void CollModCoordinator::checkIfOptionsConflict(const BSONObj& doc) const {
    const auto otherDoc =
        CollModCoordinatorDocument::parse(IDLParserErrorContext("CollModCoordinatorDocument"), doc);

    const auto& selfReq = _request.toBSON();
    const auto& otherReq = otherDoc.getCollModRequest().toBSON();

    uassert(ErrorCodes::ConflictingOperationInProgress,
            str::stream() << "Another collMod for namespace " << nss()
                          << " is being executed with different parameters: " << selfReq,
            SimpleBSONObjComparator::kInstance.evaluate(selfReq == otherReq));
}

boost::optional<BSONObj> CollModCoordinator::reportForCurrentOp(
    MongoProcessInterface::CurrentOpConnectionsMode connMode,
    MongoProcessInterface::CurrentOpSessionsMode sessionMode) noexcept {

    BSONObjBuilder cmdBob;
    if (const auto& optComment = getForwardableOpMetadata().getComment()) {
        cmdBob.append(optComment.get().firstElement());
    }

    const auto currPhase = [&]() {
        stdx::lock_guard l{_docMutex};
        return _doc.getPhase();
    }();

    cmdBob.appendElements(_request.toBSON());
    BSONObjBuilder bob;
    bob.append("type", "op");
    bob.append("desc", "CollModCoordinator");
    bob.append("op", "command");
    bob.append("ns", nss().toString());
    bob.append("command", cmdBob.obj());
    bob.append("currentPhase", currPhase);
    bob.append("active", true);
    return bob.obj();
}

void CollModCoordinator::_enterPhase(Phase newPhase) {
    StateDoc newDoc(_doc);
    newDoc.setPhase(newPhase);

    LOGV2_DEBUG(6069401,
                2,
                "CollMod coordinator phase transition",
                "namespace"_attr = nss(),
                "newPhase"_attr = CollModCoordinatorPhase_serializer(newDoc.getPhase()),
                "oldPhase"_attr = CollModCoordinatorPhase_serializer(_doc.getPhase()));

    if (_doc.getPhase() == Phase::kUnset) {
        newDoc = _insertStateDocument(std::move(newDoc));
    } else {
        ServiceContext::UniqueOperationContext uniqueOpCtx;
        auto opCtx = cc().getOperationContext();
        if (!opCtx) {
            uniqueOpCtx = cc().makeOperationContext();
            opCtx = uniqueOpCtx.get();
        }
        newDoc = _updateStateDocument(opCtx, std::move(newDoc));
    }

    {
        stdx::unique_lock ul{_docMutex};
        _doc = std::move(newDoc);
    }
}

void CollModCoordinator::_performNoopRetryableWriteOnParticipants(
    OperationContext* opCtx, const std::shared_ptr<executor::TaskExecutor>& executor) {
    auto shardsAndConfigsvr = [&] {
        const auto shardRegistry = Grid::get(opCtx)->shardRegistry();
        auto participants = shardRegistry->getAllShardIds(opCtx);
        participants.emplace_back(shardRegistry->getConfigShard()->getId());
        return participants;
    }();

    _doc = _updateSession(opCtx, _doc);
    sharding_ddl_util::performNoopRetryableWriteOnShards(
        opCtx, shardsAndConfigsvr, getCurrentSession(_doc), executor);
}

void CollModCoordinator::_saveCollectionInfoOnCoordinatorIfNecessary(OperationContext* opCtx) {
    if (!_collInfo) {
        CollectionInfo info;
        info.timeSeriesOptions = timeseries::getTimeseriesOptions(opCtx, nss(), true);
        info.nsForTargeting =
            info.timeSeriesOptions ? nss().makeTimeseriesBucketsNamespace() : nss();
        info.isSharded = isShardedColl(opCtx, info.nsForTargeting);
        _collInfo = std::move(info);
    }
}

void CollModCoordinator::_saveShardingInfoOnCoordinatorIfNecessary(OperationContext* opCtx) {
    tassert(
        6522700, "Sharding information must be gathered after collection information", _collInfo);
    if (!_shardingInfo && _collInfo->isSharded) {
        ShardingInfo info;
        info.isPrimaryOwningChunks = false;
        const auto chunkManager =
            uassertStatusOK(Grid::get(opCtx)->catalogCache()->getCollectionRoutingInfoWithRefresh(
                opCtx, _collInfo->nsForTargeting));

        // Coordinator is guaranteed to be running on primary shard
        info.primaryShard = ShardingState::get(opCtx)->shardId();

        std::set<ShardId> shardIdsSet;
        chunkManager.getAllShardIds(&shardIdsSet);
        std::vector<ShardId> participantsNotOwningChunks;

        std::vector<ShardId> shardIdsVec;
        shardIdsVec.reserve(shardIdsSet.size());
        for (const auto& shard : shardIdsSet) {
            if (shard != info.primaryShard) {
                shardIdsVec.push_back(shard);
            } else {
                info.isPrimaryOwningChunks = true;
            }
        }

        auto allShards = Grid::get(opCtx)->shardRegistry()->getAllShardIds(opCtx);
        for (const auto& shard : allShards) {
            if (std::find(shardIdsVec.begin(), shardIdsVec.end(), shard) == shardIdsVec.end() &&
                shard != info.primaryShard) {
                participantsNotOwningChunks.push_back(shard);
            }
        }

        info.participantsOwningChunks = std::move(shardIdsVec);
        info.participantsNotOwningChunks = std::move(participantsNotOwningChunks);
        _shardingInfo = std::move(info);
    }
}

std::vector<AsyncRequestsSender::Response> CollModCoordinator::_sendCollModToPrimaryShard(
    OperationContext* opCtx,
    ShardsvrCollModParticipant& request,
    const std::shared_ptr<executor::ScopedTaskExecutor>& executor) {
    // A view definition will only be present on the primary shard. So we pass an addition
    // 'performViewChange' flag only to the primary shard.
    request.setPerformViewChange(true);

    return sharding_ddl_util::sendAuthenticatedCommandToShards(
        opCtx,
        nss().db(),
        CommandHelpers::appendMajorityWriteConcern(request.toBSON({})),
        {_shardingInfo->primaryShard},
        **executor,
        _shardingInfo->isPrimaryOwningChunks);
}

std::vector<AsyncRequestsSender::Response> CollModCoordinator::_sendCollModToParticipantShards(
    OperationContext* opCtx,
    ShardsvrCollModParticipant& request,
    const std::shared_ptr<executor::ScopedTaskExecutor>& executor) {
    request.setPerformViewChange(false);

    // The collMod command targets all shards, regardless of whether they have chunks. The shards
    // that have no chunks for the collection will not throw nor will be included in the responses.

    sharding_ddl_util::sendAuthenticatedCommandToShards(
        opCtx,
        nss().db(),
        CommandHelpers::appendMajorityWriteConcern(request.toBSON({})),
        _shardingInfo->participantsNotOwningChunks,
        **executor,
        false /* throwOnError */);

    return sharding_ddl_util::sendAuthenticatedCommandToShards(
        opCtx,
        nss().db(),
        CommandHelpers::appendMajorityWriteConcern(request.toBSON({})),
        _shardingInfo->participantsOwningChunks,
        **executor,
        true /* throwOnError */);
}

ExecutorFuture<void> CollModCoordinator::_runImpl(
    std::shared_ptr<executor::ScopedTaskExecutor> executor,
    const CancellationToken& token) noexcept {
    return ExecutorFuture<void>(**executor)
        .then([this, executor = executor, anchor = shared_from_this()] {
            auto opCtxHolder = cc().makeOperationContext();
            auto* opCtx = opCtxHolder.get();
            getForwardableOpMetadata().setOn(opCtx);

            if (_doc.getPhase() > Phase::kUnset) {
                _performNoopRetryableWriteOnParticipants(opCtx, **executor);
            }

            {
                AutoGetCollection coll{
                    opCtx, nss(), MODE_IS, AutoGetCollectionViewMode::kViewsPermitted};
                checkCollectionUUIDMismatch(opCtx, nss(), *coll, _request.getCollectionUUID());
            }

            _saveCollectionInfoOnCoordinatorIfNecessary(opCtx);

            auto isGranularityUpdate = hasTimeSeriesGranularityUpdate(_request);
            uassert(6201808,
                    "Cannot use time-series options for a non-timeseries collection",
                    _collInfo->timeSeriesOptions || !isGranularityUpdate);
            if (isGranularityUpdate) {
                uassert(ErrorCodes::InvalidOptions,
                        "Invalid transition for timeseries.granularity. Can only transition "
                        "from 'seconds' to 'minutes' or 'minutes' to 'hours'.",
                        timeseries::isValidTimeseriesGranularityTransition(
                            _collInfo->timeSeriesOptions->getGranularity(),
                            *_request.getTimeseries()->getGranularity()));
            }
        })
        .then(_executePhase(
            Phase::kBlockShards,
            [this, executor = executor, anchor = shared_from_this()] {
                auto opCtxHolder = cc().makeOperationContext();
                auto* opCtx = opCtxHolder.get();
                getForwardableOpMetadata().setOn(opCtx);

                _doc = _updateSession(opCtx, _doc);

                _saveCollectionInfoOnCoordinatorIfNecessary(opCtx);

                if (_collInfo->isSharded) {
                    const auto migrationsAlreadyBlockedForBucketNss =
                        hasTimeSeriesGranularityUpdate(_request) &&
                        _doc.getMigrationsAlreadyBlockedForBucketNss();

                    if (!migrationsAlreadyBlockedForBucketNss) {
                        _doc.setCollUUID(sharding_ddl_util::getCollectionUUID(
                            opCtx, _collInfo->nsForTargeting, true /* allowViews */));
                        sharding_ddl_util::stopMigrations(
                            opCtx, _collInfo->nsForTargeting, _doc.getCollUUID());
                    }
                }

                _saveShardingInfoOnCoordinatorIfNecessary(opCtx);

                if (_collInfo->isSharded && hasTimeSeriesGranularityUpdate(_request)) {
                    {
                        // Persist the migrationAlreadyBlocked flag on the coordinator document
                        auto newDoc = _doc;
                        newDoc.setMigrationsAlreadyBlockedForBucketNss(true);
                        _updateStateDocument(opCtx, std::move(newDoc));
                    }

                    ShardsvrParticipantBlock blockCRUDOperationsRequest(_collInfo->nsForTargeting);
                    const auto cmdObj = CommandHelpers::appendMajorityWriteConcern(
                        blockCRUDOperationsRequest.toBSON({}));
                    std::vector<ShardId> shards = _shardingInfo->participantsOwningChunks;
                    if (_shardingInfo->isPrimaryOwningChunks) {
                        shards.push_back(_shardingInfo->primaryShard);
                    }
                    sharding_ddl_util::sendAuthenticatedCommandToShards(
                        opCtx, nss().db(), cmdObj, shards, **executor, true /* throwOnError */);
                }
            }))
        .then(_executePhase(
            Phase::kUpdateConfig,
            [this, executor = executor, anchor = shared_from_this()] {
                collModBeforeConfigServerUpdate.pauseWhileSet();

                auto opCtxHolder = cc().makeOperationContext();
                auto* opCtx = opCtxHolder.get();
                getForwardableOpMetadata().setOn(opCtx);

                _doc = _updateSession(opCtx, _doc);

                _saveCollectionInfoOnCoordinatorIfNecessary(opCtx);
                _saveShardingInfoOnCoordinatorIfNecessary(opCtx);

                if (_collInfo->isSharded && _collInfo->timeSeriesOptions &&
                    hasTimeSeriesGranularityUpdate(_request)) {
                    ConfigsvrCollMod request(_collInfo->nsForTargeting, _request);
                    const auto cmdObj =
                        CommandHelpers::appendMajorityWriteConcern(request.toBSON({}));

                    const auto& configShard = Grid::get(opCtx)->shardRegistry()->getConfigShard();
                    uassertStatusOK(Shard::CommandResponse::getEffectiveStatus(
                        configShard->runCommand(opCtx,
                                                ReadPreferenceSetting(ReadPreference::PrimaryOnly),
                                                nss().db().toString(),
                                                cmdObj,
                                                Shard::RetryPolicy::kIdempotent)));
                }
            }))
        .then(_executePhase(
            Phase::kUpdateShards,
            [this, executor = executor, anchor = shared_from_this()] {
                auto opCtxHolder = cc().makeOperationContext();
                auto* opCtx = opCtxHolder.get();
                getForwardableOpMetadata().setOn(opCtx);

                _doc = _updateSession(opCtx, _doc);

                _saveCollectionInfoOnCoordinatorIfNecessary(opCtx);
                _saveShardingInfoOnCoordinatorIfNecessary(opCtx);

                if (_collInfo->isSharded) {
                    try {
                        if (!_firstExecution) {
                            bool allowMigrations = sharding_ddl_util::checkAllowMigrations(
                                opCtx, _collInfo->nsForTargeting);
                            if (_result.is_initialized() && allowMigrations) {
                                // The command finished and we have the response. Return it.
                                return;
                            } else if (allowMigrations) {
                                // Previous run on a different node completed, but we lost the
                                // result in the stepdown. Restart from stage in which we disallow
                                // migrations.
                                _enterPhase(Phase::kBlockShards);
                                uasserted(ErrorCodes::Interrupted,
                                          "Retriable error to move to previous stage");
                            }
                        }

                        ShardsvrCollModParticipant request(nss(), _request);
                        bool needsUnblock = _collInfo->timeSeriesOptions &&
                            hasTimeSeriesGranularityUpdate(_request);
                        request.setNeedsUnblock(needsUnblock);

                        // If trying to convert an index to unique, executes a dryRun first to find
                        // any duplicates without actually changing the indexes to avoid
                        // inconsistent index specs on different shards. Example:
                        //   Shard0: {_id: 0, a: 1}
                        //   Shard1: {_id: 1, a: 2}, {_id: 2, a: 2}
                        //   When trying to convert index {a: 1} to unique, the dry run will return
                        //   the duplicate errors to the user without converting the indexes.
                        if (isCollModIndexUniqueConversion(_request)) {
                            // The 'dryRun' option only works with 'unique' index option. We need to
                            // strip out other incompatible options.
                            auto dryRunRequest = ShardsvrCollModParticipant{
                                nss(), makeCollModDryRunRequest(_request)};
                            std::vector<ShardId> shards = _shardingInfo->participantsOwningChunks;
                            if (_shardingInfo->isPrimaryOwningChunks) {
                                shards.push_back(_shardingInfo->primaryShard);
                            }
                            sharding_ddl_util::sendAuthenticatedCommandToShards(
                                opCtx,
                                nss().db(),
                                CommandHelpers::appendMajorityWriteConcern(
                                    dryRunRequest.toBSON({})),
                                shards,
                                **executor);
                        }

                        std::vector<AsyncRequestsSender::Response> responses;

                        // In the case of the participants, we are broadcasting the collMod to all
                        // the shards. On one hand, if the shard contains chunks for the
                        // collections, we parse all the responses. On the other hand, if the shard
                        // does not contain chunks, we make a best effort to not process the
                        // returned responses or throw any errors.

                        auto primaryResponse = _sendCollModToPrimaryShard(opCtx, request, executor);
                        if (_shardingInfo->isPrimaryOwningChunks) {
                            responses.insert(responses.end(),
                                             std::make_move_iterator(primaryResponse.begin()),
                                             std::make_move_iterator(primaryResponse.end()));
                        }

                        auto participantsResponses =
                            _sendCollModToParticipantShards(opCtx, request, executor);
                        responses.insert(responses.end(),
                                         std::make_move_iterator(participantsResponses.begin()),
                                         std::make_move_iterator(participantsResponses.end()));


                        BSONObjBuilder builder;
                        std::string errmsg;
                        auto ok =
                            appendRawResponses(opCtx, &errmsg, &builder, responses).responseOK;
                        if (!errmsg.empty()) {
                            CommandHelpers::appendSimpleCommandStatus(builder, ok, errmsg);
                        }
                        _result = builder.obj();
                        sharding_ddl_util::resumeMigrations(
                            opCtx, _collInfo->nsForTargeting, _doc.getCollUUID());
                    } catch (DBException& ex) {
                        if (!_isRetriableErrorForDDLCoordinator(ex.toStatus())) {
                            sharding_ddl_util::resumeMigrations(
                                opCtx, _collInfo->nsForTargeting, _doc.getCollUUID());
                        }
                        throw;
                    }
                } else {
                    CollMod cmd(nss());
                    cmd.setCollModRequest(_request);
                    BSONObjBuilder collModResBuilder;
                    uassertStatusOK(timeseries::processCollModCommandWithTimeSeriesTranslation(
                        opCtx, nss(), cmd, true, &collModResBuilder));
                    auto collModRes = collModResBuilder.obj();

                    const auto dbInfo = uassertStatusOK(
                        Grid::get(opCtx)->catalogCache()->getDatabase(opCtx, nss().db()));
                    const auto shard = uassertStatusOK(
                        Grid::get(opCtx)->shardRegistry()->getShard(opCtx, dbInfo->getPrimary()));
                    BSONObjBuilder builder;
                    builder.appendElements(collModRes);
                    BSONObjBuilder subBuilder(builder.subobjStart("raw"));
                    subBuilder.append(shard->getConnString().toString(), collModRes);
                    subBuilder.doneFast();
                    _result = builder.obj();
                }
            }))
        .onError([this, anchor = shared_from_this()](const Status& status) {
            if (!status.isA<ErrorCategory::NotPrimaryError>() &&
                !status.isA<ErrorCategory::ShutdownError>()) {
                LOGV2_ERROR(5757002,
                            "Error running collMod",
                            "namespace"_attr = nss(),
                            "error"_attr = redact(status));
            }
            return status;
        });
}

}  // namespace mongo
