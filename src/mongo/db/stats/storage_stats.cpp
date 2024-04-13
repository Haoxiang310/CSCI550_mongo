/**
 *    Copyright (C) 2018-present MongoDB, Inc.
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

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kFTDC

#include "mongo/platform/basic.h"

#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/collection_catalog.h"
#include "mongo/db/catalog/database_holder.h"
#include "mongo/db/catalog/index_catalog.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/index/index_access_method.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/s/balancer_stats_registry.h"
#include "mongo/db/timeseries/bucket_catalog.h"
#include "mongo/db/timeseries/timeseries_stats.h"
#include "mongo/logv2/log.h"
#include "mongo/s/sharding_feature_flags_gen.h"

#include "mongo/db/stats/storage_stats.h"

namespace mongo {

Status appendCollectionStorageStats(OperationContext* opCtx,
                                    const NamespaceString& nss,
                                    const StorageStatsSpec& storageStatsSpec,
                                    BSONObjBuilder* result) {
    static constexpr auto kOrphanCountField = "numOrphanDocs"_sd;

    auto scale = storageStatsSpec.getScale().value_or(1);
    bool verbose = storageStatsSpec.getVerbose();
    bool waitForLock = storageStatsSpec.getWaitForLock();
    bool numericOnly = storageStatsSpec.getNumericOnly();

    const auto bucketNss =
        nss.isTimeseriesBucketsCollection() ? nss : nss.makeTimeseriesBucketsNamespace();
    // Hold reference to the catalog for collection lookup without locks to be safe.
    auto catalog = CollectionCatalog::get(opCtx);
    auto bucketsColl = catalog->lookupCollectionByNamespace(opCtx, bucketNss);
    const bool mayBeTimeseries = bucketsColl && bucketsColl->getTimeseriesOptions();
    const auto collNss =
        (mayBeTimeseries && !nss.isTimeseriesBucketsCollection()) ? std::move(bucketNss) : nss;

    boost::optional<AutoGetCollectionForReadCommandMaybeLockFree> autoColl;
    try {
        autoColl.emplace(opCtx,
                         collNss,
                         AutoGetCollectionViewMode::kViewsForbidden,
                         waitForLock ? Date_t::max() : Date_t::now());
    } catch (const ExceptionForCat<ErrorCategory::Interruption>&) {
        LOGV2_DEBUG(3088801, 2, "Failed to retrieve storage statistics", logAttrs(collNss));
        return Status::OK();
    }

    const auto& collection = autoColl->getCollection();  // Will be set if present
    const bool isTimeseries = collection && collection->getTimeseriesOptions().has_value();

    // We decided the requested namespace was a time series view, so we redirected to the underlying
    // buckets collection. However, when we tried to acquire that collection, it did not exist or it
    // did not have time series options, which means it was dropped and potentially recreated in
    // between the two calls. Logically, the collection that we were looking for does not exist.
    bool logicallyNotFound = collNss != nss && !isTimeseries;

    if (!collection || logicallyNotFound) {
        result->appendNumber("size", 0);
        result->appendNumber("count", 0);
        result->appendNumber(kOrphanCountField, 0);
        result->appendNumber("storageSize", 0);
        result->append("totalSize", 0);
        result->append("nindexes", 0);
        result->appendNumber("totalIndexSize", 0);
        result->append("indexDetails", BSONObj());
        result->append("indexSizes", BSONObj());
        result->append("scaleFactor", scale);
        return {ErrorCodes::NamespaceNotFound,
                "Collection [" + collNss.toString() + "] not found."};
    }

    long long size = collection->dataSize(opCtx) / scale;
    result->appendNumber("size", size);

    long long numRecords = collection->numRecords(opCtx);
    if (isTimeseries) {
        BSONObjBuilder bob(result->subobjStart("timeseries"));
        bob.append("bucketsNs", collNss.ns());
        bob.appendNumber("bucketCount", numRecords);
        if (numRecords) {
            bob.append("avgBucketSize", collection->averageObjectSize(opCtx));
        }
        BucketCatalog::get(opCtx).appendExecutionStats(collNss.getTimeseriesViewNamespace(), &bob);
        TimeseriesStats::get(collection.get()).append(&bob);
    } else {
        result->appendNumber("count", numRecords);
        if (numRecords) {
            result->append("avgObjSize", collection->averageObjectSize(opCtx));
        }
    }

    if (serverGlobalParams.clusterRole == ClusterRole::ShardServer &&
        !nss.isNamespaceAlwaysUnsharded()) {
        if (serverGlobalParams.featureCompatibility.isVersionInitialized() &&
            feature_flags::gOrphanTracking.isEnabled(serverGlobalParams.featureCompatibility)) {
            result->appendNumber(
                kOrphanCountField,
                BalancerStatsRegistry::get(opCtx)->getCollNumOrphanDocsFromDiskIfNeeded(
                    opCtx, collection->uuid()));
        }
    } else {
        result->appendNumber(kOrphanCountField, 0);
    }

    const RecordStore* recordStore = collection->getRecordStore();
    auto storageSize =
        static_cast<long long>(recordStore->storageSize(opCtx, result, verbose ? 1 : 0));
    result->appendNumber("storageSize", storageSize / scale);
    result->appendNumber("freeStorageSize",
                         static_cast<long long>(recordStore->freeStorageSize(opCtx)) / scale);

    const bool isCapped = collection->isCapped();
    result->appendBool("capped", isCapped);
    if (isCapped) {
        result->appendNumber("max", collection->getCappedMaxDocs());
        result->appendNumber("maxSize", collection->getCappedMaxSize() / scale);
    }

    if (numericOnly) {
        recordStore->appendNumericCustomStats(opCtx, result, scale);
    } else {
        recordStore->appendAllCustomStats(opCtx, result, scale);
    }

    const IndexCatalog* indexCatalog = collection->getIndexCatalog();
    result->append("nindexes", indexCatalog->numIndexesTotal(opCtx));

    BSONObjBuilder indexDetails;
    std::vector<std::string> indexBuilds;

    auto it = indexCatalog->getIndexIterator(
        opCtx, IndexCatalog::InclusionPolicy::kReady | IndexCatalog::InclusionPolicy::kUnfinished);
    while (it->more()) {
        const IndexCatalogEntry* entry = it->next();
        const IndexDescriptor* descriptor = entry->descriptor();
        const IndexAccessMethod* iam = entry->accessMethod();
        invariant(iam);

        BSONObjBuilder bob;
        if (iam->appendCustomStats(opCtx, &bob, scale)) {
            indexDetails.append(descriptor->indexName(), bob.obj());
        }

        // Not all indexes in the collection stats may be visible or consistent with our
        // snapshot. For this reason, it is unsafe to check `isReady` on the entry, which
        // asserts that the index's in-memory state is consistent with our snapshot.
        if (!entry->isPresentInMySnapshot(opCtx)) {
            continue;
        }

        if (!entry->isReadyInMySnapshot(opCtx)) {
            indexBuilds.push_back(descriptor->indexName());
        }
    }

    result->append("indexDetails", indexDetails.obj());
    result->append("indexBuilds", indexBuilds);

    BSONObjBuilder indexSizes;
    long long indexSize = collection->getIndexSize(opCtx, &indexSizes, scale);

    result->appendNumber("totalIndexSize", indexSize / scale);
    result->appendNumber("totalSize", (storageSize + indexSize) / scale);
    result->append("indexSizes", indexSizes.obj());
    result->append("scaleFactor", scale);

    return Status::OK();
}

Status appendCollectionRecordCount(OperationContext* opCtx,
                                   const NamespaceString& nss,
                                   BSONObjBuilder* result) {
    AutoGetCollectionForReadCommandMaybeLockFree collection(opCtx, nss);
    if (!collection) {
        return {ErrorCodes::NamespaceNotFound,
                str::stream() << "Collection [" << nss.toString() << "] not found."};
    }

    result->appendNumber("count", static_cast<long long>(collection->numRecords(opCtx)));

    return Status::OK();
}
}  // namespace mongo
