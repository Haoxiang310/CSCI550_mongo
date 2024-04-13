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

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kSharding

#include "mongo/platform/basic.h"

#include "mongo/db/s/balancer/cluster_statistics_impl.h"

#include <algorithm>

#include "mongo/base/status_with.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/client/read_preference.h"
#include "mongo/logv2/log.h"
#include "mongo/s/catalog/type_shard.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/grid.h"
#include "mongo/s/shard_util.h"
#include "mongo/util/str.h"

namespace mongo {

using ShardStatistics = ClusterStatistics::ShardStatistics;

ClusterStatisticsImpl::ClusterStatisticsImpl(BalancerRandomSource& random) : _random(random) {}

ClusterStatisticsImpl::~ClusterStatisticsImpl() = default;

StatusWith<std::vector<ShardStatistics>> ClusterStatisticsImpl::getStats(OperationContext* opCtx) {
    return _getStats(opCtx, boost::none);
}

StatusWith<std::vector<ShardStatistics>> ClusterStatisticsImpl::getCollStats(
    OperationContext* opCtx, NamespaceString const& ns) {
    return _getStats(opCtx, ns);
}

StatusWith<std::vector<ShardStatistics>> ClusterStatisticsImpl::_getStats(
    OperationContext* opCtx, boost::optional<NamespaceString> ns) {
    // Get a list of all the shards that are participating in this balance round along with any
    // maximum allowed quotas and current utilization. We get the latter by issuing
    // db.serverStatus() (mem.mapped) to all shards.
    //
    // TODO: skip unresponsive shards and mark information as stale.
    auto shardsStatus = Grid::get(opCtx)->catalogClient()->getAllShards(
        opCtx, repl::ReadConcernLevel::kMajorityReadConcern);
    if (!shardsStatus.isOK()) {
        return shardsStatus.getStatus();
    }

    auto& shards = shardsStatus.getValue().value;

    std::shuffle(shards.begin(), shards.end(), _random);

    std::vector<ShardStatistics> stats;

    for (const auto& shard : shards) {
        const auto shardSizeStatus = [&]() -> StatusWith<long long> {
            if (ns) {
                return shardutil::retrieveCollectionShardSize(opCtx, shard.getName(), *ns);
            }
            // optimization for the case where the balancer does not care about the dataSize
            if (!shard.getMaxSizeMB()) {
                return 0;
            }
            return shardutil::retrieveTotalShardSize(opCtx, shard.getName());
        }();

        if (!shardSizeStatus.isOK()) {
            const auto& status = shardSizeStatus.getStatus();

            return status.withContext(str::stream()
                                      << "Unable to obtain shard utilization information for "
                                      << shard.getName());
        }

        std::set<std::string> shardTags;

        for (const auto& shardTag : shard.getTags()) {
            shardTags.insert(shardTag);
        }

        stats.emplace_back(shard.getName(),
                           shard.getMaxSizeMB() * 1024 * 1024,
                           shardSizeStatus.getValue(),
                           shard.getDraining(),
                           std::move(shardTags),
                           ShardStatistics::use_bytes_t{});
    }

    return stats;
}

}  // namespace mongo
