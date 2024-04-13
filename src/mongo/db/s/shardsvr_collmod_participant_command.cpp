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

#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/catalog_raii.h"
#include "mongo/db/commands.h"
#include "mongo/db/s/collection_sharding_runtime.h"
#include "mongo/db/s/collmod_coordinator.h"
#include "mongo/db/s/database_sharding_state.h"
#include "mongo/db/s/recoverable_critical_section_service.h"
#include "mongo/db/s/shard_filtering_metadata_refresh.h"
#include "mongo/db/s/sharded_collmod_gen.h"
#include "mongo/db/s/sharding_state.h"
#include "mongo/db/timeseries/catalog_helper.h"
#include "mongo/db/timeseries/timeseries_collmod.h"
#include "mongo/logv2/log.h"
#include "mongo/s/catalog/sharding_catalog_client.h"

namespace mongo {
namespace {

class ShardSvrCollModParticipantCommand final
    : public TypedCommand<ShardSvrCollModParticipantCommand> {
public:
    using Request = ShardsvrCollModParticipant;
    using Response = CollModReply;

    std::string help() const override {
        return "Internal command, which is exported by the shards. Do not call "
               "directly. Unblocks CRUD and processes collMod.";
    }

    bool skipApiVersionCheck() const override {
        // Internal command (server to server).
        return true;
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return Command::AllowedOnSecondary::kNever;
    }

    class Invocation final : public InvocationBase {
    public:
        using InvocationBase::InvocationBase;

        Response typedRun(OperationContext* opCtx) {
            uassertStatusOK(ShardingState::get(opCtx)->canAcceptShardedCommands());

            CommandHelpers::uassertCommandRunWithMajority(Request::kCommandName,
                                                          opCtx->getWriteConcern());

            opCtx->setAlwaysInterruptAtStepDownOrUp();

            // If the needsUnblock flag is set, we must have blocked the CRUD operations in the
            // previous phase of collMod operation for granularity updates. Unblock it now after we
            // have updated the granularity.
            if (request().getNeedsUnblock()) {
                // This is only ever used for time-series collection as of now.
                uassert(6102802,
                        "collMod unblocking should always be on a time-series collection",
                        timeseries::getTimeseriesOptions(opCtx, ns(), true));
                auto bucketNs = ns().makeTimeseriesBucketsNamespace();

                {
                    // Clear the filtering metadata before releasing the critical section to prevent
                    // scenarios where a stepDown/stepUp will leave the node with wrong metadata.
                    // Cleanup on secondary nodes is performed by the release of the section.
                    AutoGetCollection autoColl(opCtx, bucketNs, MODE_IX);
                    CollectionShardingRuntime::get(opCtx, bucketNs)->clearFilteringMetadata(opCtx);
                }

                auto service = RecoverableCriticalSectionService::get(opCtx);
                const auto reason = BSON("command"
                                         << "ShardSvrParticipantBlockCommand"
                                         << "ns" << bucketNs.toString());
                service->releaseRecoverableCriticalSection(
                    opCtx, bucketNs, reason, ShardingCatalogClient::kLocalWriteConcern);
            }

            BSONObjBuilder builder;
            CollMod cmd(ns());
            cmd.setCollModRequest(request().getCollModRequest());

            // This flag is set from the collMod coordinator. We do not allow view definition change
            // on non-primary shards since it's not in the view catalog.
            auto performViewChange = request().getPerformViewChange();
            uassertStatusOK(timeseries::processCollModCommandWithTimeSeriesTranslation(
                opCtx, ns(), cmd, performViewChange, &builder));
            return CollModReply::parse(IDLParserErrorContext("CollModReply"), builder.obj());
        }

    private:
        NamespaceString ns() const override {
            return request().getNamespace();
        }

        bool supportsWriteConcern() const override {
            return true;
        }

        void doCheckAuthorization(OperationContext* opCtx) const override {
            uassert(ErrorCodes::Unauthorized,
                    "Unauthorized",
                    AuthorizationSession::get(opCtx->getClient())
                        ->isAuthorizedForActionsOnResource(ResourcePattern::forClusterResource(),
                                                           ActionType::internal));
        }
    };
} shardsvrCollModParticipantCommand;

}  // namespace
}  // namespace mongo
