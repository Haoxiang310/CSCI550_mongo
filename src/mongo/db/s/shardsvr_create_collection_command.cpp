/**
 *    Copyright (C) 2020-present MongoDB, Inc.
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
#include "mongo/db/catalog/collection_catalog.h"
#include "mongo/db/commands.h"
#include "mongo/db/commands/feature_compatibility_version.h"
#include "mongo/db/s/create_collection_coordinator.h"
#include "mongo/db/s/sharding_ddl_coordinator_service.h"
#include "mongo/db/s/sharding_state.h"
#include "mongo/db/timeseries/timeseries_index_schema_conversion_functions.h"
#include "mongo/db/timeseries/timeseries_options.h"
#include "mongo/logv2/log.h"

namespace mongo {
namespace {

class ShardsvrCreateCollectionCommand final : public TypedCommand<ShardsvrCreateCollectionCommand> {
public:
    using Request = ShardsvrCreateCollection;
    using Response = CreateCollectionResponse;

    bool skipApiVersionCheck() const override {
        // Internal command (server to server).
        return true;
    }

    std::string help() const override {
        return "Internal command. Do not call directly. Creates a collection.";
    }

    bool adminOnly() const override {
        return false;
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }

    class Invocation final : public InvocationBase {
    public:
        using InvocationBase::InvocationBase;

        Response typedRun(OperationContext* opCtx) {
            uassertStatusOK(ShardingState::get(opCtx)->canAcceptShardedCommands());

            opCtx->setAlwaysInterruptAtStepDownOrUp();

            CommandHelpers::uassertCommandRunWithMajority(Request::kCommandName,
                                                          opCtx->getWriteConcern());

            uassert(ErrorCodes::NotImplemented,
                    "Create Collection path has not been implemented",
                    request().getShardKey());

            auto nss = ns();
            auto bucketsNs = nss.makeTimeseriesBucketsNamespace();
            auto bucketsColl =
                CollectionCatalog::get(opCtx)->lookupCollectionByNamespaceForRead(opCtx, bucketsNs);
            CreateCollectionRequest createCmdRequest = request().getCreateCollectionRequest();

            // If the 'system.buckets' exists or 'timeseries' parameters are passed in, we know that
            // we are trying shard a timeseries collection.
            if (bucketsColl || createCmdRequest.getTimeseries()) {
                uassert(5731502,
                        "Sharding a timeseries collection feature is not enabled",
                        feature_flags::gFeatureFlagShardedTimeSeries.isEnabled(
                            serverGlobalParams.featureCompatibility));

                if (bucketsColl) {
                    uassert(6159000,
                            str::stream() << "the collection '" << bucketsNs
                                          << "' does not have 'timeseries' options",
                            bucketsColl->getTimeseriesOptions());

                    if (createCmdRequest.getTimeseries()) {
                        uassert(5731500,
                                str::stream()
                                    << "the 'timeseries' spec provided must match that of exists '"
                                    << nss << "' collection",
                                timeseries::optionsAreEqual(*createCmdRequest.getTimeseries(),
                                                            *bucketsColl->getTimeseriesOptions()));
                    } else {
                        createCmdRequest.setTimeseries(bucketsColl->getTimeseriesOptions());
                    }
                }

                auto timeField = createCmdRequest.getTimeseries()->getTimeField();
                auto metaField = createCmdRequest.getTimeseries()->getMetaField();
                BSONObjIterator iter{*createCmdRequest.getShardKey()};
                while (auto elem = iter.next()) {
                    if (elem.fieldNameStringData() == timeField) {
                        uassert(5914000,
                                str::stream()
                                    << "the time field '" << timeField
                                    << "' can be only at the end of the shard key pattern",
                                !iter.more());
                    } else {
                        uassert(5914001,
                                str::stream() << "only the time field or meta field can be "
                                                 "part of shard key pattern",
                                metaField &&
                                    (elem.fieldNameStringData() == *metaField ||
                                     elem.fieldNameStringData().startsWith(*metaField + ".")));
                    }
                }
                nss = bucketsNs;
                createCmdRequest.setShardKey(
                    uassertStatusOK(timeseries::createBucketsShardKeySpecFromTimeseriesShardKeySpec(
                        *createCmdRequest.getTimeseries(), *createCmdRequest.getShardKey())));
            }

            const auto createCollectionCoordinator = [&] {
                FixedFCVRegion fixedFcvRegion(opCtx);

                auto coordinatorDoc = [&] {
                    if (serverGlobalParams.featureCompatibility.isLessThan(
                            multiversion::FeatureCompatibilityVersion::kVersion_6_0)) {
                        auto doc = CreateCollectionCoordinatorDocumentPre60Compatible();
                        doc.setShardingDDLCoordinatorMetadata(
                            {{std::move(nss),
                              DDLCoordinatorTypeEnum::kCreateCollectionPre60Compatible}});
                        doc.setCreateCollectionRequest(std::move(createCmdRequest));
                        return doc.toBSON();
                    } else {
                        auto doc = CreateCollectionCoordinatorDocument();
                        doc.setShardingDDLCoordinatorMetadata(
                            {{std::move(nss), DDLCoordinatorTypeEnum::kCreateCollection}});
                        doc.setCreateCollectionRequest(std::move(createCmdRequest));
                        return doc.toBSON();
                    }
                }();

                auto service = ShardingDDLCoordinatorService::getService(opCtx);
                return checked_pointer_cast<CreateCollectionCoordinator>(
                    service->getOrCreateInstance(opCtx, std::move(coordinatorDoc)));
            }();

            return createCollectionCoordinator->getResult(opCtx);
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

} shardsvrCreateCollectionCommand;

}  // namespace
}  // namespace mongo
