/**
 *    Copyright (C) 2023-present MongoDB, Inc.
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
#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kCommand

#include "mongo/db/commands/set_profiling_filter_globally_cmd.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/catalog/collection_catalog.h"
#include "mongo/db/commands/profile_gen.h"
#include "mongo/db/profile_filter_impl.h"
#include "mongo/logv2/log.h"

namespace mongo {

Status SetProfilingFilterGloballyCmd::checkAuthForCommand(Client* client,
                                                          const std::string& dbName,
                                                          const BSONObj& cmdObj) const {
    AuthorizationSession* authSession = AuthorizationSession::get(client);
    return authSession->isAuthorizedForActionsOnResource(ResourcePattern::forAnyNormalResource(),
                                                         ActionType::enableProfiler)
        ? Status::OK()
        : Status(ErrorCodes::Unauthorized, "unauthorized");
}

bool SetProfilingFilterGloballyCmd::run(OperationContext* opCtx,
                                        const std::string& dbName,
                                        const BSONObj& cmdObj,
                                        BSONObjBuilder& result) {
    uassert(7283301,
            str::stream() << "setProfilingFilterGlobally command requires query knob to be enabled",
            internalQueryGlobalProfilingFilter.load());

    auto request = SetProfilingFilterGloballyCmdRequest::parse(
        IDLParserErrorContext("setProfilingFilterGlobally"), cmdObj);

    // Save off the old global default setting so that we can log it and return in the result.
    auto oldDefault = ProfileFilter::getDefault();
    auto newDefault = [&request] {
        const auto& filterOrUnset = request.getFilter();
        if (auto filter = filterOrUnset.obj) {
            return std::make_shared<ProfileFilterImpl>(*filter);
        }
        return std::shared_ptr<ProfileFilterImpl>(nullptr);
    }();

    // Update the global default.
    // Note that since this is not done atomically with the collection catalog write, there is a
    // minor race condition where queries on some databases see the new global default while queries
    // on other databases see old database-specific settings. This is a temporary state and
    // shouldn't impact much in practice. We also don't have to worry about races with database
    // creation, since the global default gets picked up dynamically by queries instead of being
    // explicitly stored for new databases.
    ProfileFilter::setDefault(newDefault);

    // Writing to the CollectionCatalog requires holding the Global lock to avoid concurrent races
    // with BatchedCollectionCatalogWriter.
    boost::optional<Lock::GlobalLock> lk;
    if (!opCtx->lockState()->isNoop()) {
        // Taking the lock is only meaningful if we're in a mongod. Other mongos do not have a
        // notion of collections.
        lk.emplace(opCtx, MODE_IX);
    }

    // Update all existing database settings.
    CollectionCatalog::write(opCtx, [&](CollectionCatalog& catalog) {
        catalog.setAllDatabaseProfileFilters(newDefault);
    });

    // Capture the old setting in the result object.
    if (oldDefault) {
        result.append("was", oldDefault->serialize());
    } else {
        result.append("was", "none");
    }

    // Log the change made to server's global profiling settings.
    LOGV2(72832,
          "Profiler settings changed globally",
          "from"_attr = oldDefault ? BSON("filter" << oldDefault->serialize())
                                   : BSON("filter"
                                          << "none"),
          "to"_attr = newDefault ? BSON("filter" << newDefault->serialize())
                                 : BSON("filter"
                                        << "none"));
    return true;
}
}  // namespace mongo
