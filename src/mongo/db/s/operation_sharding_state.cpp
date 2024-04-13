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

#include "mongo/db/s/operation_sharding_state.h"

#include "mongo/db/s/sharding_api_d_params_gen.h"

namespace mongo {
namespace {

const OperationContext::Decoration<OperationShardingState> shardingMetadataDecoration =
    OperationContext::declareDecoration<OperationShardingState>();

}  // namespace

OperationShardingState::OperationShardingState() = default;

OperationShardingState::~OperationShardingState() {
    invariant(!_shardingOperationFailedStatus);
}

OperationShardingState& OperationShardingState::get(OperationContext* opCtx) {
    return shardingMetadataDecoration(opCtx);
}

bool OperationShardingState::isComingFromRouter(OperationContext* opCtx) {
    const auto& oss = get(opCtx);
    return !oss._databaseVersions.empty() || !oss._shardVersions.empty();
}

void OperationShardingState::setShardRole(OperationContext* opCtx,
                                          const NamespaceString& nss,
                                          const boost::optional<ChunkVersion>& shardVersion,
                                          const boost::optional<DatabaseVersion>& databaseVersion) {
    auto& oss = OperationShardingState::get(opCtx);

    bool shardVersionInserted = false;
    bool databaseVersionInserted = false;
    try {
        boost::optional<OperationShardingState::ShardVersionTracker&> shardVersionTracker;
        if (shardVersion) {
            auto emplaceResult = oss._shardVersions.try_emplace(nss.ns(), *shardVersion);
            shardVersionInserted = emplaceResult.second;
            shardVersionTracker = emplaceResult.first->second;
            if (!shardVersionInserted) {
                uassert(640570,
                        str::stream()
                            << "Illegal attempt to change the expected shard version for " << nss
                            << " from " << shardVersionTracker->v << " to " << *shardVersion
                            << " at recursion level " << shardVersionTracker->recursion,
                        shardVersionTracker->v == *shardVersion);
                invariant(shardVersionTracker->recursion > 0);
            } else {
                invariant(shardVersionTracker->recursion == 0);
            }
        }

        boost::optional<OperationShardingState::DatabaseVersionTracker&> dbVersionTracker;
        if (databaseVersion) {
            auto emplaceResult = oss._databaseVersions.try_emplace(nss.db(), *databaseVersion);
            databaseVersionInserted = emplaceResult.second;
            dbVersionTracker = emplaceResult.first->second;
            if (!databaseVersionInserted) {
                uassert(640571,
                        str::stream()
                            << "Illegal attempt to change the expected database version for "
                            << nss.db() << " from " << dbVersionTracker->v << " to "
                            << *databaseVersion << " at recursion level "
                            << dbVersionTracker->recursion,
                        dbVersionTracker->v == *databaseVersion);
                invariant(dbVersionTracker->recursion > 0);
            } else {
                invariant(dbVersionTracker->recursion == 0);
            }
        }

        // Update the recursion at the end to preserve the strong exception guarantee.
        if (shardVersionTracker) {
            shardVersionTracker->recursion++;
        }
        if (dbVersionTracker) {
            dbVersionTracker->recursion++;
        }
    } catch (const DBException&) {
        // Clean any oss update done within this method on failure to get a strong exception
        // guarantee on ScopedSetShardRole objects.
        if (shardVersionInserted) {
            oss._shardVersions.erase(nss.ns());
        }
        if (databaseVersionInserted) {
            oss._databaseVersions.erase(nss.db());
        }

        throw;
    }
}

boost::optional<ChunkVersion> OperationShardingState::getShardVersion(const NamespaceString& nss) {
    const auto it = _shardVersions.find(nss.ns());
    if (it != _shardVersions.end()) {
        return it->second.v;
    }
    return boost::none;
}

bool OperationShardingState::hasDbVersion() const {
    return !_databaseVersions.empty();
}

boost::optional<DatabaseVersion> OperationShardingState::getDbVersion(StringData dbName) const {
    const auto it = _databaseVersions.find(dbName);
    if (it != _databaseVersions.end()) {
        return it->second.v;
    }
    return boost::none;
}

Status OperationShardingState::waitForCriticalSectionToComplete(
    OperationContext* opCtx, SharedSemiFuture<void> critSecSignal) noexcept {
    // Must not block while holding a lock
    invariant(!opCtx->lockState()->isLocked());

    // If we are in a transaction, limit the time we can wait behind the critical section. This is
    // needed in order to prevent distributed deadlocks in situations where a DDL operation needs to
    // acquire the critical section on several shards.
    //
    // In such cases, shard running a transaction could be waiting for the critical section to be
    // exited, while on another shard the transaction has already executed some statement and
    // stashed locks which prevent the critical section from being acquired in that node. Limiting
    // the wait behind the critical section will ensure that the transaction will eventually get
    // aborted.
    if (opCtx->inMultiDocumentTransaction()) {
        try {
            opCtx->runWithDeadline(
                opCtx->getServiceContext()->getFastClockSource()->now() +
                    Milliseconds(metadataRefreshInTransactionMaxWaitBehindCritSecMS.load()),
                ErrorCodes::ExceededTimeLimit,
                [&] { critSecSignal.wait(opCtx); });
            return Status::OK();
        } catch (const DBException& ex) {
            // This is a best-effort attempt to wait for the critical section to complete, so no
            // need to handle any exceptions
            return ex.toStatus();
        }
    } else {
        return critSecSignal.waitNoThrow(opCtx);
    }
}

void OperationShardingState::setShardingOperationFailedStatus(const Status& status) {
    invariant(!_shardingOperationFailedStatus);
    _shardingOperationFailedStatus = std::move(status);
}

boost::optional<Status> OperationShardingState::resetShardingOperationFailedStatus() {
    if (!_shardingOperationFailedStatus) {
        return boost::none;
    }
    Status failedStatus = Status(*_shardingOperationFailedStatus);
    _shardingOperationFailedStatus = boost::none;
    return failedStatus;
}

using ScopedAllowImplicitCollectionCreate_UNSAFE =
    OperationShardingState::ScopedAllowImplicitCollectionCreate_UNSAFE;

ScopedAllowImplicitCollectionCreate_UNSAFE::ScopedAllowImplicitCollectionCreate_UNSAFE(
    OperationContext* opCtx, bool forceCSRAsUnknownAfterCollectionCreation)
    : _opCtx(opCtx) {
    auto& oss = get(_opCtx);
    invariant(!oss._allowCollectionCreation);
    oss._allowCollectionCreation = true;
    oss._forceCSRAsUnknownAfterCollectionCreation = forceCSRAsUnknownAfterCollectionCreation;
}

ScopedAllowImplicitCollectionCreate_UNSAFE::~ScopedAllowImplicitCollectionCreate_UNSAFE() {
    auto& oss = get(_opCtx);
    invariant(oss._allowCollectionCreation);
    oss._allowCollectionCreation = false;
    oss._forceCSRAsUnknownAfterCollectionCreation = false;
}

ScopedSetShardRole::ScopedSetShardRole(OperationContext* opCtx,
                                       NamespaceString nss,
                                       boost::optional<ChunkVersion> shardVersion,
                                       boost::optional<DatabaseVersion> databaseVersion)
    : _opCtx(opCtx),
      _nss(std::move(nss)),
      _shardVersion(std::move(shardVersion)),
      _databaseVersion(std::move(databaseVersion)) {
    OperationShardingState::setShardRole(_opCtx, _nss, _shardVersion, _databaseVersion);
}

ScopedSetShardRole::~ScopedSetShardRole() {
    auto& oss = OperationShardingState::get(_opCtx);

    if (_shardVersion) {
        auto it = oss._shardVersions.find(_nss.ns());
        invariant(it != oss._shardVersions.end());
        auto& tracker = it->second;
        invariant(--tracker.recursion >= 0);
        if (tracker.recursion == 0)
            oss._shardVersions.erase(it);
    }

    if (_databaseVersion) {
        auto it = oss._databaseVersions.find(_nss.db());
        invariant(it != oss._databaseVersions.end());
        auto& tracker = it->second;
        invariant(--tracker.recursion >= 0);
        if (tracker.recursion == 0)
            oss._databaseVersions.erase(it);
    }
}

}  // namespace mongo
