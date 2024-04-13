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

#include "uncommitted_catalog_updates.h"

namespace mongo {

const RecoveryUnit::Decoration<UncommittedCatalogUpdates> getUncommittedCatalogUpdates =
    RecoveryUnit::declareDecoration<UncommittedCatalogUpdates>();

UncommittedCatalogUpdates& UncommittedCatalogUpdates::get(OperationContext* opCtx) {
    return getUncommittedCatalogUpdates(opCtx->recoveryUnit());
}

UncommittedCatalogUpdates::CollectionLookupResult UncommittedCatalogUpdates::lookupCollection(
    OperationContext* opCtx, UUID uuid) {
    auto& entries = UncommittedCatalogUpdates::get(opCtx)._entries;

    // Perform a reverse search so we find the most recent entry affecting this uuid.
    auto it = std::find_if(entries.rbegin(), entries.rend(), [uuid](auto&& entry) {
        // Renamed actions don't have UUIDs.
        if (entry.action == Entry::Action::kRenamedCollection) {
            return false;
        }

        return entry.uuid() == uuid;
    });

    if (it == entries.rend()) {
        return {false, nullptr, false};
    }
    return {true, it->collection, (it->action == Entry::Action::kCreatedCollection)};
}

UncommittedCatalogUpdates::CollectionLookupResult UncommittedCatalogUpdates::lookupCollection(
    OperationContext* opCtx, const NamespaceString& nss) {
    auto& entries = UncommittedCatalogUpdates::get(opCtx)._entries;
    // Perform a reverse search so we find most recent entry affecting this namespace.
    auto it = std::find_if(entries.rbegin(), entries.rend(), [&nss](auto&& entry) {
        return entry.nss == nss && isCollectionEntry(entry);
    });
    if (it == entries.rend()) {
        return {false, nullptr, false};
    }

    return {true, it->collection, (it->action == Entry::Action::kCreatedCollection)};
}

boost::optional<const ViewsForDatabase&> UncommittedCatalogUpdates::getViewsForDatabase(
    StringData dbName) const {
    // Perform a reverse search so we find most recent entry affecting this namespace.
    auto it = std::find_if(_entries.rbegin(), _entries.rend(), [&](auto&& entry) {
        return entry.nss.db() == dbName && entry.viewsForDb;
    });
    if (it == _entries.rend()) {
        return boost::none;
    }
    return {*it->viewsForDb};
}

void UncommittedCatalogUpdates::createCollection(OperationContext* opCtx,
                                                 std::shared_ptr<Collection> coll) {
    _createCollection(opCtx, coll, UncommittedCatalogUpdates::Entry::Action::kCreatedCollection);
}

void UncommittedCatalogUpdates::recreateCollection(OperationContext* opCtx,
                                                   std::shared_ptr<Collection> coll) {
    _createCollection(opCtx, coll, UncommittedCatalogUpdates::Entry::Action::kRecreatedCollection);
}

void UncommittedCatalogUpdates::_createCollection(OperationContext* opCtx,
                                                  std::shared_ptr<Collection> coll,
                                                  Entry::Action action) {
    const auto& nss = coll->ns();
    auto uuid = coll->uuid();
    _entries.push_back({action, coll, nss, uuid});

    // When we create a collection after a drop we skip registering the collection in the
    // preCommitHook and register it during the same commit handler that we unregister the
    // collection.
    if (action == Entry::Action::kCreatedCollection) {
        opCtx->recoveryUnit()->registerPreCommitHook([uuid](OperationContext* opCtx) {
            auto uncommittedCatalogUpdates = UncommittedCatalogUpdates::get(opCtx);
            auto [found, createdColl, newColl] = lookupCollection(opCtx, uuid);
            if (!createdColl) {
                return;
            }

            // Invariant that a collection is found.
            invariant(createdColl.get(), uuid.toString());

            // This will throw when registering a namespace which is already in use.
            CollectionCatalog::write(opCtx, [&, coll = createdColl](CollectionCatalog& catalog) {
                catalog.registerCollection(opCtx, coll);
            });

            opCtx->recoveryUnit()->onRollback([opCtx, uuid]() {
                CollectionCatalog::write(opCtx, [&](CollectionCatalog& catalog) {
                    catalog.deregisterCollection(opCtx, uuid);
                });
            });
        });
    }

    // We hold a reference to prevent the collection from being deleted when `PublishCatalogUpdates`
    // runs its rollback handler as that happens first. Other systems may have setup some rollback
    // handler that need to interact with this collection.
    opCtx->recoveryUnit()->onRollback([coll]() {});
}

void UncommittedCatalogUpdates::writableCollection(std::shared_ptr<Collection> collection) {
    const auto& ns = collection->ns();
    _entries.push_back({Entry::Action::kWritableCollection, std::move(collection), ns});
}

void UncommittedCatalogUpdates::renameCollection(const Collection* collection,
                                                 const NamespaceString& from) {
    auto it = std::find_if(_entries.rbegin(), _entries.rend(), [collection](auto&& entry) {
        return entry.collection.get() == collection;
    });
    invariant(it != _entries.rend());
    it->nss = collection->ns();
    _entries.push_back({Entry::Action::kRenamedCollection, nullptr, from, boost::none, it->nss});
}

void UncommittedCatalogUpdates::dropCollection(const Collection* collection) {
    auto it =
        std::find_if(_entries.rbegin(), _entries.rend(), [uuid = collection->uuid()](auto&& entry) {
            return entry.uuid() == uuid;
        });
    if (it == _entries.rend()) {
        // An entry with this uuid was not found so add a new entry.
        _entries.push_back(
            {Entry::Action::kDroppedCollection, nullptr, collection->ns(), collection->uuid()});
        return;
    }

    if (it->action == Entry::Action::kRecreatedCollection) {
        // Scenario: i. drop nss  ->  ii. recreate nss  ->  iii. drop nss again
        //
        // In this case, we don't have to create another drop entry (iii), we can simply delete
        // entry (ii) such that subsequent lookups will find the previous drop (i).
        _entries.erase(it.base());
        return;
    }

    // If the entry doesn't have a Collection pointer, no further action is needed.
    if (!it->collection) {
        return;
    }

    // Transform the found entry into a dropped entry.
    invariant(it->collection.get() == collection);
    it->action = Entry::Action::kDroppedCollection;
    it->externalUUID = it->collection->uuid();
    it->collection = nullptr;
}

void UncommittedCatalogUpdates::replaceViewsForDatabase(StringData dbName,
                                                        ViewsForDatabase&& vfdb) {
    _entries.push_back({Entry::Action::kReplacedViewsForDatabase,
                        nullptr,
                        NamespaceString{dbName},
                        boost::none,
                        {},
                        std::move(vfdb)});
}

void UncommittedCatalogUpdates::addView(OperationContext* opCtx, const NamespaceString& nss) {
    opCtx->recoveryUnit()->registerPreCommitHook([nss](OperationContext* opCtx) {
        CollectionCatalog::write(opCtx, [opCtx, nss](CollectionCatalog& catalog) {
            catalog.registerUncommittedView(opCtx, nss);
        });
    });
    opCtx->recoveryUnit()->onRollback([opCtx, nss]() {
        CollectionCatalog::write(
            opCtx, [&](CollectionCatalog& catalog) { catalog.deregisterUncommittedView(nss); });
    });
    _entries.push_back({Entry::Action::kAddViewResource, nullptr, nss});
}

void UncommittedCatalogUpdates::removeView(const NamespaceString& nss) {
    _entries.push_back({Entry::Action::kRemoveViewResource, nullptr, nss});
}

std::vector<UncommittedCatalogUpdates::Entry> UncommittedCatalogUpdates::releaseEntries() {
    std::vector<Entry> ret;
    std::swap(ret, _entries);
    return ret;
}

void UncommittedCatalogUpdates::setIgnoreExternalViewChanges(StringData dbName, bool value) {
    if (value) {
        _ignoreExternalViewChanges.emplace(dbName);
    } else {
        _ignoreExternalViewChanges.erase(dbName);
    }
}

bool UncommittedCatalogUpdates::shouldIgnoreExternalViewChanges(StringData dbName) const {
    return _ignoreExternalViewChanges.contains(dbName);
}

bool UncommittedCatalogUpdates::isCreatedCollection(OperationContext* opCtx,
                                                    const NamespaceString& nss) {
    const auto& lookupResult = lookupCollection(opCtx, nss);
    return lookupResult.newColl;
}

}  // namespace mongo
