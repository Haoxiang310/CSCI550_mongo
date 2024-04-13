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
#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "collection_catalog.h"

#include "mongo/db/catalog/database.h"
#include "mongo/db/catalog/uncommitted_catalog_updates.h"
#include "mongo/db/concurrency/lock_manager_defs.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/server_options.h"
#include "mongo/db/storage/recovery_unit.h"
#include "mongo/db/storage/snapshot_helper.h"
#include "mongo/logv2/log.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/uuid.h"

namespace mongo {
namespace {
struct LatestCollectionCatalog {
    std::shared_ptr<CollectionCatalog> catalog = std::make_shared<CollectionCatalog>();
};
const ServiceContext::Decoration<LatestCollectionCatalog> getCatalog =
    ServiceContext::declareDecoration<LatestCollectionCatalog>();

std::shared_ptr<CollectionCatalog> batchedCatalogWriteInstance;
absl::flat_hash_set<Collection*> batchedCatalogClonedCollections;

const OperationContext::Decoration<std::shared_ptr<const CollectionCatalog>> stashedCatalog =
    OperationContext::declareDecoration<std::shared_ptr<const CollectionCatalog>>();

const auto maxUuid = UUID::parse("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF").getValue();
const auto minUuid = UUID::parse("00000000-0000-0000-0000-000000000000").getValue();
}  // namespace

class IgnoreExternalViewChangesForDatabase {
public:
    IgnoreExternalViewChangesForDatabase(OperationContext* opCtx, StringData dbName)
        : _opCtx(opCtx), _dbName(dbName) {
        auto& uncommittedCatalogUpdates = UncommittedCatalogUpdates::get(_opCtx);
        uncommittedCatalogUpdates.setIgnoreExternalViewChanges(_dbName, true);
    }

    ~IgnoreExternalViewChangesForDatabase() {
        auto& uncommittedCatalogUpdates = UncommittedCatalogUpdates::get(_opCtx);
        uncommittedCatalogUpdates.setIgnoreExternalViewChanges(_dbName, false);
    }

private:
    OperationContext* _opCtx;
    std::string _dbName;
};

/**
 * Publishes all uncommitted Collection actions registered on UncommittedCatalogUpdates to the
 * catalog. All catalog updates are performed under the same write to ensure no external observer
 * can see a partial update. Cleans up UncommittedCatalogUpdates on both commit and rollback to
 * make it behave like a decoration on a WriteUnitOfWork.
 *
 * It needs to be registered with registerChangeForCatalogVisibility so other commit handlers can
 * still write to this Collection.
 */
class CollectionCatalog::PublishCatalogUpdates final : public RecoveryUnit::Change {
public:
    static constexpr size_t kNumStaticActions = 2;

    static void setCollectionInCatalog(CollectionCatalog& catalog,
                                       std::shared_ptr<Collection> collection) {
        catalog._collections = catalog._collections.set(collection->ns(), collection);
        catalog._catalog = catalog._catalog.set(collection->uuid(), collection);
        // TODO SERVER-64608 Use tenantID from ns
        auto dbIdPair = std::make_pair(TenantDatabaseName(boost::none, collection->ns().db()),
                                       collection->uuid());
        catalog._orderedCollections = catalog._orderedCollections.set(dbIdPair, collection);
    }

    PublishCatalogUpdates(OperationContext* opCtx,
                          UncommittedCatalogUpdates& uncommittedCatalogUpdates)
        : _opCtx(opCtx), _uncommittedCatalogUpdates(uncommittedCatalogUpdates) {}

    static void ensureRegisteredWithRecoveryUnit(
        OperationContext* opCtx, UncommittedCatalogUpdates& UncommittedCatalogUpdates) {
        if (opCtx->recoveryUnit()->hasRegisteredChangeForCatalogVisibility())
            return;

        opCtx->recoveryUnit()->registerChangeForCatalogVisibility(
            std::make_unique<PublishCatalogUpdates>(opCtx, UncommittedCatalogUpdates));
    }

    void commit(boost::optional<Timestamp> commitTime) override {
        boost::container::small_vector<CollectionCatalog::CatalogWriteFn, kNumStaticActions>
            writeJobs;

        // Create catalog write jobs for all updates registered in this WriteUnitOfWork
        auto entries = _uncommittedCatalogUpdates.releaseEntries();
        for (auto&& entry : entries) {
            switch (entry.action) {
                case UncommittedCatalogUpdates::Entry::Action::kWritableCollection: {
                    writeJobs.push_back(
                        [collection = std::move(entry.collection)](CollectionCatalog& catalog) {
                            setCollectionInCatalog(catalog, std::move(collection));
                        });
                    break;
                }
                case UncommittedCatalogUpdates::Entry::Action::kRenamedCollection: {
                    writeJobs.push_back(
                        [& from = entry.nss, &to = entry.renameTo](CollectionCatalog& catalog) {
                            catalog._collections = catalog._collections.erase(from);

                            auto fromStr = from.ns();
                            auto toStr = to.ns();

                            ResourceId oldRid = ResourceId(RESOURCE_COLLECTION, fromStr);
                            ResourceId newRid = ResourceId(RESOURCE_COLLECTION, toStr);

                            catalog.removeResource(oldRid, fromStr);
                            catalog.addResource(newRid, toStr);
                        });
                    break;
                }
                case UncommittedCatalogUpdates::Entry::Action::kDroppedCollection: {
                    writeJobs.push_back(
                        [opCtx = _opCtx, uuid = *entry.uuid()](CollectionCatalog& catalog) {
                            catalog.deregisterCollection(opCtx, uuid);
                        });
                    break;
                }
                case UncommittedCatalogUpdates::Entry::Action::kRecreatedCollection: {
                    writeJobs.push_back([opCtx = _opCtx, collection = entry.collection](
                                            CollectionCatalog& catalog) {
                        catalog.registerCollection(opCtx, std::move(collection));
                    });
                    // Fallthrough to the createCollection case to finish committing the collection.
                }
                case UncommittedCatalogUpdates::Entry::Action::kCreatedCollection: {
                    auto collPtr = entry.collection.get();

                    // By this point, we may or may not have reserved an oplog slot for the
                    // collection creation.
                    // For example, multi-document transactions will only reserve the oplog slot at
                    // commit time. As a result, we may or may not have a reliable value to use to
                    // set the new collection's minimum visible snapshot until commit time.
                    // Pre-commit hooks do not presently have awareness of the commit timestamp, so
                    // we must update the minVisibleTimestamp with the appropriate value. This is
                    // fine because the collection should not be visible in the catalog until we
                    // call setCommitted(true).
                    if (commitTime) {
                        collPtr->setMinimumVisibleSnapshot(commitTime.get());
                    }
                    collPtr->setCommitted(true);
                    break;
                }
                case UncommittedCatalogUpdates::Entry::Action::kReplacedViewsForDatabase: {
                    writeJobs.push_back(
                        [dbName = entry.nss.db(),
                         &viewsForDb = entry.viewsForDb.get()](CollectionCatalog& catalog) {
                            catalog._replaceViewsForDatabase(dbName, std::move(viewsForDb));
                        });
                    break;
                }
                case UncommittedCatalogUpdates::Entry::Action::kAddViewResource: {
                    writeJobs.push_back([& viewName = entry.nss](CollectionCatalog& catalog) {
                        auto viewRid = ResourceId(RESOURCE_COLLECTION, viewName.ns());
                        catalog.addResource(viewRid, viewName.ns());
                        catalog.deregisterUncommittedView(viewName);
                    });
                    break;
                }
                case UncommittedCatalogUpdates::Entry::Action::kRemoveViewResource: {
                    writeJobs.push_back([& viewName = entry.nss](CollectionCatalog& catalog) {
                        auto viewRid = ResourceId(RESOURCE_COLLECTION, viewName.ns());
                        catalog.removeResource(viewRid, viewName.ns());
                    });
                    break;
                }
            };
        }

        // Write all catalog updates to the catalog in the same write to ensure atomicity.
        if (!writeJobs.empty()) {
            CollectionCatalog::write(_opCtx, [&writeJobs](CollectionCatalog& catalog) {
                for (auto&& job : writeJobs) {
                    job(catalog);
                }
            });
        }
    }

    void rollback() override {
        _uncommittedCatalogUpdates.releaseEntries();
    }

private:
    OperationContext* _opCtx;
    UncommittedCatalogUpdates& _uncommittedCatalogUpdates;
};

CollectionCatalog::iterator::iterator(const TenantDatabaseName& tenantDbName,
                                      OrderedCollectionMap::iterator it,
                                      const OrderedCollectionMap& map)
    : _map{map}, _mapIter{it}, _end(_map.upper_bound(std::make_pair(tenantDbName, maxUuid))) {
    _skipUncommitted();
}

CollectionCatalog::iterator::value_type CollectionCatalog::iterator::operator*() {
    if (_mapIter == _map.end()) {
        return nullptr;
    }
    return _mapIter->second.get();
}

CollectionCatalog::iterator CollectionCatalog::iterator::operator++() {
    invariant(_mapIter != _map.end());
    invariant(_mapIter != _end);
    _mapIter++;
    _skipUncommitted();
    return *this;
}

bool CollectionCatalog::iterator::operator==(const iterator& other) const {
    invariant(_map == other._map);

    if (other._mapIter == other._map.end()) {
        return _mapIter == _map.end();
    } else if (_mapIter == _map.end()) {
        return other._mapIter == other._map.end();
    }

    return _mapIter->first.second == other._mapIter->first.second;
}

bool CollectionCatalog::iterator::operator!=(const iterator& other) const {
    return !(*this == other);
}

void CollectionCatalog::iterator::_skipUncommitted() {
    // Advance to the next collection that is visible outside of its transaction.
    while (_mapIter != _end && !_mapIter->second->isCommitted()) {
        ++_mapIter;
    }
}

CollectionCatalog::Range::Range(const OrderedCollectionMap& map,
                                const TenantDatabaseName& tenantDbName)
    : _map{map}, _tenantDbName{tenantDbName} {}

CollectionCatalog::iterator CollectionCatalog::Range::begin() const {
    return {_tenantDbName, _map.lower_bound(std::make_pair(_tenantDbName, minUuid)), _map};
}

CollectionCatalog::iterator CollectionCatalog::Range::end() const {
    return {_tenantDbName, _map.upper_bound(std::make_pair(_tenantDbName, maxUuid)), _map};
}

bool CollectionCatalog::Range::empty() const {
    return begin() == end();
}

std::shared_ptr<const CollectionCatalog> CollectionCatalog::get(ServiceContext* svcCtx) {
    return atomic_load(&getCatalog(svcCtx).catalog);
}

std::shared_ptr<const CollectionCatalog> CollectionCatalog::get(OperationContext* opCtx) {
    // If there is a batched catalog write ongoing and we are the one doing it return this instance
    // so we can observe our own writes. There may be other callers that reads the CollectionCatalog
    // without any locks, they must see the immutable regular instance.
    if (batchedCatalogWriteInstance && opCtx->lockState()->isW()) {
        return batchedCatalogWriteInstance;
    }

    const auto& stashed = stashedCatalog(opCtx);
    if (stashed)
        return stashed;
    return get(opCtx->getServiceContext());
}

void CollectionCatalog::stash(OperationContext* opCtx,
                              std::shared_ptr<const CollectionCatalog> catalog) {
    stashedCatalog(opCtx) = std::move(catalog);
}

void CollectionCatalog::write(ServiceContext* svcCtx, CatalogWriteFn job) {
    // We should never have ongoing batching here. When batching is in progress the caller should
    // use the overload with OperationContext so we can verify that the global exlusive lock is
    // being held.
    invariant(!batchedCatalogWriteInstance);

    // It is potentially expensive to copy the collection catalog so we batch the operations by only
    // having one concurrent thread copying the catalog and executing all the write jobs.

    struct JobEntry {
        JobEntry(CatalogWriteFn write) : job(std::move(write)) {}

        CatalogWriteFn job;

        struct CompletionInfo {
            // Used to wait for job to complete by worker thread
            Mutex mutex;
            stdx::condition_variable cv;

            // Exception storage if we threw during job execution, so we can transfer the exception
            // back to the calling thread
            std::exception_ptr exception;

            // The job is completed when the catalog we modified has been committed back to the
            // storage or if we threw during its execution
            bool completed = false;
        };

        // Shared state for completion info as JobEntry's gets deleted when we are finished
        // executing. No shared state means that this job belongs to the same thread executing them.
        std::shared_ptr<CompletionInfo> completion;
    };

    static std::list<JobEntry> queue;
    static bool workerExists = false;
    static Mutex mutex =
        MONGO_MAKE_LATCH("CollectionCatalog::write");  // Protecting the two globals above

    invariant(job);

    // Current batch of jobs to execute
    std::list<JobEntry> pending;
    {
        stdx::unique_lock lock(mutex);
        queue.emplace_back(std::move(job));

        // If worker already exists, then wait on our condition variable until the job is completed
        if (workerExists) {
            auto completion = std::make_shared<JobEntry::CompletionInfo>();
            queue.back().completion = completion;
            lock.unlock();

            stdx::unique_lock completionLock(completion->mutex);
            const bool& completed = completion->completed;
            completion->cv.wait(completionLock, [&completed]() { return completed; });

            // Throw any exception that was caught during execution of our job. Make sure we destroy
            // the exception_ptr on the same thread that throws the exception to avoid a data race
            // between destroying the exception_ptr and reading the exception.
            auto ex = std::move(completion->exception);
            if (ex)
                std::rethrow_exception(ex);
            return;
        }

        // No worker existed, then we take this responsibility
        workerExists = true;
        pending.splice(pending.end(), queue);
    }

    // Implementation for thread with worker responsibility below, only one thread at a time can be
    // in here. Keep track of completed jobs so we can notify them when we've written back the
    // catalog to storage
    std::list<JobEntry> completed;
    std::exception_ptr myException;

    auto& storage = getCatalog(svcCtx);
    // hold onto base so if we need to delete it we can do it outside of the lock
    auto base = atomic_load(&storage.catalog);
    // copy the collection catalog, this could be expensive, but we will only have one pending
    // collection in flight at a given time
    auto clone = std::make_shared<CollectionCatalog>(*base);

    // Execute jobs until we drain the queue
    while (true) {
        for (auto&& current : pending) {
            // Store any exception thrown during job execution so we can notify the calling thread
            try {
                current.job(*clone);
            } catch (...) {
                if (current.completion)
                    current.completion->exception = std::current_exception();
                else
                    myException = std::current_exception();
            }
        }
        // Transfer the jobs we just executed to the completed list
        completed.splice(completed.end(), pending);

        stdx::lock_guard lock(mutex);
        if (queue.empty()) {
            // Queue is empty, store catalog and relinquish responsibility of being worker thread
            atomic_store(&storage.catalog, std::move(clone));
            workerExists = false;
            break;
        }

        // Transfer jobs in queue to the pending list
        pending.splice(pending.end(), queue);
    }

    for (auto&& entry : completed) {
        if (!entry.completion) {
            continue;
        }

        stdx::lock_guard completionLock(entry.completion->mutex);
        entry.completion->completed = true;
        entry.completion->cv.notify_one();
    }
    LOGV2_DEBUG(
        5255601, 1, "Finished writing to the CollectionCatalog", "jobs"_attr = completed.size());
    if (myException)
        std::rethrow_exception(myException);
}

void CollectionCatalog::write(OperationContext* opCtx,
                              std::function<void(CollectionCatalog&)> job) {
    // Calling the writer must be done with the GlobalLock held. Otherwise we risk having the
    // BatchedCollectionCatalogWriter and this caller concurrently modifying the catalog. This is
    // because normal operations calling this will all be serialized, but
    // BatchedCollectionCatalogWriter skips this mechanism as it knows it is the sole user of the
    // server by holding a Global MODE_X lock.
    invariant(opCtx->lockState()->isNoop() || opCtx->lockState()->isLocked());

    // If global MODE_X lock are held we can re-use a cloned CollectionCatalog instance when
    // 'batchedCatalogWriteInstance' is set. Make sure we are the one holding the write lock.
    if (batchedCatalogWriteInstance) {
        invariant(opCtx->lockState()->isW());
        job(*batchedCatalogWriteInstance);
        return;
    }

    write(opCtx->getServiceContext(), std::move(job));
}

Status CollectionCatalog::createView(OperationContext* opCtx,
                                     const NamespaceString& viewName,
                                     const NamespaceString& viewOn,
                                     const BSONArray& pipeline,
                                     const BSONObj& collation,
                                     const ViewsForDatabase::PipelineValidatorFn& pipelineValidator,
                                     const ViewUpsertMode insertViewMode) const {
    // A view document direct write can occur via the oplog application path, which may only hold a
    // lock on the collection being updated (the database views collection).
    invariant(insertViewMode == ViewUpsertMode::kAlreadyDurableView ||
              opCtx->lockState()->isCollectionLockedForMode(viewName, MODE_IX));
    invariant(opCtx->lockState()->isCollectionLockedForMode(
        NamespaceString(viewName.db(), NamespaceString::kSystemDotViewsCollectionName), MODE_X));

    invariant(_viewsForDatabase.find(viewName.db()));
    const ViewsForDatabase& viewsForDb = *_getViewsForDatabase(opCtx, viewName.db());

    auto& uncommittedCatalogUpdates = UncommittedCatalogUpdates::get(opCtx);
    if (uncommittedCatalogUpdates.shouldIgnoreExternalViewChanges(viewName.db())) {
        return Status::OK();
    }

    if (viewName.db() != viewOn.db())
        return Status(ErrorCodes::BadValue,
                      "View must be created on a view or collection in the same database");

    if (viewsForDb.lookup(viewName) || _collections.find(viewName))
        return Status(ErrorCodes::NamespaceExists, "Namespace already exists");

    if (!NamespaceString::validCollectionName(viewOn.coll()))
        return Status(ErrorCodes::InvalidNamespace,
                      str::stream() << "invalid name for 'viewOn': " << viewOn.coll());

    auto collator = ViewsForDatabase::parseCollator(opCtx, collation);
    if (!collator.isOK())
        return collator.getStatus();

    Status result = Status::OK();
    {
        IgnoreExternalViewChangesForDatabase ignore(opCtx, viewName.db());

        result = _createOrUpdateView(opCtx,
                                     viewName,
                                     viewOn,
                                     pipeline,
                                     pipelineValidator,
                                     std::move(collator.getValue()),
                                     ViewsForDatabase{viewsForDb},
                                     insertViewMode);
    }

    return result;
}

Status CollectionCatalog::modifyView(
    OperationContext* opCtx,
    const NamespaceString& viewName,
    const NamespaceString& viewOn,
    const BSONArray& pipeline,
    const ViewsForDatabase::PipelineValidatorFn& pipelineValidator) const {
    invariant(opCtx->lockState()->isCollectionLockedForMode(viewName, MODE_X));
    invariant(opCtx->lockState()->isCollectionLockedForMode(
        NamespaceString(viewName.db(), NamespaceString::kSystemDotViewsCollectionName), MODE_X));

    invariant(_viewsForDatabase.find(viewName.db()));
    const ViewsForDatabase& viewsForDb = *_getViewsForDatabase(opCtx, viewName.db());

    if (viewName.db() != viewOn.db())
        return Status(ErrorCodes::BadValue,
                      "View must be created on a view or collection in the same database");

    auto viewPtr = viewsForDb.lookup(viewName);
    if (!viewPtr)
        return Status(ErrorCodes::NamespaceNotFound,
                      str::stream() << "cannot modify missing view " << viewName.ns());

    if (!NamespaceString::validCollectionName(viewOn.coll()))
        return Status(ErrorCodes::InvalidNamespace,
                      str::stream() << "invalid name for 'viewOn': " << viewOn.coll());

    Status result = Status::OK();
    {
        IgnoreExternalViewChangesForDatabase ignore(opCtx, viewName.db());

        result = _createOrUpdateView(opCtx,
                                     viewName,
                                     viewOn,
                                     pipeline,
                                     pipelineValidator,
                                     CollatorInterface::cloneCollator(viewPtr->defaultCollator()),
                                     ViewsForDatabase{viewsForDb},
                                     ViewUpsertMode::kUpdateView);
    }

    return result;
}

Status CollectionCatalog::dropView(OperationContext* opCtx, const NamespaceString& viewName) const {
    invariant(opCtx->lockState()->isCollectionLockedForMode(viewName, MODE_IX));
    invariant(opCtx->lockState()->isCollectionLockedForMode(
        NamespaceString(viewName.db(), NamespaceString::kSystemDotViewsCollectionName), MODE_X));

    invariant(_viewsForDatabase.find(viewName.db()));
    const ViewsForDatabase& viewsForDb = *_getViewsForDatabase(opCtx, viewName.db());
    viewsForDb.requireValidCatalog();

    // Make sure the view exists before proceeding.
    if (!viewsForDb.lookup(viewName)) {
        return {ErrorCodes::NamespaceNotFound,
                str::stream() << "cannot drop missing view: " << viewName.ns()};
    }

    Status result = Status::OK();
    {
        IgnoreExternalViewChangesForDatabase ignore(opCtx, viewName.db());

        ViewsForDatabase writable{viewsForDb};

        writable.durable->remove(opCtx, viewName);
        writable.viewGraph.remove(viewName);
        writable.viewMap.erase(viewName.ns());
        writable.stats = {};

        // Reload the view catalog with the changes applied.
        result = writable.reload(opCtx);
        if (result.isOK()) {
            auto& uncommittedCatalogUpdates = UncommittedCatalogUpdates::get(opCtx);
            uncommittedCatalogUpdates.removeView(viewName);
            uncommittedCatalogUpdates.replaceViewsForDatabase(viewName.db(), std::move(writable));

            PublishCatalogUpdates::ensureRegisteredWithRecoveryUnit(opCtx,
                                                                    uncommittedCatalogUpdates);
        }
    }

    return result;
}

Status CollectionCatalog::reloadViews(OperationContext* opCtx, StringData dbName) const {
    invariant(opCtx->lockState()->isCollectionLockedForMode(
        NamespaceString(dbName, NamespaceString::kSystemDotViewsCollectionName), MODE_IS));

    auto& uncommittedCatalogUpdates = UncommittedCatalogUpdates::get(opCtx);
    if (uncommittedCatalogUpdates.shouldIgnoreExternalViewChanges(dbName)) {
        return Status::OK();
    }

    LOGV2_DEBUG(22546, 1, "Reloading view catalog for database", "db"_attr = dbName);

    // Create a copy of the ViewsForDatabase instance to modify it. Reset the views for this
    // database, but preserve the DurableViewCatalog pointer.
    const ViewsForDatabase* viewsForDbPtr = _viewsForDatabase.find(dbName);
    invariant(viewsForDbPtr);
    ViewsForDatabase viewsForDb = *viewsForDbPtr;

    viewsForDb.valid = false;
    viewsForDb.viewGraphNeedsRefresh = true;
    viewsForDb.viewMap.clear();
    viewsForDb.stats = {};

    auto status = viewsForDb.reload(opCtx);
    CollectionCatalog::write(opCtx, [&](CollectionCatalog& catalog) {
        catalog._replaceViewsForDatabase(dbName, std::move(viewsForDb));
    });

    return status;
}

void CollectionCatalog::onCreateCollection(OperationContext* opCtx,
                                           std::shared_ptr<Collection> coll) const {
    invariant(coll);

    auto& uncommittedCatalogUpdates = UncommittedCatalogUpdates::get(opCtx);
    auto [found, existingColl, newColl] =
        UncommittedCatalogUpdates::lookupCollection(opCtx, coll->ns());
    uassert(31370,
            str::stream() << "collection already exists. ns: " << coll->ns(),
            existingColl == nullptr);

    // When we already have a drop and recreate the collection, we want to seamlessly swap out the
    // collection in the catalog under a single critical section. So we register the recreated
    // collection in the same commit handler that we unregister the dropped collection (as opposed
    // to registering the new collection inside of a preCommitHook).
    if (found) {
        uncommittedCatalogUpdates.recreateCollection(opCtx, std::move(coll));
    } else {
        uncommittedCatalogUpdates.createCollection(opCtx, std::move(coll));
    }

    PublishCatalogUpdates::ensureRegisteredWithRecoveryUnit(opCtx, uncommittedCatalogUpdates);
}

void CollectionCatalog::onCollectionRename(OperationContext* opCtx,
                                           Collection* coll,
                                           const NamespaceString& fromCollection) const {
    invariant(coll);

    auto& uncommittedCatalogUpdates = UncommittedCatalogUpdates::get(opCtx);
    uncommittedCatalogUpdates.renameCollection(coll, fromCollection);
}

void CollectionCatalog::dropCollection(OperationContext* opCtx, Collection* coll) const {
    invariant(coll);

    auto& uncommittedCatalogUpdates = UncommittedCatalogUpdates::get(opCtx);
    uncommittedCatalogUpdates.dropCollection(coll);

    // Requesting a writable collection normally ensures we have registered PublishCatalogUpdates
    // with the recovery unit. However, when the writable Collection was requested in Inplace mode
    // (or is the oplog) this is not the case. So make sure we are registered in all cases.
    PublishCatalogUpdates::ensureRegisteredWithRecoveryUnit(opCtx, uncommittedCatalogUpdates);
}

void CollectionCatalog::onOpenDatabase(OperationContext* opCtx,
                                       StringData dbName,
                                       ViewsForDatabase&& viewsForDb) {
    invariant(opCtx->lockState()->isDbLockedForMode(dbName, MODE_IS));
    uassert(ErrorCodes::AlreadyInitialized,
            str::stream() << "Database " << dbName << " is already initialized",
            !_viewsForDatabase.find(dbName));

    _viewsForDatabase = _viewsForDatabase.set(dbName.toString(), std::move(viewsForDb));
}

void CollectionCatalog::onCloseDatabase(OperationContext* opCtx, TenantDatabaseName tenantDbName) {
    invariant(opCtx->lockState()->isDbLockedForMode(tenantDbName.dbName(), MODE_X));
    auto rid = ResourceId(RESOURCE_DATABASE, tenantDbName.dbName());
    removeResource(rid, tenantDbName.dbName());
    _viewsForDatabase = _viewsForDatabase.erase(tenantDbName.dbName());
}

void CollectionCatalog::onCloseCatalog() {
    if (_shadowCatalog) {
        return;
    }

    _shadowCatalog.emplace();
    for (auto& entry : _catalog)
        _shadowCatalog = _shadowCatalog->insert({entry.first, entry.second->ns()});
}

void CollectionCatalog::onOpenCatalog() {
    invariant(_shadowCatalog);
    _shadowCatalog.reset();
    ++_epoch;
}

uint64_t CollectionCatalog::getEpoch() const {
    return _epoch;
}

CollectionCatalog::Range CollectionCatalog::range(const TenantDatabaseName& tenantDbName) const {
    return {_orderedCollections, tenantDbName};
}

std::shared_ptr<const Collection> CollectionCatalog::lookupCollectionByUUIDForRead(
    OperationContext* opCtx, const UUID& uuid) const {
    auto [found, uncommittedColl, newColl] =
        UncommittedCatalogUpdates::lookupCollection(opCtx, uuid);
    if (uncommittedColl) {
        return uncommittedColl;
    }

    auto coll = _lookupCollectionByUUID(uuid);
    return (coll && coll->isCommitted()) ? coll : nullptr;
}

Collection* CollectionCatalog::lookupCollectionByUUIDForMetadataWrite(OperationContext* opCtx,
                                                                      const UUID& uuid) const {
    auto& uncommittedCatalogUpdates = UncommittedCatalogUpdates::get(opCtx);
    auto [found, uncommittedPtr, newColl] =
        UncommittedCatalogUpdates::lookupCollection(opCtx, uuid);
    if (found) {
        // The uncommittedPtr will be nullptr in the case of drop.
        if (!uncommittedPtr.get()) {
            return nullptr;
        }

        auto nss = uncommittedPtr->ns();
        // If the collection is newly created, invariant on the collection being locked in MODE_IX.
        invariant(!newColl || opCtx->lockState()->isCollectionLockedForMode(nss, MODE_IX),
                  nss.toString());
        return uncommittedPtr.get();
    }

    std::shared_ptr<Collection> coll = _lookupCollectionByUUID(uuid);

    if (!coll || !coll->isCommitted())
        return nullptr;

    if (coll->ns().isOplog())
        return coll.get();

    invariant(opCtx->lockState()->isCollectionLockedForMode(coll->ns(), MODE_X));

    // Skip cloning and return directly if allowed.
    if (_alreadyClonedForBatchedWriter(coll)) {
        return coll.get();
    }

    auto cloned = coll->clone();
    auto ptr = cloned.get();

    // If we are in a batch write, set this Collection instance in the batched catalog write
    // instance. We don't want to store as uncommitted in this case as we need to observe the write
    // on the thread doing the batch write and it would trigger the regular path where we do a
    // copy-on-write on the catalog when committing.
    if (_isCatalogBatchWriter()) {
        batchedCatalogClonedCollections.emplace(cloned.get());
        PublishCatalogUpdates::setCollectionInCatalog(*batchedCatalogWriteInstance,
                                                      std::move(cloned));
        return ptr;
    }

    uncommittedCatalogUpdates.writableCollection(std::move(cloned));

    PublishCatalogUpdates::ensureRegisteredWithRecoveryUnit(opCtx, uncommittedCatalogUpdates);

    return ptr;
}

CollectionPtr CollectionCatalog::lookupCollectionByUUID(OperationContext* opCtx, UUID uuid) const {
    // If UUID is managed by UncommittedCatalogUpdates (but not newly created) return the pointer
    // which will be nullptr in case of a drop.
    auto [found, uncommittedPtr, newColl] =
        UncommittedCatalogUpdates::lookupCollection(opCtx, uuid);
    if (found) {
        return uncommittedPtr.get();
    }

    auto coll = _lookupCollectionByUUID(uuid);
    return (coll && coll->isCommitted())
        ? CollectionPtr(opCtx, coll.get(), LookupCollectionForYieldRestore(coll->ns()))
        : CollectionPtr();
}

bool CollectionCatalog::isCollectionAwaitingVisibility(UUID uuid) const {
    auto coll = _lookupCollectionByUUID(uuid);
    return coll && !coll->isCommitted();
}

std::shared_ptr<Collection> CollectionCatalog::_lookupCollectionByUUID(UUID uuid) const {
    const std::shared_ptr<Collection>* coll = _catalog.find(uuid);
    return coll ? *coll : nullptr;
}

std::shared_ptr<const Collection> CollectionCatalog::lookupCollectionByNamespaceForRead(
    OperationContext* opCtx, const NamespaceString& nss) const {

    auto [found, uncommittedColl, newColl] =
        UncommittedCatalogUpdates::lookupCollection(opCtx, nss);
    if (uncommittedColl) {
        return uncommittedColl;
    }

    // Report the drop or rename as nothing new was created.
    if (found) {
        return nullptr;
    }

    const std::shared_ptr<Collection>* collPtr = _collections.find(nss);
    auto coll = collPtr ? *collPtr : nullptr;
    return (coll && coll->isCommitted()) ? coll : nullptr;
}

Collection* CollectionCatalog::lookupCollectionByNamespaceForMetadataWrite(
    OperationContext* opCtx, const NamespaceString& nss) const {
    // Oplog is special and can only be modified in a few contexts. It is modified inplace and care
    // need to be taken for concurrency.
    if (nss.isOplog()) {
        return const_cast<Collection*>(lookupCollectionByNamespace(opCtx, nss).get());
    }

    auto& uncommittedCatalogUpdates = UncommittedCatalogUpdates::get(opCtx);
    auto [found, uncommittedPtr, newColl] = UncommittedCatalogUpdates::lookupCollection(opCtx, nss);


    // If uncommittedPtr is valid, found is always true. Return the pointer as the collection still
    // exists.
    if (uncommittedPtr) {
        // If the collection is newly created, invariant on the collection being locked in MODE_IX.
        invariant(!newColl || opCtx->lockState()->isCollectionLockedForMode(nss, MODE_IX),
                  nss.toString());
        return uncommittedPtr.get();
    }

    // Report the drop or rename as nothing new was created.
    if (found) {
        return nullptr;
    }

    const std::shared_ptr<Collection>* collPtr = _collections.find(nss);
    auto coll = collPtr ? *collPtr : nullptr;

    if (!coll || !coll->isCommitted())
        return nullptr;

    invariant(opCtx->lockState()->isCollectionLockedForMode(nss, MODE_X));

    // Skip cloning and return directly if allowed.
    if (_alreadyClonedForBatchedWriter(coll)) {
        return coll.get();
    }

    auto cloned = coll->clone();
    auto ptr = cloned.get();

    // If we are in a batch write, set this Collection instance in the batched catalog write
    // instance. We don't want to store as uncommitted in this case as we need to observe the write
    // on the thread doing the batch write and it would trigger the regular path where we do a
    // copy-on-write on the catalog when committing.
    if (_isCatalogBatchWriter()) {
        batchedCatalogClonedCollections.emplace(cloned.get());
        PublishCatalogUpdates::setCollectionInCatalog(*batchedCatalogWriteInstance,
                                                      std::move(cloned));
        return ptr;
    }

    uncommittedCatalogUpdates.writableCollection(std::move(cloned));

    PublishCatalogUpdates::ensureRegisteredWithRecoveryUnit(opCtx, uncommittedCatalogUpdates);

    return ptr;
}

CollectionPtr CollectionCatalog::lookupCollectionByNamespace(OperationContext* opCtx,
                                                             const NamespaceString& nss) const {
    // If uncommittedPtr is valid, found is always true. Return the pointer as the collection still
    // exists.
    auto [found, uncommittedPtr, newColl] = UncommittedCatalogUpdates::lookupCollection(opCtx, nss);
    if (uncommittedPtr) {
        return uncommittedPtr.get();
    }

    // Report the drop or rename as nothing new was created.
    if (found) {
        return nullptr;
    }

    const std::shared_ptr<Collection>* collPtr = _collections.find(nss);
    auto coll = collPtr ? *collPtr : nullptr;
    return (coll && coll->isCommitted())
        ? CollectionPtr(opCtx, coll.get(), LookupCollectionForYieldRestore(coll->ns()))
        : nullptr;
}

boost::optional<NamespaceString> CollectionCatalog::lookupNSSByUUID(OperationContext* opCtx,
                                                                    const UUID& uuid) const {
    auto [found, uncommittedPtr, newColl] =
        UncommittedCatalogUpdates::lookupCollection(opCtx, uuid);
    // If UUID is managed by uncommittedCatalogUpdates return its corresponding namespace if the
    // Collection exists, boost::none otherwise.
    if (found) {
        if (uncommittedPtr)
            return uncommittedPtr->ns();
        return boost::none;
    }

    const std::shared_ptr<Collection>* collPtr = _catalog.find(uuid);
    if (collPtr) {
        auto coll = *collPtr;
        boost::optional<NamespaceString> ns = coll->ns();
        invariant(!ns.value().isEmpty());
        return coll->isCommitted() ? ns : boost::none;
    }

    // Only in the case that the catalog is closed and a UUID is currently unknown, resolve it
    // using the pre-close state. This ensures that any tasks reloading the catalog can see their
    // own updates.
    if (_shadowCatalog) {
        auto* shadowIt = _shadowCatalog->find(uuid);
        if (shadowIt)
            return *shadowIt;
    }
    return boost::none;
}

boost::optional<UUID> CollectionCatalog::lookupUUIDByNSS(OperationContext* opCtx,
                                                         const NamespaceString& nss) const {
    auto [found, uncommittedPtr, newColl] = UncommittedCatalogUpdates::lookupCollection(opCtx, nss);
    if (uncommittedPtr) {
        return uncommittedPtr->uuid();
    }

    if (found) {
        return boost::none;
    }

    const std::shared_ptr<Collection>* collPtr = _collections.find(nss);
    if (collPtr) {
        auto coll = *collPtr;
        const boost::optional<UUID>& uuid = coll->uuid();
        return coll->isCommitted() ? uuid : boost::none;
    }
    return boost::none;
}

void CollectionCatalog::iterateViews(OperationContext* opCtx,
                                     StringData dbName,
                                     ViewIteratorCallback callback,
                                     ViewCatalogLookupBehavior lookupBehavior) const {
    auto viewsForDb = _getViewsForDatabase(opCtx, dbName);
    if (!viewsForDb) {
        return;
    }

    if (lookupBehavior != ViewCatalogLookupBehavior::kAllowInvalidViews) {
        viewsForDb->requireValidCatalog();
    }

    for (auto&& view : viewsForDb->viewMap) {
        if (!callback(*view.second)) {
            break;
        }
    }
}

std::shared_ptr<const ViewDefinition> CollectionCatalog::lookupView(
    OperationContext* opCtx, const NamespaceString& ns) const {
    auto viewsForDb = _getViewsForDatabase(opCtx, ns.db());
    if (!viewsForDb) {
        return nullptr;
    }

    if (!viewsForDb->valid && opCtx->getClient()->isFromUserConnection()) {
        // We want to avoid lookups on invalid collection names.
        if (!NamespaceString::validCollectionName(ns.ns())) {
            return nullptr;
        }

        // ApplyOps should work on a valid existing collection, despite the presence of bad views
        // otherwise the server would crash. The view catalog will remain invalid until the bad view
        // definitions are removed.
        viewsForDb->requireValidCatalog();
    }

    return viewsForDb->lookup(ns);
}

std::shared_ptr<const ViewDefinition> CollectionCatalog::lookupViewWithoutValidatingDurable(
    OperationContext* opCtx, const NamespaceString& ns) const {
    auto viewsForDb = _getViewsForDatabase(opCtx, ns.db());
    if (!viewsForDb) {
        return nullptr;
    }

    return viewsForDb->lookup(ns);
}

NamespaceString CollectionCatalog::resolveNamespaceStringOrUUID(
    OperationContext* opCtx, NamespaceStringOrUUID nsOrUUID) const {
    if (auto& nss = nsOrUUID.nss()) {
        uassert(ErrorCodes::InvalidNamespace,
                str::stream() << "Namespace " << *nss << " is not a valid collection name",
                nss->isValid());
        return std::move(*nss);
    }

    auto resolvedNss = lookupNSSByUUID(opCtx, *nsOrUUID.uuid());

    uassert(ErrorCodes::NamespaceNotFound,
            str::stream() << "Unable to resolve " << nsOrUUID.toString(),
            resolvedNss && resolvedNss->isValid());

    uassert(ErrorCodes::NamespaceNotFound,
            str::stream() << "UUID " << nsOrUUID.toString() << " specified in " << nsOrUUID.dbname()
                          << " resolved to a collection in a different database: " << *resolvedNss,
            resolvedNss->db() == nsOrUUID.dbname());

    return std::move(*resolvedNss);
}

bool CollectionCatalog::checkIfCollectionSatisfiable(UUID uuid, CollectionInfoFn predicate) const {
    invariant(predicate);

    auto collection = _lookupCollectionByUUID(uuid);

    if (!collection) {
        return false;
    }

    return predicate(collection.get());
}

std::vector<UUID> CollectionCatalog::getAllCollectionUUIDsFromDb(
    const TenantDatabaseName& tenantDbName) const {
    auto minUuid = UUID::parse("00000000-0000-0000-0000-000000000000").getValue();
    auto it = _orderedCollections.lower_bound(std::make_pair(tenantDbName, minUuid));

    std::vector<UUID> ret;
    while (it != _orderedCollections.end() && it->first.first == tenantDbName) {
        if (it->second->isCommitted()) {
            ret.push_back(it->first.second);
        }
        ++it;
    }
    return ret;
}

std::vector<NamespaceString> CollectionCatalog::getAllCollectionNamesFromDb(
    OperationContext* opCtx, const TenantDatabaseName& tenantDbName) const {
    invariant(opCtx->lockState()->isDbLockedForMode(tenantDbName.dbName(), MODE_S));

    auto minUuid = UUID::parse("00000000-0000-0000-0000-000000000000").getValue();

    std::vector<NamespaceString> ret;
    for (auto it = _orderedCollections.lower_bound(std::make_pair(tenantDbName, minUuid));
         it != _orderedCollections.end() && it->first.first == tenantDbName;
         ++it) {
        if (it->second->isCommitted()) {
            ret.push_back(it->second->ns());
        }
    }
    return ret;
}

std::vector<TenantDatabaseName> CollectionCatalog::getAllDbNames() const {
    std::vector<TenantDatabaseName> ret;
    auto maxUuid = UUID::parse("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF").getValue();
    auto iter = _orderedCollections.upper_bound(std::make_pair(TenantDatabaseName(), maxUuid));
    while (iter != _orderedCollections.end()) {
        auto tenantDbName = iter->first.first;
        if (iter->second->isCommitted()) {
            ret.push_back(tenantDbName);
        } else {
            // If the first collection found for `tenantDbName` is not yet committed, increment the
            // iterator to find the next visible collection (possibly under a different
            // `tenantDbName`).
            iter++;
            continue;
        }
        // Move on to the next database after `tenantDbName`.
        iter = _orderedCollections.upper_bound(std::make_pair(tenantDbName, maxUuid));
    }
    return ret;
}

void CollectionCatalog::setAllDatabaseProfileFilters(std::shared_ptr<ProfileFilter> filter) {
    auto dbProfileSettingsWriter = _databaseProfileSettings.transient();
    for (const auto& [dbName, settings] : _databaseProfileSettings) {
        ProfileSettings clone = settings;
        clone.filter = filter;
        dbProfileSettingsWriter.set(dbName, std::move(clone));
    }
    _databaseProfileSettings = dbProfileSettingsWriter.persistent();
}

void CollectionCatalog::setDatabaseProfileSettings(
    StringData dbName, CollectionCatalog::ProfileSettings newProfileSettings) {
    _databaseProfileSettings =
        _databaseProfileSettings.set(dbName.toString(), std::move(newProfileSettings));
}

CollectionCatalog::ProfileSettings CollectionCatalog::getDatabaseProfileSettings(
    StringData dbName) const {
    const ProfileSettings* settings = _databaseProfileSettings.find(dbName);
    if (settings) {
        return *settings;
    }

    return {serverGlobalParams.defaultProfile, ProfileFilter::getDefault()};
}

void CollectionCatalog::clearDatabaseProfileSettings(StringData dbName) {
    _databaseProfileSettings = _databaseProfileSettings.erase(dbName.toString());
}

CollectionCatalog::Stats CollectionCatalog::getStats() const {
    return _stats;
}

boost::optional<ViewsForDatabase::Stats> CollectionCatalog::getViewStatsForDatabase(
    OperationContext* opCtx, StringData dbName) const {
    auto viewsForDb = _getViewsForDatabase(opCtx, dbName);
    if (!viewsForDb) {
        return boost::none;
    }
    return viewsForDb->stats;
}

CollectionCatalog::ViewCatalogSet CollectionCatalog::getViewCatalogDbNames(
    OperationContext* opCtx) const {
    ViewCatalogSet results;
    for (const auto& dbNameViewSetPair : _viewsForDatabase) {
        // TODO (SERVER-63206): Return stored TenantDatabaseName
        results.insert(TenantDatabaseName{boost::none, dbNameViewSetPair.first});
    }

    return results;
}

void CollectionCatalog::registerCollection(OperationContext* opCtx,
                                           std::shared_ptr<Collection> coll) {
    auto nss = coll->ns();
    auto uuid = coll->uuid();
    // TODO SERVER-64608 Use tenantId from nss
    auto tenantDbName = TenantDatabaseName(boost::none, nss.db());
    _ensureNamespaceDoesNotExist(opCtx, nss, NamespaceType::kAll);

    LOGV2_DEBUG(20280,
                1,
                "Registering collection {namespace} with UUID {uuid}",
                "Registering collection",
                logAttrs(nss),
                "uuid"_attr = uuid);

    auto dbIdPair = std::make_pair(tenantDbName, uuid);

    // Make sure no entry related to this uuid.
    invariant(!_catalog.find(uuid));
    invariant(_orderedCollections.find(dbIdPair) == _orderedCollections.end());

    _catalog = _catalog.set(uuid, coll);
    _collections = _collections.set(nss, coll);
    _orderedCollections = _orderedCollections.set(dbIdPair, coll);

    if (!nss.isOnInternalDb() && !nss.isSystem()) {
        _stats.userCollections += 1;
        if (coll->isCapped()) {
            _stats.userCapped += 1;
        }
        if (coll->isClustered()) {
            _stats.userClustered += 1;
        }
    } else {
        _stats.internal += 1;
    }

    invariant(static_cast<size_t>(_stats.internal + _stats.userCollections) == _collections.size());

    // TODO SERVER-62918 create ResourceId for db with TenantDatabaseName.
    auto dbRid = ResourceId(RESOURCE_DATABASE, tenantDbName.dbName());
    addResource(dbRid, tenantDbName.dbName());

    auto collRid = ResourceId(RESOURCE_COLLECTION, nss.ns());
    addResource(collRid, nss.ns());
}

std::shared_ptr<Collection> CollectionCatalog::deregisterCollection(OperationContext* opCtx,
                                                                    const UUID& uuid) {
    invariant(_catalog.find(uuid));

    auto coll = std::move(_catalog[uuid]);
    auto ns = coll->ns();
    // TODO SERVER-64608 Use tenantID from ns
    auto tenantDbName = TenantDatabaseName(boost::none, coll->ns().db());
    auto dbIdPair = std::make_pair(tenantDbName, uuid);

    LOGV2_DEBUG(20281, 1, "Deregistering collection", logAttrs(ns), "uuid"_attr = uuid);

    // Make sure collection object exists.
    invariant(_collections.find(ns));
    invariant(_orderedCollections.find(dbIdPair) != _orderedCollections.end());

    _orderedCollections = _orderedCollections.erase(dbIdPair);
    _collections = _collections.erase(ns);
    _catalog = _catalog.erase(uuid);

    if (!ns.isOnInternalDb() && !ns.isSystem()) {
        _stats.userCollections -= 1;
        if (coll->isCapped()) {
            _stats.userCapped -= 1;
        }
        if (coll->isClustered()) {
            _stats.userClustered -= 1;
        }
    } else {
        _stats.internal -= 1;
    }

    invariant(static_cast<size_t>(_stats.internal + _stats.userCollections) == _collections.size());

    coll->onDeregisterFromCatalog(opCtx);

    auto collRid = ResourceId(RESOURCE_COLLECTION, ns.ns());
    removeResource(collRid, ns.ns());

    return coll;
}

void CollectionCatalog::registerUncommittedView(OperationContext* opCtx,
                                                const NamespaceString& nss) {
    invariant(opCtx->lockState()->isCollectionLockedForMode(
        NamespaceString(nss.db(), NamespaceString::kSystemDotViewsCollectionName), MODE_X));

    // Since writing to system.views requires an X lock, we only need to cross-check collection
    // namespaces here.
    _ensureNamespaceDoesNotExist(opCtx, nss, NamespaceType::kCollection);

    _uncommittedViews = _uncommittedViews.insert(nss);
}

void CollectionCatalog::deregisterUncommittedView(const NamespaceString& nss) {
    _uncommittedViews = _uncommittedViews.erase(nss);
}

void CollectionCatalog::_ensureNamespaceDoesNotExist(OperationContext* opCtx,
                                                     const NamespaceString& nss,
                                                     NamespaceType type) const {
    auto existingCollection = _collections.find(nss);
    if (existingCollection) {
        LOGV2(5725001,
              "Conflicted registering namespace, already have a collection with the same namespace",
              "nss"_attr = nss);
        throw WriteConflictException();
    }

    if (type == NamespaceType::kAll) {
        if (_uncommittedViews.find(nss)) {
            LOGV2(5725002,
                  "Conflicted registering namespace, already have a view with the same namespace",
                  "nss"_attr = nss);
            throw WriteConflictException();
        }

        if (auto viewsForDb = _getViewsForDatabase(opCtx, nss.db())) {
            if (viewsForDb->lookup(nss) != nullptr) {
                LOGV2(
                    5725003,
                    "Conflicted registering namespace, already have a view with the same namespace",
                    "nss"_attr = nss);
                uasserted(ErrorCodes::NamespaceExists,
                          "Conflicted registering namespace, already have a view with the same "
                          "namespace");
            }
        }
    }
}

void CollectionCatalog::deregisterAllCollectionsAndViews() {
    LOGV2(20282, "Deregistering all the collections");
    for (auto& entry : _catalog) {
        auto uuid = entry.first;
        auto ns = entry.second->ns();

        LOGV2_DEBUG(20283, 1, "Deregistering collection", logAttrs(ns), "uuid"_attr = uuid);
    }

    _collections = {};
    _orderedCollections = {};
    _catalog = {};
    _viewsForDatabase = {};
    _stats = {};

    _resourceInformation = {};
}

void CollectionCatalog::clearViews(OperationContext* opCtx, StringData dbName) const {
    invariant(opCtx->lockState()->isCollectionLockedForMode(
        NamespaceString(dbName, NamespaceString::kSystemDotViewsCollectionName), MODE_X));

    const ViewsForDatabase* viewsForDbPtr = _viewsForDatabase.find(dbName);
    invariant(viewsForDbPtr);

    ViewsForDatabase viewsForDb = *viewsForDbPtr;

    viewsForDb.viewMap.clear();
    viewsForDb.viewGraph.clear();
    viewsForDb.valid = true;
    viewsForDb.viewGraphNeedsRefresh = false;
    viewsForDb.stats = {};
    CollectionCatalog::write(opCtx, [&](CollectionCatalog& catalog) {
        catalog._replaceViewsForDatabase(dbName, std::move(viewsForDb));
    });
}
boost::optional<std::string> CollectionCatalog::lookupResourceName(const ResourceId& rid) const {
    invariant(rid.getType() == RESOURCE_DATABASE || rid.getType() == RESOURCE_COLLECTION);

    auto search = _resourceInformation.find(rid);
    if (search == _resourceInformation.end()) {
        return boost::none;
    }
    const std::set<std::string>& namespaces = search->second;

    // When there are multiple namespaces mapped to the same ResourceId, return boost::none as the
    // ResourceId does not identify a single namespace.
    if (namespaces.size() > 1) {
        return boost::none;
    }

    return *namespaces.begin();
}

void CollectionCatalog::removeResource(const ResourceId& rid, const std::string& entry) {
    invariant(rid.getType() == RESOURCE_DATABASE || rid.getType() == RESOURCE_COLLECTION);

    auto search = _resourceInformation.find(rid);
    if (search == _resourceInformation.end()) {
        return;
    }

    std::set<std::string> namespaces = search->second;
    namespaces.erase(entry);

    // Remove the map entry if this is the last namespace in the set for the ResourceId.
    if (namespaces.size() == 0) {
        _resourceInformation = _resourceInformation.erase(search, rid);
    } else {
        _resourceInformation = _resourceInformation.set(rid, std::move(namespaces));
    }
}

void CollectionCatalog::addResource(const ResourceId& rid, const std::string& entry) {
    invariant(rid.getType() == RESOURCE_DATABASE || rid.getType() == RESOURCE_COLLECTION);

    auto search = _resourceInformation.find(rid);
    if (search == _resourceInformation.end()) {
        std::set<std::string> newSet = {entry};
        _resourceInformation = _resourceInformation.set(rid, std::move(newSet));
        return;
    }

    if (const auto& namespaces = search->second; namespaces.count(entry) > 0) {
        return;
    }

    std::set<std::string> namespaces = search->second;
    namespaces.insert(entry);
    _resourceInformation = _resourceInformation.set(rid, std::move(namespaces));
}

void CollectionCatalog::invariantHasExclusiveAccessToCollection(OperationContext* opCtx,
                                                                const NamespaceString& nss) {
    auto& uncommittedCatalogUpdates = UncommittedCatalogUpdates::get(opCtx);
    invariant(opCtx->lockState()->isCollectionLockedForMode(nss, MODE_X) ||
                  (uncommittedCatalogUpdates.isCreatedCollection(opCtx, nss) &&
                   opCtx->lockState()->isCollectionLockedForMode(nss, MODE_IX)),
              nss.toString());
}

boost::optional<const ViewsForDatabase&> CollectionCatalog::_getViewsForDatabase(
    OperationContext* opCtx, StringData dbName) const {
    auto& uncommittedCatalogUpdates = UncommittedCatalogUpdates::get(opCtx);
    auto uncommittedViews = uncommittedCatalogUpdates.getViewsForDatabase(dbName);
    if (uncommittedViews) {
        return uncommittedViews;
    }

    const ViewsForDatabase* viewsForDb = _viewsForDatabase.find(dbName);
    if (!viewsForDb) {
        return boost::none;
    }
    return *viewsForDb;
}

void CollectionCatalog::_replaceViewsForDatabase(StringData dbName, ViewsForDatabase&& views) {
    _viewsForDatabase = _viewsForDatabase.set(dbName.toString(), std::move(views));
}

Status CollectionCatalog::_createOrUpdateView(
    OperationContext* opCtx,
    const NamespaceString& viewName,
    const NamespaceString& viewOn,
    const BSONArray& pipeline,
    const ViewsForDatabase::PipelineValidatorFn& pipelineValidator,
    std::unique_ptr<CollatorInterface> collator,
    ViewsForDatabase&& viewsForDb,
    ViewUpsertMode insertViewMode) const {
    // A view document direct write can occur via the oplog application path, which may only hold a
    // lock on the collection being updated (the database views collection).
    invariant(insertViewMode == ViewUpsertMode::kAlreadyDurableView ||
              opCtx->lockState()->isCollectionLockedForMode(viewName, MODE_IX));
    invariant(opCtx->lockState()->isCollectionLockedForMode(
        NamespaceString(viewName.db(), NamespaceString::kSystemDotViewsCollectionName), MODE_X));

    viewsForDb.requireValidCatalog();

    // Build the BSON definition for this view to be saved in the durable view catalog and/or to
    // insert in the viewMap. If the collation is empty, omit it from the definition altogether.
    BSONObjBuilder viewDefBuilder;
    viewDefBuilder.append("_id", viewName.ns());
    viewDefBuilder.append("viewOn", viewOn.coll());
    viewDefBuilder.append("pipeline", pipeline);
    if (collator) {
        viewDefBuilder.append("collation", collator->getSpec().toBSON());
    }

    BSONObj viewDef = viewDefBuilder.obj();
    BSONObj ownedPipeline = pipeline.getOwned();
    ViewDefinition view(
        viewName.db(), viewName.coll(), viewOn.coll(), ownedPipeline, std::move(collator));

    // If the view is already in the durable view catalog, we don't need to validate the graph. If
    // we need to update the durable view catalog, we need to check that the resulting dependency
    // graph is acyclic and within the maximum depth.
    const bool viewGraphNeedsValidation = insertViewMode != ViewUpsertMode::kAlreadyDurableView;
    Status graphStatus =
        viewsForDb.upsertIntoGraph(opCtx, view, pipelineValidator, viewGraphNeedsValidation);
    if (!graphStatus.isOK()) {
        return graphStatus;
    }

    if (insertViewMode != ViewUpsertMode::kAlreadyDurableView) {
        viewsForDb.durable->upsert(opCtx, viewName, viewDef);
    }

    viewsForDb.valid = false;
    auto res = [&] {
        switch (insertViewMode) {
            case ViewUpsertMode::kCreateView:
            case ViewUpsertMode::kAlreadyDurableView:
                return viewsForDb.insert(opCtx, viewDef);
            case ViewUpsertMode::kUpdateView:
                viewsForDb.viewMap.clear();
                viewsForDb.viewGraphNeedsRefresh = true;
                viewsForDb.stats = {};

                // Reload the view catalog with the changes applied.
                return viewsForDb.reload(opCtx);
        }
        MONGO_UNREACHABLE;
    }();

    if (res.isOK()) {
        auto& uncommittedCatalogUpdates = UncommittedCatalogUpdates::get(opCtx);
        uncommittedCatalogUpdates.addView(opCtx, viewName);
        uncommittedCatalogUpdates.replaceViewsForDatabase(viewName.db(), std::move(viewsForDb));

        PublishCatalogUpdates::ensureRegisteredWithRecoveryUnit(opCtx, uncommittedCatalogUpdates);
    }

    return res;
}


bool CollectionCatalog::_isCatalogBatchWriter() const {
    return batchedCatalogWriteInstance.get() == this;
}

bool CollectionCatalog::_alreadyClonedForBatchedWriter(
    const std::shared_ptr<Collection>& collection) const {
    // We may skip cloning the Collection instance if and only if have already cloned it for write
    // use in this batch writer.
    return _isCatalogBatchWriter() && batchedCatalogClonedCollections.contains(collection.get());
}

CollectionCatalogStasher::CollectionCatalogStasher(OperationContext* opCtx)
    : _opCtx(opCtx), _stashed(false) {}

CollectionCatalogStasher::CollectionCatalogStasher(OperationContext* opCtx,
                                                   std::shared_ptr<const CollectionCatalog> catalog)
    : _opCtx(opCtx), _stashed(true) {
    invariant(catalog);
    CollectionCatalog::stash(_opCtx, std::move(catalog));
}

CollectionCatalogStasher::CollectionCatalogStasher(CollectionCatalogStasher&& other)
    : _opCtx(other._opCtx), _stashed(other._stashed) {
    other._stashed = false;
}

CollectionCatalogStasher::~CollectionCatalogStasher() {
    if (_opCtx->isLockFreeReadsOp()) {
        // Leave the catalog stashed on the opCtx because there is another Stasher instance still
        // using it.
        return;
    }

    reset();
}

void CollectionCatalogStasher::stash(std::shared_ptr<const CollectionCatalog> catalog) {
    CollectionCatalog::stash(_opCtx, std::move(catalog));
    _stashed = true;
}

void CollectionCatalogStasher::reset() {
    if (_stashed) {
        CollectionCatalog::stash(_opCtx, nullptr);
        _stashed = false;
    }
}

const Collection* LookupCollectionForYieldRestore::operator()(OperationContext* opCtx,
                                                              const UUID& uuid) const {
    auto collection = CollectionCatalog::get(opCtx)->lookupCollectionByUUIDForRead(opCtx, uuid);

    // Collection dropped during yielding.
    if (!collection) {
        return nullptr;
    }

    // Collection renamed during yielding.
    // This check ensures that we are locked on the same namespace and that it is safe to return
    // the C-style pointer to the Collection.
    if (collection->ns() != _nss) {
        return nullptr;
    }

    // After yielding and reacquiring locks, the preconditions that were used to select our
    // ReadSource initially need to be checked again. We select a ReadSource based on replication
    // state. After a query yields its locks, the replication state may have changed, invalidating
    // our current choice of ReadSource. Using the same preconditions, change our ReadSource if
    // necessary.
    SnapshotHelper::changeReadSourceIfNeeded(opCtx, collection->ns());

    return collection.get();
}

BatchedCollectionCatalogWriter::BatchedCollectionCatalogWriter(OperationContext* opCtx)
    : _opCtx(opCtx) {
    invariant(_opCtx->lockState()->isW());
    invariant(!batchedCatalogWriteInstance);
    invariant(batchedCatalogClonedCollections.empty());

    auto& storage = getCatalog(_opCtx->getServiceContext());
    // hold onto base so if we need to delete it we can do it outside of the lock
    _base = atomic_load(&storage.catalog);
    // copy the collection catalog, this could be expensive, store it for future writes during this
    // batcher
    batchedCatalogWriteInstance = std::make_shared<CollectionCatalog>(*_base);
    _batchedInstance = batchedCatalogWriteInstance.get();
}
BatchedCollectionCatalogWriter::~BatchedCollectionCatalogWriter() {
    invariant(_opCtx->lockState()->isW());
    invariant(_batchedInstance == batchedCatalogWriteInstance.get());

    // Publish out batched instance, validate that no other writers have been able to write during
    // the batcher.
    auto& storage = getCatalog(_opCtx->getServiceContext());
    invariant(
        atomic_compare_exchange_strong(&storage.catalog, &_base, batchedCatalogWriteInstance));

    // Clear out batched pointer so no more attempts of batching are made
    _batchedInstance = nullptr;
    batchedCatalogWriteInstance = nullptr;
    batchedCatalogClonedCollections.clear();
}

}  // namespace mongo
