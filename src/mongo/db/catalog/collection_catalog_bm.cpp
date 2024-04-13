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

#include "mongo/db/tenant_database_name.h"
#include <benchmark/benchmark.h>

#include "mongo/db/catalog/collection_catalog.h"
#include "mongo/db/catalog/collection_mock.h"
#include "mongo/db/concurrency/d_concurrency.h"
#include "mongo/db/concurrency/lock_state.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/service_context.h"
#include "mongo/util/uuid.h"

namespace mongo {

namespace {

class LockerImplClientObserver : public ServiceContext::ClientObserver {
public:
    LockerImplClientObserver() = default;
    ~LockerImplClientObserver() = default;

    void onCreateClient(Client* client) final {}

    void onDestroyClient(Client* client) final {}

    void onCreateOperationContext(OperationContext* opCtx) override {
        opCtx->setLockState(std::make_unique<LockerImpl>(opCtx->getServiceContext()));
    }

    void onDestroyOperationContext(OperationContext* opCtx) final {}
};

const ServiceContext::ConstructorActionRegisterer clientObserverRegisterer{
    "CollectionCatalogBenchmarkClientObserver",
    [](ServiceContext* service) {
        service->registerClientObserver(std::make_unique<LockerImplClientObserver>());
    },
    [](ServiceContext* serviceContext) {}};

ServiceContext* setupServiceContext() {
    auto serviceContext = ServiceContext::make();
    auto serviceContextPtr = serviceContext.get();
    setGlobalServiceContext(std::move(serviceContext));
    return serviceContextPtr;
}

void createCollections(OperationContext* opCtx, int numCollections) {
    Lock::GlobalLock globalLk(opCtx, MODE_X);
    BatchedCollectionCatalogWriter batched(opCtx);

    for (auto i = 0; i < numCollections; i++) {
        const NamespaceString nss("collection_catalog_bm", std::to_string(i));
        CollectionCatalog::write(opCtx, [&](CollectionCatalog& catalog) {
            catalog.registerCollection(opCtx, std::make_shared<CollectionMock>(nss));
        });
    }
}

}  // namespace

void BM_CollectionCatalogWrite(benchmark::State& state) {
    auto serviceContext = setupServiceContext();
    ThreadClient threadClient(serviceContext);
    ServiceContext::UniqueOperationContext opCtx = threadClient->makeOperationContext();

    createCollections(opCtx.get(), state.range(0));

    Lock::GlobalLock lk{opCtx.get(), MODE_IX};

    for (auto _ : state) {
        benchmark::ClobberMemory();
        CollectionCatalog::write(opCtx.get(), [&](CollectionCatalog& catalog) {});
    }
}

void BM_CollectionCatalogWriteBatchedWithGlobalExclusiveLock(benchmark::State& state) {
    auto serviceContext = setupServiceContext();
    ThreadClient threadClient(serviceContext);
    ServiceContext::UniqueOperationContext opCtx = threadClient->makeOperationContext();

    createCollections(opCtx.get(), state.range(0));

    Lock::GlobalLock globalLk(opCtx.get(), MODE_X);
    BatchedCollectionCatalogWriter batched(opCtx.get());

    for (auto _ : state) {
        benchmark::ClobberMemory();
        CollectionCatalog::write(opCtx.get(), [&](CollectionCatalog& catalog) {});
    }
}

void BM_CollectionCatalogCreateDropCollection(benchmark::State& state) {
    auto serviceContext = setupServiceContext();
    ThreadClient threadClient(serviceContext);
    ServiceContext::UniqueOperationContext opCtx = threadClient->makeOperationContext();
    Lock::GlobalLock globalLk(opCtx.get(), MODE_X);

    createCollections(opCtx.get(), state.range(0));

    for (auto _ : state) {
        benchmark::ClobberMemory();
        CollectionCatalog::write(opCtx.get(), [&](CollectionCatalog& catalog) {
            const NamespaceString nss("collection_catalog_bm", std::to_string(state.range(0)));
            const UUID uuid = UUID::gen();
            catalog.registerCollection(opCtx.get(), std::make_shared<CollectionMock>(uuid, nss));
            catalog.deregisterCollection(opCtx.get(), uuid);
        });
    }
}

void BM_CollectionCatalogCreateNCollectionsBatched(benchmark::State& state) {
    for (auto _ : state) {
        benchmark::ClobberMemory();

        auto serviceContext = setupServiceContext();
        ThreadClient threadClient(serviceContext);
        ServiceContext::UniqueOperationContext opCtx = threadClient->makeOperationContext();

        Lock::GlobalLock globalLk(opCtx.get(), MODE_X);
        BatchedCollectionCatalogWriter batched(opCtx.get());

        auto numCollections = state.range(0);
        for (auto i = 0; i < numCollections; i++) {
            const NamespaceString nss("collection_catalog_bm", std::to_string(i));
            CollectionCatalog::write(opCtx.get(), [&](CollectionCatalog& catalog) {
                catalog.registerCollection(opCtx.get(), std::make_shared<CollectionMock>(nss));
            });
        }
    }
}

void BM_CollectionCatalogCreateNCollections(benchmark::State& state) {
    for (auto _ : state) {
        benchmark::ClobberMemory();

        auto serviceContext = setupServiceContext();
        ThreadClient threadClient(serviceContext);
        ServiceContext::UniqueOperationContext opCtx = threadClient->makeOperationContext();
        Lock::GlobalLock globalLk(opCtx.get(), MODE_X);

        auto numCollections = state.range(0);
        for (auto i = 0; i < numCollections; i++) {
            const NamespaceString nss("collection_catalog_bm", std::to_string(i));
            CollectionCatalog::write(opCtx.get(), [&](CollectionCatalog& catalog) {
                catalog.registerCollection(opCtx.get(), std::make_shared<CollectionMock>(nss));
            });
        }
    }
}

void BM_CollectionCatalogLookupCollectionByNamespace(benchmark::State& state) {
    auto serviceContext = setupServiceContext();
    ThreadClient threadClient(serviceContext);
    ServiceContext::UniqueOperationContext opCtx = threadClient->makeOperationContext();

    createCollections(opCtx.get(), state.range(0));
    const NamespaceString nss("collection_catalog_bm", std::to_string(state.range(0) / 2));

    for (auto _ : state) {
        benchmark::ClobberMemory();
        auto coll =
            CollectionCatalog::get(opCtx.get())->lookupCollectionByNamespace(opCtx.get(), nss);
        invariant(coll);
    }
}

void BM_CollectionCatalogLookupCollectionByUUID(benchmark::State& state) {
    auto serviceContext = setupServiceContext();
    ThreadClient threadClient(serviceContext);
    ServiceContext::UniqueOperationContext opCtx = threadClient->makeOperationContext();

    createCollections(opCtx.get(), state.range(0));
    const NamespaceString nss("collection_catalog_bm", std::to_string(state.range(0) / 2));
    auto coll = CollectionCatalog::get(opCtx.get())->lookupCollectionByNamespace(opCtx.get(), nss);
    invariant(coll->ns() == nss);
    const UUID uuid = coll->uuid();

    for (auto _ : state) {
        benchmark::ClobberMemory();
        auto res = CollectionCatalog::get(opCtx.get())->lookupCollectionByUUID(opCtx.get(), uuid);
        invariant(res == coll);
    }
}

void BM_CollectionCatalogIterateCollections(benchmark::State& state) {
    auto serviceContext = setupServiceContext();
    ThreadClient threadClient(serviceContext);
    ServiceContext::UniqueOperationContext opCtx = threadClient->makeOperationContext();

    createCollections(opCtx.get(), state.range(0));

    for (auto _ : state) {
        benchmark::ClobberMemory();
        auto catalog = CollectionCatalog::get(opCtx.get());
        auto count = 0;
        for ([[maybe_unused]] auto&& coll :
             catalog->range(TenantDatabaseName(boost::none, "collection_catalog_bm"))) {
            benchmark::DoNotOptimize(count++);
        }
    }
}

BENCHMARK(BM_CollectionCatalogWrite)->Ranges({{{1}, {100'000}}});
BENCHMARK(BM_CollectionCatalogWriteBatchedWithGlobalExclusiveLock)->Ranges({{{1}, {100'000}}});
BENCHMARK(BM_CollectionCatalogCreateDropCollection)->Ranges({{{1}, {100'000}}});
BENCHMARK(BM_CollectionCatalogCreateNCollectionsBatched)->Ranges({{{1}, {100'000}}});
BENCHMARK(BM_CollectionCatalogCreateNCollections)->Ranges({{{1}, {32'768}}});
BENCHMARK(BM_CollectionCatalogLookupCollectionByNamespace)->Ranges({{{1}, {100'000}}});
BENCHMARK(BM_CollectionCatalogLookupCollectionByUUID)->Ranges({{{1}, {100'000}}});
BENCHMARK(BM_CollectionCatalogIterateCollections)->Ranges({{{1}, {100'000}}});

}  // namespace mongo
