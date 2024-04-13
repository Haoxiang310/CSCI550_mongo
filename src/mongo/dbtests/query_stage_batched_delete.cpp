/**
 *    Copyright (C) 2022-present MongoDB, Inc.
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

#include "mongo/platform/basic.h"

#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/client.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/exec/batched_delete_stage.h"
#include "mongo/db/exec/collection_scan.h"
#include "mongo/db/exec/delete_stage.h"
#include "mongo/db/exec/queued_data_stage.h"
#include "mongo/db/op_observer_noop.h"
#include "mongo/db/query/canonical_query.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/checkpointer.h"
#include "mongo/dbtests/dbtests.h"
#include "mongo/util/tick_source_mock.h"

namespace mongo {
namespace QueryStageBatchedDelete {

static const NamespaceString nss("unittests.QueryStageBatchedDelete");

// For the following tests, fix the targetBatchDocs to 10 documents.
static const int targetBatchDocs = 10;
static const Milliseconds targetBatchTimeMS = Milliseconds(5);

/**
 * Simulates how long each document takes to delete.
 *
 * Deletes on a batch of documents are executed in a single call to BatchedDeleteStage::work(). The
 * ClockAdvancingOpObserver is necessary to advance time per document delete, rather than per batch
 * delete.
 */
class ClockAdvancingOpObserver : public OpObserverNoop {
public:
    void aboutToDelete(OperationContext* opCtx,
                       const NamespaceString& nss,
                       const UUID& uuid,
                       const BSONObj& doc) override {

        if (docDurationMap.find(doc) != docDurationMap.end()) {
            tickSource->advance(docDurationMap.find(doc)->second);
        }
    }

    void setDeleteRecordDurationMillis(BSONObj targetDoc, Milliseconds duration) {
        docDurationMap.insert(std::make_pair(targetDoc, duration));
    }

    SimpleBSONObjUnorderedMap<Milliseconds> docDurationMap;
    TickSourceMock<Milliseconds>* tickSource;
};

class QueryStageBatchedDeleteTest : public unittest::Test {
public:
    QueryStageBatchedDeleteTest() : _client(&_opCtx) {
        // Since this test overrides the tick source on the global service context, it may
        // conflict with the checkpoint thread, which needs to create an operation context.
        // Since this test suite is run in isolation, it should be safe to disable the
        // background job before installing a new tick source.
        auto service = _opCtx.getServiceContext();
        if (!_tickSource) {
            if (auto checkpointer = Checkpointer::get(service)) {
                // BackgrounJob::cancel() keeps the checkpoint thread from starting.
                // However, if it is already running, we use Checkpoint::shutdown()
                // to wait for it to stop.
                if (!checkpointer->cancel().isOK()) {
                    checkpointer->shutdown({ErrorCodes::ShutdownInProgress, ""});
                }
            }

            auto tickSource = std::make_unique<TickSourceMock<Milliseconds>>();
            _tickSource = tickSource.get();
            service->setTickSource(std::move(tickSource));
        }
        _tickSource->reset(1);
        std::unique_ptr<ClockAdvancingOpObserver> opObserverUniquePtr =
            std::make_unique<ClockAdvancingOpObserver>();
        opObserverUniquePtr->tickSource = _tickSource;
        _opObserver = opObserverUniquePtr.get();
        service->setOpObserver(std::move(opObserverUniquePtr));
    }

    virtual ~QueryStageBatchedDeleteTest() {
        _client.dropCollection(nss.ns());
    }

    TickSourceMock<Milliseconds>* tickSource() {
        return _tickSource;
    }

    // Populates the collection with nDocs of shape {_id: <int i>, a: <int i>}.
    void prePopulateCollection(int nDocs) {
        for (int i = 0; i < nDocs; i++) {
            insert(BSON("_id" << i << "a" << i));
        }
    }

    void insert(const BSONObj& obj) {
        _client.insert(nss.ns(), obj);
    }

    // Inserts documents later deleted in a single 'batch' due to targetBatchTimMS or
    // targetBatchDocs. Tells the opObserver how much to advance the clock when a document is about
    // to be deleted.
    void insertTimedBatch(std::vector<std::pair<BSONObj, Milliseconds>> timedBatch) {
        Milliseconds totalDurationOfBatch{0};
        for (auto [doc, duration] : timedBatch) {
            _client.insert(nss.ns(), doc);
            _opObserver->setDeleteRecordDurationMillis(doc, duration);
            totalDurationOfBatch += duration;
        }

        // Enfore test correctness:
        // If the totalDurationOfBatch is larger than the targetBatchTimeMS, the last document of
        // the 'timedBatch' made the batch exceed targetBatchTimeMS.
        if (totalDurationOfBatch > targetBatchTimeMS) {
            auto batchSize = timedBatch.size();
            ASSERT_LT(totalDurationOfBatch - timedBatch[batchSize - 1].second, targetBatchTimeMS);
        }
    }

    void remove(const BSONObj& obj) {
        _client.remove(nss.ns(), obj);
    }

    void update(BSONObj& query, BSONObj& updateSpec) {
        _client.update(nss.ns(), query, updateSpec);
    }

    void getRecordIds(const CollectionPtr& collection,
                      CollectionScanParams::Direction direction,
                      std::vector<RecordId>* out) {
        WorkingSet ws;

        CollectionScanParams params;
        params.direction = direction;
        params.tailable = false;

        std::unique_ptr<CollectionScan> scan(
            new CollectionScan(_expCtx.get(), collection, params, &ws, nullptr));
        while (!scan->isEOF()) {
            WorkingSetID id = WorkingSet::INVALID_ID;
            PlanStage::StageState state = scan->work(&id);
            if (PlanStage::ADVANCED == state) {
                WorkingSetMember* member = ws.get(id);
                verify(member->hasRecordId());
                out->push_back(member->recordId);
            }
        }
    }

    std::unique_ptr<CanonicalQuery> canonicalize(const BSONObj& query) {
        auto findCommand = std::make_unique<FindCommandRequest>(nss);
        findCommand->setFilter(query);
        auto statusWithCQ = CanonicalQuery::canonicalize(&_opCtx, std::move(findCommand));
        ASSERT_OK(statusWithCQ.getStatus());
        return std::move(statusWithCQ.getValue());
    }

    // Uses the default _expCtx tied to the test suite.
    std::unique_ptr<BatchedDeleteStage> makeBatchedDeleteStage(
        WorkingSet* ws, const CollectionPtr& coll, CanonicalQuery* deleteParamsFilter = nullptr) {
        return makeBatchedDeleteStage(ws, coll, _expCtx.get(), deleteParamsFilter);
    }

    std::unique_ptr<BatchedDeleteStage> makeBatchedDeleteStage(
        WorkingSet* ws,
        const CollectionPtr& coll,
        ExpressionContext* expCtx,
        CanonicalQuery* deleteParamsFilter = nullptr) {

        CollectionScanParams collScanParams;
        auto batchedDeleteParams = std::make_unique<BatchedDeleteStageBatchParams>();
        batchedDeleteParams->targetBatchDocs = targetBatchDocs;
        batchedDeleteParams->targetBatchTimeMS = targetBatchTimeMS;

        // DeleteStageParams must always be multi.
        auto deleteParams = std::make_unique<DeleteStageParams>();
        deleteParams->isMulti = true;
        deleteParams->canonicalQuery = deleteParamsFilter;

        return std::make_unique<BatchedDeleteStage>(
            expCtx,
            std::move(deleteParams),
            std::move(batchedDeleteParams),
            ws,
            coll,
            new CollectionScan(expCtx, coll, collScanParams, ws, nullptr));
    }

protected:
    const ServiceContext::UniqueOperationContext _opCtxPtr = cc().makeOperationContext();
    OperationContext& _opCtx = *_opCtxPtr;

    boost::intrusive_ptr<ExpressionContext> _expCtx =
        make_intrusive<ExpressionContext>(&_opCtx, nullptr, nss);
    ClockAdvancingOpObserver* _opObserver;
    static TickSourceMock<Milliseconds>* _tickSource;

private:
    DBDirectClient _client;
};

// static
TickSourceMock<Milliseconds>* QueryStageBatchedDeleteTest::_tickSource = nullptr;

// Confirms batched deletes wait until a batch meets the targetBatchDocs before deleting documents.
TEST_F(QueryStageBatchedDeleteTest, BatchedDeleteTargetBatchDocsBasic) {
    dbtests::WriteContextForTests ctx(&_opCtx, nss.ns());
    auto nDocs = 52;
    prePopulateCollection(nDocs);

    const CollectionPtr& coll = ctx.getCollection();
    ASSERT(coll);

    WorkingSet ws;
    auto deleteStage = makeBatchedDeleteStage(&ws, coll);
    const DeleteStats* stats = static_cast<const DeleteStats*>(deleteStage->getSpecificStats());

    int nIterations = 0;
    PlanStage::StageState state = PlanStage::NEED_TIME;
    WorkingSetID id = WorkingSet::INVALID_ID;
    while ((state = deleteStage->work(&id)) != PlanStage::IS_EOF) {
        ASSERT_EQUALS(state, PlanStage::NEED_TIME);

        // Only delete documents once the current batch reaches targetBatchDocs.
        nIterations++;
        int batch = nIterations / (int)targetBatchDocs;
        ASSERT_EQUALS(stats->docsDeleted, targetBatchDocs * batch);
    }

    // There should be 2 more docs deleted by the time the command returns EOF.
    ASSERT_EQUALS(state, PlanStage::IS_EOF);
    ASSERT_EQUALS(stats->docsDeleted, nDocs);
}

// A staged document is removed while the BatchedDeleteStage is in a savedState. Upon restoring its
// state, BatchedDeleteStage's snapshot is incremented and it can see the document has been removed
// and skips over it.
TEST_F(QueryStageBatchedDeleteTest, BatchedDeleteStagedDocIsDeleted) {
    dbtests::WriteContextForTests ctx(&_opCtx, nss.ns());
    auto nDocs = 11;
    prePopulateCollection(nDocs);

    const CollectionPtr& coll = ctx.getCollection();
    ASSERT(coll);

    // Get the RecordIds that would be returned by an in-order scan.
    std::vector<RecordId> recordIds;
    getRecordIds(coll, CollectionScanParams::FORWARD, &recordIds);

    WorkingSet ws;
    auto deleteStage = makeBatchedDeleteStage(&ws, coll);
    const DeleteStats* stats = static_cast<const DeleteStats*>(deleteStage->getSpecificStats());

    // Index to pause at before fetching the remaining documents into the delete batch.
    int pauseBatchingIdx = 6;

    WorkingSetID id = WorkingSet::INVALID_ID;
    PlanStage::StageState state = PlanStage ::NEED_TIME;
    for (int i = 0; i < pauseBatchingIdx; i++) {
        deleteStage->work(&id);
        ASSERT_EQUALS(state, PlanStage::NEED_TIME);
        ASSERT_EQUALS(stats->docsDeleted, 0);
    }

    {
        // Delete a document that has already been added to the delete batch.
        deleteStage->saveState();
        BSONObj targetDoc = coll->docFor(&_opCtx, recordIds[pauseBatchingIdx - 2]).value();
        ASSERT(!targetDoc.isEmpty());
        remove(targetDoc);
        // Increases the snapshotId.
        deleteStage->restoreState(&coll);
    }

    while ((state = deleteStage->work(&id)) != PlanStage::IS_EOF) {
        ASSERT_EQUALS(state, PlanStage::NEED_TIME);
    }

    ASSERT_EQUALS(state, PlanStage::IS_EOF);
    ASSERT_EQUALS(stats->docsDeleted, nDocs - 1);
}

// A document staged for batched deletion is removed while the BatchedDeleteStage is still fetching
// documents. The BatchedDeleteStage tries to delete documents with a stale snapshot, gets a
// WriteConflict, yields, and then deletes the batch using a more recent snapshot that accounts for
// the concurrent data changes.
TEST_F(QueryStageBatchedDeleteTest, BatchedDeleteStagedDocIsDeletedWriteConflict) {
    if (storageGlobalParams.engine == "ephemeralForTest") {
        // TODO SERVER-64778 Investigate how to enable for the ephemeralForTest storage engine.
        return;
    }

    auto serviceContext = getGlobalServiceContext();

    // Issue the batched delete through different client than the default _client test member.
    auto batchedDeleteClient = serviceContext->makeClient("batchedDeleteClient");
    auto batchedDeleteOpCtx = batchedDeleteClient->makeOperationContext();
    boost::intrusive_ptr<ExpressionContext> batchedDeleteExpCtx =
        make_intrusive<ExpressionContext>(batchedDeleteOpCtx.get(), nullptr, nss);

    // Acquire locks for the batched delete.
    Lock::DBLock dbLk1(batchedDeleteOpCtx.get(), nss.db(), LockMode::MODE_IX);
    Lock::CollectionLock collLk1(batchedDeleteOpCtx.get(), nss, LockMode::MODE_IX);

    auto nDocs = 11;
    prePopulateCollection(nDocs);
    const CollectionPtr& coll = CollectionCatalog::get(batchedDeleteOpCtx.get())
                                    ->lookupCollectionByNamespace(batchedDeleteOpCtx.get(), nss);

    ASSERT(coll);

    // Get the RecordIds that would be returned by an in-order scan.
    std::vector<RecordId> recordIds;
    getRecordIds(coll, CollectionScanParams::FORWARD, &recordIds);


    WorkingSet ws;
    auto deleteStage = makeBatchedDeleteStage(&ws, coll, batchedDeleteExpCtx.get());
    const DeleteStats* stats = static_cast<const DeleteStats*>(deleteStage->getSpecificStats());

    // Index to pause at before fetching the remaining documents into the delete batch.
    int pauseBatchingIdx = 6;

    WorkingSetID id = WorkingSet::INVALID_ID;
    PlanStage::StageState state = PlanStage ::NEED_TIME;
    for (int i = 0; i < pauseBatchingIdx; i++) {
        deleteStage->work(&id);
        ASSERT_EQUALS(state, PlanStage::NEED_TIME);
        ASSERT_EQUALS(stats->docsDeleted, 0);
    }

    // Find the document to delete with the same OpertionContext that holds the locks.
    BSONObj targetDoc =
        coll->docFor(batchedDeleteOpCtx.get(), recordIds[pauseBatchingIdx - 2]).value();
    ASSERT(!targetDoc.isEmpty());

    {
        // Use the default _opCtx and _client to simulate a separate client running the remove.
        // Aquires locks through DBClient.
        remove(targetDoc);
    }

    int nYields = 0;
    while ((state = deleteStage->work(&id)) != PlanStage::IS_EOF) {
        if (state == PlanStage::NEED_YIELD) {
            // The BatchedDeleteStage tried to delete a document with a stale snapshot. A
            // WriteConflict was thrown before any deletes were committed.
            ASSERT_EQUALS(stats->docsDeleted, 0);
            nYields++;
        } else {
            ASSERT_EQUALS(state, PlanStage::NEED_TIME);
        }
    }

    // Confirm there was a yield.
    ASSERT_EQUALS(nYields, 1);

    ASSERT_EQUALS(state, PlanStage::IS_EOF);
    ASSERT_EQUALS(stats->docsDeleted, nDocs - 1);
}

// One of the staged documents is updated and then the BatchedDeleteStage increments its snapshot
// before discovering the mismatch.
TEST_F(QueryStageBatchedDeleteTest, BatchedDeleteStagedDocIsUpdatedToNotMatch) {
    dbtests::WriteContextForTests ctx(&_opCtx, nss.ns());
    auto nDocs = 11;
    prePopulateCollection(nDocs);

    const CollectionPtr& coll = ctx.getCollection();
    ASSERT(coll);

    // Only delete documents whose 'a' field is greater than or equal to 0.
    const BSONObj query = BSON("a" << BSON("$gte" << 0));
    const std::unique_ptr<CanonicalQuery> cq(canonicalize(query));

    WorkingSet ws;
    auto deleteStage = makeBatchedDeleteStage(&ws, coll, cq.get());
    const DeleteStats* stats = static_cast<const DeleteStats*>(deleteStage->getSpecificStats());

    // Index to pause at before fetching the remaining documents into the delete batch.
    int pauseBatchingIdx = 6;

    WorkingSetID id = WorkingSet::INVALID_ID;
    PlanStage::StageState state = PlanStage ::NEED_TIME;
    for (int i = 0; i < pauseBatchingIdx; i++) {
        deleteStage->work(&id);
        ASSERT_EQUALS(state, PlanStage::NEED_TIME);
        ASSERT_EQUALS(stats->docsDeleted, 0);
    }

    {
        // Update a staged document so it no longer matches the delete query.
        deleteStage->saveState();
        BSONObj queryObj = BSON("_id" << 2);
        BSONObj updateObj = BSON("a" << -1);
        update(queryObj, updateObj);
        // Increases the snapshotId.
        deleteStage->restoreState(&coll);
    }

    while ((state = deleteStage->work(&id)) != PlanStage::IS_EOF) {
        ASSERT_EQUALS(state, PlanStage::NEED_TIME);
    }

    ASSERT_EQUALS(state, PlanStage::IS_EOF);
    ASSERT_EQUALS(stats->docsDeleted, nDocs - 1);
}

// Simulates one client performing a batched delete while another updates a document staged for
// deletion. The BatchedDeleteStage tries to delete documents with a stale snapshot, gets a
// WriteConflict, yields, and then deletes the batch using a more recent snapshot that accounts for
// the concurrent data changes.
TEST_F(QueryStageBatchedDeleteTest, BatchedDeleteStagedDocIsUpdatedToNotMatchClientsWriteConflict) {
    auto serviceContext = getGlobalServiceContext();

    // Issue the batched delete through different client than the default _client test member.
    auto batchedDeleteClient = serviceContext->makeClient("batchedDeleteClient");
    auto batchedDeleteOpCtx = batchedDeleteClient->makeOperationContext();
    boost::intrusive_ptr<ExpressionContext> batchedDeleteExpCtx =
        make_intrusive<ExpressionContext>(batchedDeleteOpCtx.get(), nullptr, nss);

    // Acquire locks for the batched delete.
    Lock::DBLock dbLk1(batchedDeleteOpCtx.get(), nss.db(), LockMode::MODE_IX);
    Lock::CollectionLock collLk1(batchedDeleteOpCtx.get(), nss, LockMode::MODE_IX);

    auto nDocs = 11;
    prePopulateCollection(nDocs);
    const CollectionPtr& coll = CollectionCatalog::get(batchedDeleteOpCtx.get())
                                    ->lookupCollectionByNamespace(batchedDeleteOpCtx.get(), nss);

    ASSERT(coll);

    // Only delete documents whose 'a' field is greater than or equal to 0.
    const BSONObj query = BSON("a" << BSON("$gte" << 0));
    const std::unique_ptr<CanonicalQuery> cq(canonicalize(query));

    WorkingSet ws;
    auto deleteStage = makeBatchedDeleteStage(&ws, coll, batchedDeleteExpCtx.get(), cq.get());
    const DeleteStats* stats = static_cast<const DeleteStats*>(deleteStage->getSpecificStats());

    // Index to pause at before fetching the remaining documents into the delete batch.
    int pauseBatchingIdx = 6;

    WorkingSetID id = WorkingSet::INVALID_ID;
    PlanStage::StageState state = PlanStage ::NEED_TIME;
    for (int i = 0; i < pauseBatchingIdx; i++) {
        deleteStage->work(&id);
        ASSERT_EQUALS(state, PlanStage::NEED_TIME);
        ASSERT_EQUALS(stats->docsDeleted, 0);
    }

    {
        // Simulate another client running an update operation.
        BSONObj queryObj = BSON("_id" << 2);
        BSONObj updateObj = BSON("a" << -1);
        // Update uses the '_opCtx' tied to the test suite instead of 'batchedDeleteOpCtx'.
        update(queryObj, updateObj);
    }

    int nYields = 0;
    while ((state = deleteStage->work(&id)) != PlanStage::IS_EOF) {
        if (state == PlanStage::NEED_YIELD) {
            // The BatchedDeleteStage tried to delete a document with a stale snapshot. A
            // WriteConflict was thrown before any deletes were committed.
            ASSERT_EQUALS(stats->docsDeleted, 0);
            nYields++;
        } else {
            ASSERT_EQUALS(state, PlanStage::NEED_TIME);
        }
    }

    // Confirm there was a yield.
    ASSERT_EQUALS(nYields, 1);

    ASSERT_EQUALS(state, PlanStage::IS_EOF);
    ASSERT_EQUALS(stats->docsDeleted, nDocs - 1);
}

// Tests targetBatchTimeMS is enforced.
TEST_F(QueryStageBatchedDeleteTest, BatchedDeleteTargetBatchTimeMSBasic) {
    dbtests::WriteContextForTests ctx(&_opCtx, nss.ns());

    std::vector<std::pair<BSONObj, Milliseconds>> timedBatch0{
        {BSON("_id" << 1 << "a" << 1), Milliseconds(2)},
        {BSON("_id" << 2 << "a" << 2), Milliseconds(2)},
        {BSON("_id" << 3 << "a" << 3), Milliseconds(2)},
    };
    std::vector<std::pair<BSONObj, Milliseconds>> timedBatch1{
        {BSON("_id" << 4 << "a" << 4), Milliseconds(2)},
        {BSON("_id" << 5 << "a" << 5), Milliseconds(2)},
    };

    insertTimedBatch(timedBatch0);
    insertTimedBatch(timedBatch1);

    int batchSize0 = timedBatch0.size();
    int batchSize1 = timedBatch1.size();
    int nDocs = batchSize0 + batchSize1;

    const CollectionPtr& coll = ctx.getCollection();
    ASSERT(coll);

    WorkingSet ws;
    auto deleteStage = makeBatchedDeleteStage(&ws, coll);
    const DeleteStats* stats = static_cast<const DeleteStats*>(deleteStage->getSpecificStats());

    PlanStage::StageState state = PlanStage::NEED_TIME;
    WorkingSetID id = WorkingSet::INVALID_ID;

    // Stages all documents in the buffer before executing deletes since nDocs <
    // targetBatchDocs.
    {
        ASSERT_LTE(nDocs, targetBatchDocs);
        for (auto i = 0; i < nDocs; i++) {
            state = deleteStage->work(&id);
            ASSERT_EQ(stats->docsDeleted, 0);
            ASSERT_EQ(state, PlanStage::NEED_TIME);
        }
    }

    // Batch 0 deletions.
    {
        Timer timer(tickSource());
        state = deleteStage->work(&id);
        ASSERT_EQ(stats->docsDeleted, batchSize0);
        ASSERT_EQ(state, PlanStage::NEED_TIME);
        ASSERT_GTE(Milliseconds(timer.millis()), targetBatchTimeMS);
    }

    // Batch 1 deletions.
    {
        // Drain the rest of the buffer before fetching from a new WorkingSetMember.
        Timer timer(tickSource());
        state = deleteStage->work(&id);
        ASSERT_EQ(stats->docsDeleted, nDocs);
        ASSERT_EQ(state, PlanStage::NEED_TIME);
        ASSERT_LTE(Milliseconds(timer.millis()), targetBatchTimeMS);
    }

    // Completes multi delete execution.
    {
        state = deleteStage->work(&id);
        ASSERT_EQ(stats->docsDeleted, nDocs);
        ASSERT_EQ(state, PlanStage::IS_EOF);
    }
}

// Tests when the total time it takes to delete targetBatchDocs exceeds targetBatchTimeMS.
TEST_F(QueryStageBatchedDeleteTest, BatchedDeleteTargetBatchTimeMSWithTargetBatchDocs) {
    dbtests::WriteContextForTests ctx(&_opCtx, nss.ns());

    std::vector<std::pair<BSONObj, Milliseconds>> timedBatch0{
        {BSON("_id" << 1 << "a" << 1), Milliseconds(1)},
        {BSON("_id" << 2 << "a" << 2), Milliseconds(0)},
        {BSON("_id" << 3 << "a" << 3), Milliseconds(0)},
        {BSON("_id" << 4 << "a" << 4), Milliseconds(0)},
        {BSON("_id" << 5 << "a" << 5), Milliseconds(0)},
        {BSON("_id" << 6 << "a" << 6), Milliseconds(0)},
        {BSON("_id" << 7 << "a" << 7), Milliseconds(0)},
        {BSON("_id" << 8 << "a" << 8), Milliseconds(4)},
    };

    std::vector<std::pair<BSONObj, Milliseconds>> timedBatch1{
        {BSON("_id" << 9 << "a" << 9), Milliseconds(1)},
        {BSON("_id" << 10 << "a" << 10), Milliseconds(1)},
    };

    std::vector<std::pair<BSONObj, Milliseconds>> timedBatch2{
        {BSON("_id" << 11 << "a" << 11), Milliseconds(1)},
        {BSON("_id" << 12 << "a" << 12), Milliseconds(1)},
    };

    // Populate the collection before executing the BatchedDeleteStage.
    insertTimedBatch(timedBatch0);
    insertTimedBatch(timedBatch1);
    insertTimedBatch(timedBatch2);

    int batchSize0 = timedBatch0.size();
    int batchSize1 = timedBatch1.size();
    int batchSize2 = timedBatch2.size();
    int nDocs = batchSize0 + batchSize1 + batchSize2;

    const CollectionPtr& coll = ctx.getCollection();
    ASSERT(coll);

    WorkingSet ws;
    auto deleteStage = makeBatchedDeleteStage(&ws, coll);

    const DeleteStats* stats = static_cast<const DeleteStats*>(deleteStage->getSpecificStats());

    PlanStage::StageState state = PlanStage::NEED_TIME;
    WorkingSetID id = WorkingSet::INVALID_ID;

    // Stages up to targetBatchDocs - 1 documents in the buffer.
    {
        for (auto i = 0; i < targetBatchDocs - 1; i++) {
            state = deleteStage->work(&id);
            ASSERT_EQ(stats->docsDeleted, 0);
            ASSERT_EQ(state, PlanStage::NEED_TIME);
        }
    }

    // Batch0 deletions.
    {
        Timer timer(tickSource());
        state = deleteStage->work(&id);
        ASSERT_EQ(stats->docsDeleted, batchSize0);
        ASSERT_EQ(state, PlanStage::NEED_TIME);
        ASSERT_GTE(Milliseconds(timer.millis()), targetBatchTimeMS);
    }

    // Batch1 deletions.
    {
        Timer timer(tickSource());

        // Drain the rest of the buffer before fetching from a new WorkingSetMember.
        state = deleteStage->work(&id);
        ASSERT_EQ(stats->docsDeleted, batchSize0 + batchSize1);
        ASSERT_EQ(state, PlanStage::NEED_TIME);
        ASSERT_LTE(Milliseconds(timer.millis()), targetBatchTimeMS);
    }

    // Stages the remaining documents.
    {
        for (auto i = 0; i < batchSize2; i++) {
            state = deleteStage->work(&id);
            ASSERT_EQ(stats->docsDeleted, batchSize0 + batchSize1);
            ASSERT_EQ(state, PlanStage::NEED_TIME);
        }
    }

    // Batch 2 deletions.
    {
        Timer timer(tickSource());
        state = deleteStage->work(&id);
        ASSERT_EQ(stats->docsDeleted, nDocs);
        ASSERT_EQ(state, PlanStage::IS_EOF);
        ASSERT_LT(Milliseconds(timer.millis()), targetBatchTimeMS);
    }
}
}  // namespace QueryStageBatchedDelete
}  // namespace mongo
