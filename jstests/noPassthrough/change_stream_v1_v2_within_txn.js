/**
 * Confirms that if a v1 token is used to resume a change stream from a point in the middle of a
 * transaction, then a >16MB event later in that transaction will be successfully split if the
 * '$changeStreamSplitLargeEvent' stage is present.
 * @tags: [
 *   uses_change_streams,
 *   uses_transactions,
 * ]
 */
load("jstests/libs/collection_drop_recreate.js");  // For 'assertDropAndRecreateCollection()'.

// Create a new replica set with 1 node (minimal).
const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

// Set up DBs and collections used in this test.
const testDB = rst.getPrimary().getDB(jsTestName());
const testColl = assertDropAndRecreateCollection(
    testDB, "test_coll", {changeStreamPreAndPostImages: {enabled: true}});

// Open a change stream with batch size 1. As we are on 6.0, this will generate v1 tokens.
const csCursor = testColl.watch([], {fullDocumentBeforeChange: "whenAvailable", batchSize: 1});

// Start a new transaction.
const session = testDB.getMongo().startSession();
const sessionDB = session.getDatabase(testDB.getName());
const sessionColl = sessionDB[testColl.getName()];
session.startTransaction();

// Within the txn, insert a large document and then update it, then commit the transaction.
assert.commandWorked(sessionColl.insert({_id: 1, largeString: "x".repeat(15 * 1024 * 1024)}));
assert.commandWorked(
    sessionColl.update({_id: 1}, {$set: {largeString: "y".repeat(15 * 1024 * 1024)}}));
session.commitTransaction();

// Assert that we see the first event (the insert) in the v1 stream.
assert.soon(() => csCursor.hasNext());
const v1InsertEvent = csCursor.next();
assert.eq(v1InsertEvent.operationType, "insert");
// TODO SERVER-82330: Validate that the 'v1InsertEvent' has a v1 resume token.

// Now open a $changeStreamSplitLargeEvent pipeline and try to resume from the v1 token.
const csSplitCursor = testColl.watch(
    [{$changeStreamSplitLargeEvent: {}}],
    {resumeAfter: v1InsertEvent._id, fullDocumentBeforeChange: "whenAvailable", batchSize: 0});

// Confirm that the update within the same transaction is correctly split in the resumed stream.
assert.soon(() => csSplitCursor.hasNext());
const postResumeEvent = csSplitCursor.next();
assert.eq(postResumeEvent.operationType, "update");
assert.eq(postResumeEvent.splitEvent.fragment, 1);

rst.stopSet();
