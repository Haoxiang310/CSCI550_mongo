/**
 * Confirms that $lookup with a non-correlated foreign pipeline returns expected results.
 */
(function() {
"use strict";

load("jstests/aggregation/extras/utils.js");  // documentEq
load("jstests/libs/fixture_helpers.js");      // For isSharded.

const testDB = db.getSiblingDB("lookup_non_correlated");
const localName = "local";
const localColl = testDB.getCollection(localName);
localColl.drop();
const foreignName = "foreign";
const foreignColl = testDB.getCollection(foreignName);
foreignColl.drop();

// Do not run the rest of the tests if the foreign collection is implicitly sharded but the flag to
// allow $lookup/$graphLookup into a sharded collection is disabled.
const getShardedLookupParam = db.adminCommand({getParameter: 1, featureFlagShardedLookup: 1});
const isShardedLookupEnabled = getShardedLookupParam.hasOwnProperty("featureFlagShardedLookup") &&
    getShardedLookupParam.featureFlagShardedLookup.value;
if (FixtureHelpers.isSharded(foreignColl) && !isShardedLookupEnabled) {
    return;
}

assert.commandWorked(localColl.insert({_id: "A"}));
assert.commandWorked(localColl.insert({_id: "B"}));
assert.commandWorked(localColl.insert({_id: "C"}));

assert.commandWorked(foreignColl.insert({_id: 1}));
assert.commandWorked(foreignColl.insert({_id: 2}));
assert.commandWorked(foreignColl.insert({_id: 3}));

// Basic non-correlated lookup returns expected results.
let cursor = localColl.aggregate([
    {$match: {_id: {$in: ["B", "C"]}}},
    {$sort: {_id: 1}},
    {$lookup: {from: foreignName, as: "foreignDocs", pipeline: [{$match: {_id: {"$gte": 2}}}]}},
]);

assert(cursor.hasNext());
documentEq({_id: "B", foreignDocs: [{_id: 2}, {_id: 3}]}, cursor.next());
assert(cursor.hasNext());
documentEq({_id: "C", foreignDocs: [{_id: 2}, {_id: 3}]}, cursor.next());
assert(!cursor.hasNext());

// Non-correlated lookup followed by unwind on 'as' returns expected results.
cursor = localColl.aggregate([
    {$match: {_id: "A"}},
    {$lookup: {from: foreignName, as: "foreignDocs", pipeline: [{$match: {_id: {"$gte": 2}}}]}},
    {$unwind: "$foreignDocs"}
]);

assert(cursor.hasNext());
documentEq({_id: "A", foreignDocs: {_id: 2}}, cursor.next());
assert(cursor.hasNext());
documentEq({_id: "A", foreignDocs: {_id: 3}}, cursor.next());
assert(!cursor.hasNext());

// Non-correlated lookup followed by unwind and filter on 'as' returns expected results.
cursor = localColl.aggregate([
    {$match: {_id: "A"}},
    {$lookup: {from: foreignName, as: "foreignDocs", pipeline: [{$match: {_id: {"$gte": 2}}}]}},
    {$unwind: "$foreignDocs"},
    {$match: {"foreignDocs._id": 2}}
]);

assert(cursor.hasNext());
documentEq({_id: "A", foreignDocs: {_id: 2}}, cursor.next());
assert(!cursor.hasNext());
})();
