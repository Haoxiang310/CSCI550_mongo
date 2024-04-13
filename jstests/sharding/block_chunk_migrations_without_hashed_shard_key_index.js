/**
 * Tests that chunk migrations are blocked when there is no index on a hashed shard key.
 *
 * @tags: [
 *   requires_fcv_50,
 * ]
 */

(function() {
"use strict";

load("jstests/sharding/libs/find_chunks_util.js");

const st = new ShardingTest({});

const dbName = "testDb";
const collName = "testColl";
const nss = dbName + "." + collName;
const configDB = st.s.getDB('config');

const coll = st.getDB(dbName).getCollection(collName);
const kDataString = "a".repeat(1024 * 1024);
let docs = Array.from({length: 1000}, (_, i) => ({_id: i, field: kDataString}));

assert.commandWorked(
    st.s.adminCommand({enablesharding: dbName, primaryShard: st.shard0.shardName}));
assert.commandWorked(coll.createIndex({"_id": "hashed"}));
assert.commandWorked(st.s.adminCommand({shardCollection: nss, key: {_id: "hashed"}}));

// Move all chunks to a single shard so the balancer is triggered due to data imbalance.
let chunks = findChunksUtil.findChunksByNs(configDB, nss).toArray();
chunks.forEach(chunk => {
    st.s.adminCommand({moveChunk: nss, bounds: [chunk.min, chunk.max], to: st.shard0.shardName});
});

assert.eq(0, findChunksUtil.findChunksByNs(configDB, nss, {shard: st.shard1.shardName}).itcount());

assert.commandWorked(coll.insert(docs));
assert.commandWorked(coll.dropIndex({"_id": "hashed"}));

st.startBalancer();
st.awaitBalancerRound();

// During balancing, the balancer should catch the IndexNotFound and turn off the balancer for the
// collection by setting {noBalance : true}.
assert.soon(() => {
    return configDB.getCollection('collections').findOne({_id: nss}).noBalance === true;
});

// Confirm all chunks remain on shard0.
assert.eq(0, findChunksUtil.findChunksByNs(configDB, nss, {shard: st.shard1.shardName}).itcount());

// Commands that trigger chunk migrations should fail with IndexNotFound.
assert.commandFailedWithCode(
    st.s.adminCommand(
        {moveChunk: nss, bounds: [chunks[0].min, chunks[0].max], to: st.shard1.shardName}),
    ErrorCodes.IndexNotFound);

// moveRange is not present in 5.0 so skip this assertion if FCV is lesser than 6.0 in multi-version
// test suite.
if (st.rs0.getPrimary().getDB('admin').system.version.findOne(
        {_id: 'featureCompatibilityVersion'}) == getFCVConstants().latest &&
    st.rs1.getPrimary().getDB('admin').system.version.findOne(
        {_id: 'featureCompatibilityVersion'}) == getFCVConstants().latest) {
    assert.commandFailedWithCode(
        st.s.adminCommand(
            {moveRange: nss, toShard: st.shard1.shardName, min: chunks[0].min, max: chunks[0].max}),
        ErrorCodes.IndexNotFound);
}

// Recreate the index and verify that we can re-enable balancing.
assert.commandWorked(coll.createIndex({"_id": "hashed"}));
st.enableBalancing(nss);

assert.eq(false, configDB.getCollection('collections').findOne({_id: nss}).noBalance);
st.awaitBalancerRound();
assert.soon(() => {
    return findChunksUtil.findChunksByNs(configDB, nss, {shard: st.shard1.shardName}).itcount() > 0;
});

st.stop();
})();
