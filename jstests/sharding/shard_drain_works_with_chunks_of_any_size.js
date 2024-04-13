/*
 * Shard a collection with documents spread on 2 shards and then call `removeShard` checking that:
 * - Huge non-jumbo chunks are split during draining (moveRange moves off pieces of `chunkSize` MB)
 * - Jumbo chunks are moved off (without splitting, since it's not possible)
 *
 * Regression test for SERVER-76550.
 *
 * @tags: [ requires_fcv_60 ]
 */

(function() {
"use strict";
load("jstests/sharding/libs/find_chunks_util.js");

function removeShard(st, shardName, timeout) {
    if (timeout == undefined) {
        timeout = 5 * 60 * 1000;  // 5 minutes
    }

    assert.soon(function() {
        const res = st.s.adminCommand({removeShard: shardName});
        if (!res.ok && res.code === ErrorCodes.ShardNotFound) {
            // If the config server primary steps down right after removing the config.shards doc
            // for the shard but before responding with "state": "completed", the mongos would retry
            // the _configsvrRemoveShard command against the new config server primary, which would
            // not find the removed shard in its ShardRegistry if it has done a ShardRegistry reload
            // after the config.shards doc for the shard was removed. This would cause the command
            // to fail with ShardNotFound.
            return true;
        }
        assert.commandWorked(res);
        return res.state == 'completed';
    }, "failed to remove shard " + shardName + " within " + timeout + "ms", timeout);
}

const st = new ShardingTest({other: {enableBalancer: false, chunkSize: 1}});
const mongos = st.s0;
const configDB = st.getDB('config');

st.forEachConfigServer((conn) => {
    assert.commandWorked(conn.adminCommand({
        configureFailPoint: 'overrideBalanceRoundInterval',
        mode: 'alwaysOn',
        data: {intervalMs: 100}
    }));
});

const dbName = 'test';
const collName = 'collToDrain';
const ns = dbName + '.' + collName;
const db = st.getDB(dbName);
const coll = db.getCollection(collName);

// Shard collection with shard0 as db primary
assert.commandWorked(
    mongos.adminCommand({enablesharding: dbName, primaryShard: st.shard0.shardName}));
assert.commandWorked(mongos.adminCommand({shardCollection: ns, key: {x: 1}}));

// shard0 owns docs with shard key [MinKey, 0), shard1 owns docs with shard key [0, MaxKey)
assert.commandWorked(st.s.adminCommand(
    {moveRange: ns, min: {x: 0}, max: {x: MaxKey}, toShard: st.shard1.shardName}));

// Insert ~20MB of docs with different shard keys (10MB on shard0 and 10MB on shard1)
// and ~10MB of docs with the same shard key (jumbo chunk)
const big = 'X'.repeat(1024 * 1024);  // 1MB
const jumboKey = 100;
var bulk = coll.initializeUnorderedBulkOp();
for (var i = -10; i < 10; i++) {
    bulk.insert({x: i, big: big});
    bulk.insert({x: jumboKey, big: big});
}
assert.commandWorked(bulk.execute());

// Check that there are only 2 big chunks before starting draining
const chunksBeforeDrain = findChunksUtil.findChunksByNs(configDB, ns).toArray();
assert.eq(2, chunksBeforeDrain.length);

st.startBalancer();

// Remove shard 1 and wait for all chunks to be moved off from it
removeShard(st, st.shard1.shardName);

// Check that after draining there are 12 chunks on shard0:
// - [MinKey, 0)                   original chunk on shard 1
// - [0, 1), [1, 2), ... [8, 9)    1 MB chunks
// - [9, MaxKey)                   10MB jumbo chunk
const chunksAfterDrain =
    findChunksUtil.findChunksByNs(configDB, ns, {shard: st.shard0.shardName}).toArray();
assert.eq(12, chunksAfterDrain.length);

st.stop();
})();
