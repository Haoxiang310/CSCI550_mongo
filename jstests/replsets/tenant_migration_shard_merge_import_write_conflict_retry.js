/**
 * Test that the shard merge is resilient to WriteConflict exception thrown while importing
 * collections. We will get WriteConflict exception if we try to import the files with timestamp
 * older than the stable timestamp.
 *
 * @tags: [
 *   does_not_support_encrypted_storage_engine,
 *   featureFlagShardMerge,
 *   incompatible_with_eft,
 *   incompatible_with_macos,
 *   incompatible_with_windows_tls,
 *   requires_fcv_52,
 *   requires_journaling,
 *   requires_replication,
 *   requires_persistence,
 *   requires_wiredtiger,
 *   serverless,
 * ]
 */
(function() {
"use strict";

load("jstests/libs/uuid_util.js");
load("jstests/replsets/libs/tenant_migration_test.js");
load("jstests/libs/fail_point_util.js");

const migrationId = UUID();
const tenantMigrationTest = new TenantMigrationTest({name: jsTestName()});
const donorPrimary = tenantMigrationTest.getDonorPrimary();
const recipientPrimary = tenantMigrationTest.getRecipientPrimary();

if (!TenantMigrationUtil.isShardMergeEnabled(recipientPrimary.getDB("admin"))) {
    tenantMigrationTest.stop();
    jsTestLog("Skipping Shard Merge-specific test");
    return;
}

if (TenantMigrationUtil.isShardMergeEnabled(recipientPrimary.getDB("admin"))) {
    // TODO SERVER-63789: Re-enable this test for Shard Merge
    tenantMigrationTest.stop();
    jsTestLog("Temporarily skipping Shard Merge test, dependent on SERVER-63789.");
    return;
}

const kDataDir =
    `${recipientPrimary.dbpath}/migrationTmpFiles.${extractUUIDFromObject(migrationId)}`;
assert.eq(runNonMongoProgram("mkdir", "-p", kDataDir), 0);

(function() {
jsTestLog("Generate test data");

const db = donorPrimary.getDB("myDatabase");
const collection = db["myCollection"];
const capped = db["myCappedCollection"];
assert.commandWorked(db.createCollection("myCappedCollection", {capped: true, size: 100}));
for (let c of [collection, capped]) {
    c.insertMany([{_id: 0}, {_id: 1}, {_id: 2}], {writeConcern: {w: "majority"}});
}

assert.commandWorked(db.runCommand({
    createIndexes: "myCollection",
    indexes: [{key: {a: 1}, name: "a_1"}],
    writeConcern: {w: "majority"}
}));
})();

// Enable Failpoints to simulate WriteConflict exception while importing donor files.
configureFailPoint(
    recipientPrimary, "WTWriteConflictExceptionForImportCollection", {} /* data */, {times: 1});
configureFailPoint(
    recipientPrimary, "WTWriteConflictExceptionForImportIndex", {} /* data */, {times: 1});
configureFailPoint(recipientPrimary, "skipDeleteTempDBPath");

jsTestLog("Run migration");
// The old multitenant migrations won't copy myDatabase since it doesn't start with testTenantId,
// but shard merge copies everything so we still expect myDatabase on the recipient, below.
const kTenantId = "testTenantId";
const migrationOpts = {
    migrationIdString: extractUUIDFromObject(migrationId),
    tenantId: kTenantId,
};
TenantMigrationTest.assertCommitted(
    tenantMigrationTest.runMigration(migrationOpts, {enableDonorStartMigrationFsync: true}));

tenantMigrationTest.getRecipientRst().nodes.forEach(node => {
    for (let collectionName of ["myCollection", "myCappedCollection"]) {
        jsTestLog(`Checking ${collectionName}`);
        // Use "countDocuments" to check actual docs, "count" to check sizeStorer data.
        assert.eq(donorPrimary.getDB("myDatabase")[collectionName].countDocuments({}),
                  recipientPrimary.getDB("myDatabase")[collectionName].countDocuments({}),
                  "countDocuments");
        assert.eq(donorPrimary.getDB("myDatabase")[collectionName].count(),
                  recipientPrimary.getDB("myDatabase")[collectionName].count(),
                  "count");
    }
});

tenantMigrationTest.stop();
})();
