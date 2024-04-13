/**
 * Tests that writes on the donor set succeeds when there is no migration.
 *
 * Tenant migrations are not expected to be run on servers with ephemeralForTest, and in particular
 * this test fails on ephemeralForTest because the donor has to wait for the write to set the
 * migration state to "committed" and "aborted" to be majority committed but it cannot do that on
 * ephemeralForTest.
 *
 * @tags: [
 *   incompatible_with_eft,
 *   incompatible_with_macos,
 *   incompatible_with_windows_tls,
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   serverless,
 * ]
 */
(function() {
'use strict';

load("jstests/libs/fail_point_util.js");
load("jstests/libs/parallelTester.js");
load("jstests/libs/uuid_util.js");
load("jstests/replsets/libs/tenant_migration_test.js");
load("jstests/replsets/libs/tenant_migration_util.js");
load("jstests/replsets/tenant_migration_concurrent_writes_on_donor_util.js");

const tenantMigrationTest = new TenantMigrationTest({
    name: jsTestName(),
    quickGarbageCollection: true,
});

const donorRst = tenantMigrationTest.getDonorRst();
const donorPrimary = donorRst.getPrimary();

const kCollName = "testColl";

const kTenantDefinedDbName = "0";

/**
 * Tests that the write succeeds when there is no migration.
 */
function testWritesNoMigration(testCase, testOpts) {
    runCommandForConcurrentWritesTest(testOpts);
    testCase.assertCommandSucceeded(testOpts.primaryDB, testOpts.dbName, testOpts.collName);
}

const testCases = TenantMigrationConcurrentWriteUtil.testCases;

// Run test cases with no migration.
for (const [commandName, testCase] of Object.entries(testCases)) {
    let baseDbName = commandName + "-noMigration0";

    if (testCase.skip) {
        print("Skipping " + commandName + ": " + testCase.skip);
        continue;
    }

    runTestForConcurrentWritesTest(donorPrimary,
                                   testCase,
                                   testWritesNoMigration,
                                   baseDbName + "Basic_" + kTenantDefinedDbName,
                                   kCollName);

    if (testCase.testInTransaction) {
        runTestForConcurrentWritesTest(donorPrimary,
                                       testCase,
                                       testWritesNoMigration,
                                       baseDbName + "Txn_" + kTenantDefinedDbName,
                                       kCollName,
                                       {testInTransaction: true});
    }

    if (testCase.testAsRetryableWrite) {
        runTestForConcurrentWritesTest(donorPrimary,
                                       testCase,
                                       testWritesNoMigration,
                                       baseDbName + "Retryable_" + kTenantDefinedDbName,
                                       kCollName,
                                       {testAsRetryableWrite: true});
    }
}

tenantMigrationTest.stop();
})();
