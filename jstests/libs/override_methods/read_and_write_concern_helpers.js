/**
 * Commands supporting read and write concern.
 */
var kCommandsSupportingReadConcern = new Set([
    "aggregate",
    "count",
    "distinct",
    "find",
]);

/**
 * Write commands supporting snapshot readConcern in a transaction.
 */
var kWriteCommandsSupportingSnapshotInTransaction = new Set([
    "delete",
    "findAndModify",
    "findandmodify",
    "insert",
    "update",
]);

/**
 * Commands supporting snapshot readConcern outside of transactions.
 */
var kCommandsSupportingSnapshot = new Set([
    "aggregate",
    "distinct",
    "find",
]);

var kCommandsSupportingWriteConcern = new Set([
    "_configsvrAddShard",
    "_configsvrAddShardToZone",
    "_configsvrCommitChunksMerge",
    "_configsvrCommitChunkMigration",
    "_configsvrCommitChunkSplit",
    "_configsvrCreateDatabase",
    "_configsvrMoveChunk",
    "_configsvrMoveRange",
    "_configsvrRemoveShard",
    "_configsvrRemoveShardFromZone",
    "_configsvrUpdateZoneKeyRange",
    "_mergeAuthzCollections",
    "_recvChunkStart",
    "abortTransaction",
    "appendOplogNote",
    "applyOps",
    "aggregate",
    "captrunc",
    "cleanupOrphaned",
    "clone",
    "cloneCollectionAsCapped",
    "collMod",
    "commitTransaction",
    "convertToCapped",
    "create",
    "createIndexes",
    "createRole",
    "createUser",
    "delete",
    "deleteIndexes",
    "drop",
    "dropAllRolesFromDatabase",
    "dropAllUsersFromDatabase",
    "dropDatabase",
    "dropIndexes",
    "dropRole",
    "dropUser",
    "emptycapped",
    "findAndModify",
    "findandmodify",
    "godinsert",
    "grantPrivilegesToRole",
    "grantRolesToRole",
    "grantRolesToUser",
    "insert",
    "mapReduce",
    "mapreduce",
    "moveChunk",
    "renameCollection",
    "revokePrivilegesFromRole",
    "revokeRolesFromRole",
    "revokeRolesFromUser",
    "setFeatureCompatibilityVersion",
    "testInternalTransactions",
    "update",
    "updateRole",
    "updateUser",
]);

var kCommandsSupportingWriteConcernInTransaction =
    new Set(["abortTransaction", "commitTransaction"]);
