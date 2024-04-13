// These commands were removed from mongos since the last LTS version, but will still appear in the
// listCommands output of a last LTS version mongos. A last-lts mongos will be unable to
// run a command on a latest version shard that no longer supports that command. To increase test
// coverage and allow us to run on same- and mixed-version suites, we allow these commands to have a
// test defined without always existing on the servers being used.
const commandsRemovedFromMongosSinceLastLTS = [
    "repairShardedCollectionChunksHistory",
    "configureCollectionAutoSplitter",  // TODO SERVER-62374: remove this once 5.3 becomes
                                        // last-continuos
];
// These commands were added in mongos since the last LTS version, so will not appear in the
// listCommands output of a last LTS version mongos. We will allow these commands to have a test
// defined without always existing on the mongos being used.
const commandsAddedToMongosSinceLastLTS = [
    "abortReshardCollection",
    "appendOplogNote",
    "cleanupReshardCollection",
    "commitReshardCollection",
    "compactStructuredEncryptionData",
    "configureCollectionBalancing",
    "createSearchIndexes",
    "dropSearchIndex",
    "fsyncUnlock",
    "getClusterParameter",
    "listSearchIndexes",
    "moveRange",
    "reshardCollection",
    "rotateCertificates",
    "setAllowMigrations",
    "setClusterParameter",
    "setProfilingFilterGlobally",  // TODO SERVER-73305
    "setUserWriteBlockMode",
    "testDeprecation",
    "testDeprecationInVersion2",
    "testInternalTransactions",
    "testRemoval",
    "testVersions1And2",
    "testVersion2",
    "updateSearchIndex",
];
