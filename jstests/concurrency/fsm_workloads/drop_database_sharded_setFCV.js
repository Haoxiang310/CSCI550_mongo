'use strict';

/**
 * Repeatedly creates and drops a database in concurrency with FCV changes
 *
 * @tags: [
 *   requires_sharding,
 *   # Requires all nodes to be running the latest binary.
 *   multiversion_incompatible,
 *  ]
 */

load('jstests/concurrency/fsm_libs/extend_workload.js');
load('jstests/concurrency/fsm_workloads/drop_database_sharded.js');
load("jstests/libs/override_methods/mongos_manual_intervention_actions.js");

var $config = extendWorkload($config, function($config, $super) {
    $config.states.setFCV = function(db, collName) {
        const fcvValues = [lastLTSFCV, lastContinuousFCV, latestFCV];
        const targetFCV = fcvValues[Random.randInt(3)];
        jsTestLog('Executing FCV state, setting to:' + targetFCV);
        try {
            assertAlways.commandWorked(
                db.adminCommand({setFeatureCompatibilityVersion: targetFCV}));
        } catch (e) {
            if (e.code === 5147403) {
                // Invalid fcv transition (e.g lastContinuous -> lastLTS)
                jsTestLog('setFCV: Invalid transition');
                return;
            }
            throw e;
        }
        jsTestLog('setFCV state finished');
    };

    // TODO SERVER-63983: remove the following state override once 6.0 becomes lastLTS
    $config.states.shardCollection = function(db, collName) {
        let coll = getRandomCollection(db);
        jsTestLog('Executing shardCollection state: ' + coll.getFullName());
        assertAlways.commandWorkedOrFailedWithCode(
            db.adminCommand({shardCollection: coll.getFullName(), key: {_id: 1}}),
            [ErrorCodes.NamespaceNotFound, ErrorCodes.StaleDbVersion, ErrorCodes.IllegalOperation]);
    };

    $config.transitions = {
        init: {enableSharding: 0.3, dropDatabase: 0.3, shardCollection: 0.3, setFCV: 0.1},
        enableSharding: {enableSharding: 0.3, dropDatabase: 0.3, shardCollection: 0.3, setFCV: 0.1},
        dropDatabase: {enableSharding: 0.3, dropDatabase: 0.3, shardCollection: 0.3, setFCV: 0.1},
        shardCollection:
            {enableSharding: 0.3, dropDatabase: 0.3, shardCollection: 0.3, setFCV: 0.1},
        setFCV: {enableSharding: 0.3, dropDatabase: 0.3, shardCollection: 0.3, setFCV: 0.1},
    };

    $config.teardown = function(db, collName) {
        assert.commandWorked(db.adminCommand({setFeatureCompatibilityVersion: latestFCV}));
        $super.teardown(db, collName);
    };

    return $config;
});
