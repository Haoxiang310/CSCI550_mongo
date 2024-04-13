/**
 * @tags: [requires_sharding]
 *
 * Tests that the runCommandWithReadPreferenceSecondary override refuses to duplicate a
 * readPreference that already exists inside the top-level command object.
 */

(function() {
'use strict';

load('jstests/libs/override_methods/set_read_preference_secondary.js');

const st = new ShardingTest({shards: 1});

let err = assert.throws(() => {
    assert.commandWorked(st.s.getDB('db').runCommand({
        find: 'foo',
        $readPreference: {mode: 'nearest'},
    }));
});

assert(err.message.startsWith('Cowardly refusing to override read preference of command'));

// Necessary to turn this off so that ShardingTest post-test hooks don't fail by erroneously
// performing reads against secondary nodes.
TestData.doNotOverrideReadPreference = true;

st.stop();
})();
