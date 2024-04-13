#!/bin/bash

# This script verifies that specific symbols, and specific symbols only are
# exported in mongo_crypt_v1.so

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o errexit
set -o verbose

if [ "$(uname)" != "Linux" ]; then
  echo "Skipping test, this is for linux only"
  exit 0
fi

EXTRACT_DIR="build/crypt-lib-${version}"
SOPATH="${EXTRACT_DIR}/lib/mongo_crypt_v1.so"
UNITTEST_PATH="${EXTRACT_DIR}/bin/mongo_crypt_shlib_test"
GDB_PATH="/opt/mongodbtoolchain/v3/bin/gdb"

# dump the contents of the extract dir to log
find $EXTRACT_DIR

if [ ! -f "$SOPATH" ]; then
  echo "Error: can not find library at: $SOPATH"
  exit 1
fi

#
# Check the set of exported symbols match the expected set of symbols
#
echo "Running Mongo Crypt Shared Library exported symbols test"

expect='A MONGO_CRYPT_1.0
T mongo_crypt_v1_analyze_query
T mongo_crypt_v1_bson_free
T mongo_crypt_v1_get_version
T mongo_crypt_v1_get_version_str
T mongo_crypt_v1_lib_create
T mongo_crypt_v1_lib_destroy
T mongo_crypt_v1_query_analyzer_create
T mongo_crypt_v1_query_analyzer_destroy
T mongo_crypt_v1_status_create
T mongo_crypt_v1_status_destroy
T mongo_crypt_v1_status_get_code
T mongo_crypt_v1_status_get_error
T mongo_crypt_v1_status_get_explanation'

actual="$(nm --extern-only --defined-only "$SOPATH" | awk '{ print $2, $3 }' | sort)"

if [ "$actual" != "$expect" ]; then
  echo "Error: symbols are not as expected in: $SOPATH"
  echo "Diff:"
  diff <(echo "$actual") <(echo "$expect")
  exit 1
fi

echo "Mongo Crypt Shared Library exported symbols test succeeded!"

#
# If the shared library version of the unit tests exists, run it,
# and the verify it can be debugged with gdb
#
if [ ! -f "$UNITTEST_PATH" ]; then
  echo "Skipping Mongo Crypt Shared Library unit test. Test not found at $UNITTEST_PATH"
  exit 0
fi

echo "Running Mongo Crypt Shared Library unit test"
$UNITTEST_PATH
echo "Mongo Crypt Shared Library unit test succeeded!"

if [ ! -f "$GDB_PATH" ]; then
  echo "Skipping Mongo Crypt Shared Library debuggability test. No gdb found at $GDB_PATH"
  exit 0
fi

echo "Running Mongo Crypt Shared Library debuggability test"
$GDB_PATH "$UNITTEST_PATH" --batch -ex "source ${EXTRACT_DIR}/crypt_debuggability_test.py"
echo "Mongo Crypt Shared Library shared library debuggability test succeeded!"
