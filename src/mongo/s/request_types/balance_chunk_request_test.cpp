/**
 *    Copyright (C) 2018-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/write_concern_options.h"
#include "mongo/s/request_types/balance_chunk_request_type.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace {

using unittest::assertGet;

TEST(BalanceChunkRequest, RoundTrip) {
    UUID uuid{UUID::gen()};
    ChunkVersion version(30, 1, OID::gen(), Timestamp{2, 0});
    auto obj = BalanceChunkRequest::serializeToRebalanceCommandForConfig(
        NamespaceString("DB.Test"),
        ChunkRange(BSON("A" << 100), BSON("A" << 200)),
        uuid,
        ShardId("TestShard"),
        version);

    auto request =
        assertGet(BalanceChunkRequest::parseFromConfigCommand(obj, false /* requireUUID */));
    ASSERT_EQ(NamespaceString("DB.Test"), request.getNss());
    ASSERT_BSONOBJ_EQ(ChunkRange(BSON("A" << 100), BSON("A" << 200)).toBSON(),
                      request.getChunk().getRange().toBSON());
    ASSERT_EQ(uuid, request.getChunk().getCollectionUUID());
    ASSERT_EQ(version, request.getChunk().getVersion());
}

TEST(BalanceChunkRequest, ParseFromConfigCommandNoSecondaryThrottle) {
    const ChunkVersion version(1, 0, OID::gen(), Timestamp(1, 1));
    auto request = assertGet(BalanceChunkRequest::parseFromConfigCommand(
        BSON("_configsvrMoveChunk"
             << 1 << "ns"
             << "TestDB.TestColl"
             << "min" << BSON("a" << -100LL) << "max" << BSON("a" << 100LL) << "shard"
             << "TestShard0000"
             << "lastmod" << Date_t::fromMillisSinceEpoch(version.toLong()) << "lastmodEpoch"
             << version.epoch() << "lastmodTimestamp" << version.getTimestamp()),
        false /* requireUUID */));
    const auto& chunk = request.getChunk();
    ASSERT_EQ("TestDB.TestColl", request.getNss().ns());
    ASSERT_BSONOBJ_EQ(BSON("a" << -100LL), chunk.getMin());
    ASSERT_BSONOBJ_EQ(BSON("a" << 100LL), chunk.getMax());
    ASSERT_EQ(ShardId("TestShard0000"), chunk.getShard());
    ASSERT_EQ(version, chunk.getVersion());

    const auto& secondaryThrottle = request.getSecondaryThrottle();
    ASSERT_EQ(MigrationSecondaryThrottleOptions::kDefault,
              secondaryThrottle.getSecondaryThrottle());
}

// TODO (SERVER-60792): Get rid of the collection namespace from BSON once v6.0 branches out, as it
// will become a no longer mandatory argument. Ideally both variants should be tested.
TEST(BalanceChunkRequest, ParseFromConfigCommandWithUUID) {
    const auto uuid = UUID::gen();
    const ChunkVersion version(1, 0, OID::gen(), Timestamp(1, 1));
    auto request = assertGet(BalanceChunkRequest::parseFromConfigCommand(
        BSON("_configsvrMoveChunk" << 1 << "ns"
                                   << "TestDB.TestColl"
                                   << "uuid" << uuid << "min" << BSON("a" << -100LL) << "max"
                                   << BSON("a" << 100LL) << "shard"
                                   << "TestShard0000"
                                   << "lastmod" << Date_t::fromMillisSinceEpoch(version.toLong())
                                   << "lastmodEpoch" << version.epoch() << "lastmodTimestamp"
                                   << version.getTimestamp()),
        true /* requireUUID */));
    const auto& chunk = request.getChunk();
    ASSERT_EQ(uuid, chunk.getCollectionUUID());
    ASSERT_BSONOBJ_EQ(BSON("a" << -100LL), chunk.getMin());
    ASSERT_BSONOBJ_EQ(BSON("a" << 100LL), chunk.getMax());
    ASSERT_EQ(ShardId("TestShard0000"), chunk.getShard());
    ASSERT_EQ(version, chunk.getVersion());

    const auto& secondaryThrottle = request.getSecondaryThrottle();
    ASSERT_EQ(MigrationSecondaryThrottleOptions::kDefault,
              secondaryThrottle.getSecondaryThrottle());
}

TEST(BalanceChunkRequest, ParseFromConfigCommandWithSecondaryThrottle) {
    const ChunkVersion version(1, 0, OID::gen(), Timestamp(1, 1));
    auto request = assertGet(BalanceChunkRequest::parseFromConfigCommand(
        BSON("_configsvrMoveChunk"
             << 1 << "ns"
             << "TestDB.TestColl"
             << "min" << BSON("a" << -100LL) << "max" << BSON("a" << 100LL) << "shard"
             << "TestShard0000"
             << "lastmod" << Date_t::fromMillisSinceEpoch(version.toLong()) << "lastmodEpoch"
             << version.epoch() << "lastmodTimestamp" << version.getTimestamp()
             << "secondaryThrottle"
             << BSON("_secondaryThrottle" << true << "writeConcern" << BSON("w" << 2))),
        false /* requireUUID */));
    const auto& chunk = request.getChunk();
    ASSERT_EQ("TestDB.TestColl", request.getNss().ns());
    ASSERT_BSONOBJ_EQ(BSON("a" << -100LL), chunk.getMin());
    ASSERT_BSONOBJ_EQ(BSON("a" << 100LL), chunk.getMax());
    ASSERT_EQ(ShardId("TestShard0000"), chunk.getShard());
    ASSERT_EQ(version, chunk.getVersion());

    const auto& secondaryThrottle = request.getSecondaryThrottle();
    ASSERT_EQ(MigrationSecondaryThrottleOptions::kOn, secondaryThrottle.getSecondaryThrottle());

    auto writeConcern = secondaryThrottle.getWriteConcern();
    ASSERT_EQ(2, stdx::get<int64_t>(writeConcern.w));
}

}  // namespace
}  // namespace mongo
