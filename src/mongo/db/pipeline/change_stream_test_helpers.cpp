/**
 *    Copyright (C) 2022-present MongoDB, Inc.
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

#include "mongo/db/pipeline/change_stream_test_helpers.h"

#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/exec/document_value/value.h"
#include "mongo/db/exec/document_value/value_comparator.h"
#include "mongo/db/matcher/schema/expression_internal_schema_object_match.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/change_stream_rewrite_helpers.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/document_source_change_stream.h"
#include "mongo/db/repl/oplog_entry.h"
#include "mongo/util/uuid.h"

namespace mongo::change_stream_test_helper {

const UUID& testUuid() {
    static const UUID* uuid_gen = new UUID(UUID::gen());
    return *uuid_gen;
}

LogicalSessionFromClient testLsid() {
    // Required to avoid static initialization fiasco.
    static const UUID* uuid = new UUID(UUID::gen());
    LogicalSessionFromClient lsid{};
    lsid.setId(*uuid);
    return lsid;
}

Document makeResumeToken(Timestamp ts,
                         ImplicitValue uuid,
                         ImplicitValue docKey,
                         ResumeTokenData::FromInvalidate fromInvalidate,
                         size_t txnOpIndex) {
    ResumeTokenData tokenData;
    tokenData.clusterTime = ts;
    tokenData.eventIdentifier = docKey;
    tokenData.fromInvalidate = fromInvalidate;
    tokenData.txnOpIndex = txnOpIndex;
    if (!uuid.missing())
        tokenData.uuid = uuid.getUuid();
    return ResumeToken(tokenData).toDocument();
}

/**
 * Creates an OplogEntry with given parameters and preset defaults for this test suite.
 */
repl::OplogEntry makeOplogEntry(repl::OpTypeEnum opType,
                                NamespaceString nss,
                                BSONObj object,
                                boost::optional<UUID> uuid,
                                boost::optional<bool> fromMigrate,
                                boost::optional<BSONObj> object2,
                                boost::optional<repl::OpTime> opTime,
                                OperationSessionInfo sessionInfo,
                                boost::optional<repl::OpTime> prevOpTime,
                                boost::optional<repl::OpTime> preImageOpTime) {
    long long hash = 1LL;
    return {repl::DurableOplogEntry(opTime ? *opTime : kDefaultOpTime,  // optime
                                    hash,                               // hash
                                    opType,                             // opType
                                    boost::none,                        // tenant id
                                    nss,                                // namespace
                                    uuid,                               // uuid
                                    fromMigrate,                        // fromMigrate
                                    boost::none,                      // checkExistenceForDiffInsert
                                    repl::OplogEntry::kOplogVersion,  // version
                                    object,                           // o
                                    object2,                          // o2
                                    sessionInfo,                      // sessionInfo
                                    boost::none,                      // upsert
                                    Date_t(),                         // wall clock time
                                    {},                               // statement ids
                                    prevOpTime,  // optime of previous write within same transaction
                                    preImageOpTime,  // pre-image optime
                                    boost::none,     // post-image optime
                                    boost::none,     // ShardId of resharding recipient
                                    boost::none,     // _id
                                    boost::none)};   // needsRetryImage
}
}  // namespace mongo::change_stream_test_helper
