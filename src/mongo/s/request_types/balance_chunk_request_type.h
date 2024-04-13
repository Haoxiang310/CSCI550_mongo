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

#pragma once

#include <boost/optional.hpp>

#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/request_types/migration_secondary_throttle_options.h"
#include "mongo/s/shard_id.h"

namespace mongo {

class BSONObj;
template <typename T>
class StatusWith;

/**
 * Provides support for parsing and serialization of arguments to the config server moveChunk
 * command, which controls the cluster balancer. If any changes are made to this class, they need to
 * be backwards compatible with older versions of the server.
 */
class BalanceChunkRequest {
public:
    /**
     * Parses the provided BSON content and if it is correct construct a request object with the
     * request parameters. If the '_id' field is missing in obj, ignore it.
     */
    // TODO (SERVER-60792): Get rid of "requireUUID" once v6.0 branches out. Starting from v5.1, the
    // collection UUID will always be present in the chunk.
    static StatusWith<BalanceChunkRequest> parseFromConfigCommand(const BSONObj& obj,
                                                                  bool requireUUID = true);

    /**
     * Produces a BSON object for the variant of the command, which requests the balancer to pick a
     * better location for a chunk.
     */
    static BSONObj serializeToRebalanceCommandForConfig(const NamespaceString& nss,
                                                        const ChunkRange& range,
                                                        const UUID& collectionUUID,
                                                        const ShardId& owningShard,
                                                        const ChunkVersion& expectedChunkVersion);

    const NamespaceString& getNss() const {
        return _nss;
    }

    // TODO (SERVER-60792): Get rid of setCollectionUUID() once v6.0 branches out. Starting from
    // v5.1, the collection UUID will always be present in the chunk.
    void setCollectionUUID(UUID const& uuid) {
        _chunk.setCollectionUUID(uuid);
    }

    const ChunkType& getChunk() const {
        return _chunk;
    }

    bool hasToShardId() const {
        return _toShardId.is_initialized();
    }

    const ShardId& getToShardId() const {
        return *_toShardId;
    }

    const MigrationSecondaryThrottleOptions& getSecondaryThrottle() const {
        return _secondaryThrottle;
    }

    bool getWaitForDelete() const {
        return _waitForDelete;
    }

    bool getForceJumbo() const {
        return _forceJumbo;
    }

private:
    BalanceChunkRequest(ChunkType chunk, MigrationSecondaryThrottleOptions secondaryThrottle);

    NamespaceString _nss;

    // Complete description of the chunk to be manipulated
    ChunkType _chunk;

    // Id of the shard to which it should be moved (if specified)
    boost::optional<ShardId> _toShardId;

    // The parsed secondary throttle options
    MigrationSecondaryThrottleOptions _secondaryThrottle;

    // Whether to block and wait for the range deleter to cleanup the orphaned documents at the end
    // of move.
    bool _waitForDelete;

    bool _forceJumbo;
};

}  // namespace mongo
