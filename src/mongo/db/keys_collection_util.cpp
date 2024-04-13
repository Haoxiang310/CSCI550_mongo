/**
 *    Copyright (C) 2023-present MongoDB, Inc.
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


#include "mongo/db/keys_collection_util.h"

#include "mongo/db/catalog_raii.h"
#include "mongo/db/concurrency/exception_util.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/keys_collection_document_gen.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/repl/repl_client_info.h"

namespace mongo {
namespace keys_collection_util {

ExternalKeysCollectionDocument makeExternalClusterTimeKeyDoc(BSONObj keyDoc,
                                                             boost::optional<UUID> migrationId,
                                                             boost::optional<Date_t> expireAt) {
    auto originalKeyDoc = KeysCollectionDocument::parse(IDLParserErrorContext("keyDoc"), keyDoc);

    ExternalKeysCollectionDocument externalKeyDoc(OID::gen(), originalKeyDoc.getKeyId());
    externalKeyDoc.setMigrationId(migrationId);
    externalKeyDoc.setKeysCollectionDocumentBase(originalKeyDoc.getKeysCollectionDocumentBase());
    externalKeyDoc.setTTLExpiresAt(expireAt);

    return externalKeyDoc;
}

repl::OpTime storeExternalClusterTimeKeyDocs(OperationContext* opCtx,
                                             std::vector<ExternalKeysCollectionDocument> keyDocs) {
    auto nss = NamespaceString::kExternalKeysCollectionNamespace;

    for (auto& keyDoc : keyDocs) {
        AutoGetCollection collection(opCtx, nss, MODE_IX);

        writeConflictRetry(opCtx, "CloneExternalKeyDocs", nss.ns(), [&] {
            // Note that each external key's _id is generated by the migration, so this upsert can
            // only insert.
            const auto filter =
                BSON(ExternalKeysCollectionDocument::kIdFieldName << keyDoc.getId());
            const auto updateMod = keyDoc.toBSON();

            Helpers::upsert(opCtx,
                            nss.toString(),
                            filter,
                            updateMod,
                            /*fromMigrate=*/false);
        });
    }

    return repl::ReplClientInfo::forClient(opCtx->getClient()).getLastOp();
}

}  // namespace keys_collection_util
}  // namespace mongo