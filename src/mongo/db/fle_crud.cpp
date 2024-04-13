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

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kWrite

#include "mongo/db/fle_crud.h"

#include <memory>

#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/crypto/encryption_fields_gen.h"
#include "mongo/crypto/fle_crypto.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_time_tracker.h"
#include "mongo/db/ops/write_ops_gen.h"
#include "mongo/db/ops/write_ops_parsers.h"
#include "mongo/db/query/collation/collator_factory_interface.h"
#include "mongo/db/query/find_command_gen.h"
#include "mongo/db/query/fle/server_rewrite.h"
#include "mongo/db/repl/repl_client_info.h"
#include "mongo/db/service_context.h"
#include "mongo/db/transaction_api.h"
#include "mongo/idl/idl_parser.h"
#include "mongo/logv2/log.h"
#include "mongo/rpc/factory.h"
#include "mongo/s/grid.h"
#include "mongo/s/transaction_router_resource_yielder.h"
#include "mongo/s/write_ops/batch_write_exec.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/concurrency/thread_pool.h"

MONGO_FAIL_POINT_DEFINE(fleCrudHangInsert);
MONGO_FAIL_POINT_DEFINE(fleCrudHangPreInsert);

MONGO_FAIL_POINT_DEFINE(fleCrudHangUpdate);
MONGO_FAIL_POINT_DEFINE(fleCrudHangPreUpdate);

MONGO_FAIL_POINT_DEFINE(fleCrudHangDelete);
MONGO_FAIL_POINT_DEFINE(fleCrudHangPreDelete);

MONGO_FAIL_POINT_DEFINE(fleCrudHangFindAndModify);
MONGO_FAIL_POINT_DEFINE(fleCrudHangPreFindAndModify);

namespace mongo {
namespace {

std::vector<write_ops::WriteError> singleStatusToWriteErrors(const Status& status) {
    std::vector<write_ops::WriteError> errors;

    errors.push_back(write_ops::WriteError(0, status));

    return errors;
}

void appendSingleStatusToWriteErrors(const Status& status,
                                     write_ops::WriteCommandReplyBase* replyBase) {
    std::vector<write_ops::WriteError> errors;

    if (replyBase->getWriteErrors()) {
        errors = std::move(replyBase->getWriteErrors().value());
    }

    errors.push_back(write_ops::WriteError(0, status));

    replyBase->setWriteErrors(errors);
}

void replyToResponse(OperationContext* opCtx,
                     write_ops::WriteCommandReplyBase* replyBase,
                     BatchedCommandResponse* response) {
    response->setStatus(Status::OK());
    response->setN(replyBase->getN());
    if (replyBase->getWriteErrors()) {
        for (const auto& error : *replyBase->getWriteErrors()) {
            response->addToErrDetails(error);
        }
    }

    // Update the OpTime for the reply to current OpTime
    //
    // The OpTime in the reply reflects the OpTime of when the request was run, not when it was
    // committed. The Transaction API propagates the OpTime from the commit transaction onto the
    // current thread so grab it from TLS and change the OpTime on the reply.
    //
    response->setLastOp({OperationTimeTracker::get(opCtx)->getMaxOperationTime().asTimestamp(),
                         repl::OpTime::kUninitializedTerm});
}

void responseToReply(const BatchedCommandResponse& response,
                     write_ops::WriteCommandReplyBase& replyBase) {
    if (response.isLastOpSet()) {
        replyBase.setOpTime(response.getLastOp());
    }

    if (response.isElectionIdSet()) {
        replyBase.setElectionId(response.getElectionId());
    }

    replyBase.setN(response.getN());
    if (response.isErrDetailsSet()) {
        replyBase.setWriteErrors(response.getErrDetails());
    }
}

boost::optional<BSONObj> mergeLetAndCVariables(const boost::optional<BSONObj>& let,
                                               const boost::optional<BSONObj>& c) {
    if (!let.has_value() && !c.has_value()) {
        return boost::none;
    } else if (let.has_value() && c.has_value()) {
        BSONObj obj = let.value();
        // Prioritize the fields in c over the fields in let in case of duplicates
        return obj.addFields(c.value());
    } else if (let.has_value()) {
        return let;
    }
    return c;
}
}  // namespace

std::shared_ptr<txn_api::SyncTransactionWithRetries> getTransactionWithRetriesForMongoS(
    OperationContext* opCtx) {
    return std::make_shared<txn_api::SyncTransactionWithRetries>(
        opCtx,
        Grid::get(opCtx)->getExecutorPool()->getFixedExecutor(),
        TransactionRouterResourceYielder::makeForLocalHandoff());
}

namespace {
/**
 * Make an expression context from a batch command request and a specific operation. Templated out
 * to work with update and delete.
 */
template <typename T, typename O>
boost::intrusive_ptr<ExpressionContext> makeExpCtx(OperationContext* opCtx,
                                                   const T& request,
                                                   const O& op) {
    std::unique_ptr<CollatorInterface> collator;
    if (op.getCollation()) {
        auto statusWithCollator = CollatorFactoryInterface::get(opCtx->getServiceContext())
                                      ->makeFromBSON(op.getCollation().get());

        uassertStatusOK(statusWithCollator.getStatus());
        collator = std::move(statusWithCollator.getValue());
    }
    auto expCtx = make_intrusive<ExpressionContext>(opCtx,
                                                    std::move(collator),
                                                    request.getNamespace(),
                                                    request.getLegacyRuntimeConstants(),
                                                    request.getLet());
    expCtx->stopExpressionCounters();
    return expCtx;
}

}  // namespace

std::pair<FLEBatchResult, write_ops::InsertCommandReply> processInsert(
    OperationContext* opCtx,
    const write_ops::InsertCommandRequest& insertRequest,
    GetTxnCallback getTxns) {

    auto edcNss = insertRequest.getNamespace();
    auto ei = insertRequest.getEncryptionInformation().get();

    bool bypassDocumentValidation =
        insertRequest.getWriteCommandRequestBase().getBypassDocumentValidation();

    auto efc = EncryptionInformationHelpers::getAndValidateSchema(edcNss, ei);

    auto documents = insertRequest.getDocuments();
    // TODO - how to check if a document will be too large???

    uassert(6371202,
            "Only single insert batches are supported in Queryable Encryption",
            documents.size() == 1);

    auto document = documents[0];
    EDCServerCollection::validateEncryptedFieldInfo(document, efc, bypassDocumentValidation);
    auto serverPayload = std::make_shared<std::vector<EDCServerPayloadInfo>>(
        EDCServerCollection::getEncryptedFieldInfo(document));

    if (serverPayload->size() == 0) {
        // No actual FLE2 indexed fields
        return std::pair<FLEBatchResult, write_ops::InsertCommandReply>{
            FLEBatchResult::kNotProcessed, write_ops::InsertCommandReply()};
    }

    auto reply = std::make_shared<write_ops::InsertCommandReply>();

    uint32_t stmtId = getStmtIdForWriteAt(insertRequest, 0);

    std::shared_ptr<txn_api::SyncTransactionWithRetries> trun = getTxns(opCtx);

    // The function that handles the transaction may outlive this function so we need to use
    // shared_ptrs since it runs on another thread
    auto ownedDocument = document.getOwned();
    auto insertBlock = std::make_tuple(edcNss, efc, serverPayload, stmtId);
    auto sharedInsertBlock = std::make_shared<decltype(insertBlock)>(insertBlock);

    auto swResult = trun->runNoThrow(
        opCtx,
        [sharedInsertBlock, reply, ownedDocument, bypassDocumentValidation](
            const txn_api::TransactionClient& txnClient, ExecutorPtr txnExec) {
            FLEQueryInterfaceImpl queryImpl(txnClient, getGlobalServiceContext());

            auto [edcNss2, efc2, serverPayload2, stmtId2] = *sharedInsertBlock.get();

            if (MONGO_unlikely(fleCrudHangPreInsert.shouldFail())) {
                LOGV2(6516701, "Hanging due to fleCrudHangPreInsert fail point");
                fleCrudHangPreInsert.pauseWhileSet();
            }

            *reply = uassertStatusOK(processInsert(&queryImpl,
                                                   edcNss2,
                                                   *serverPayload2.get(),
                                                   efc2,
                                                   stmtId2,
                                                   ownedDocument,
                                                   bypassDocumentValidation));

            if (MONGO_unlikely(fleCrudHangInsert.shouldFail())) {
                LOGV2(6371903, "Hanging due to fleCrudHangInsert fail point");
                fleCrudHangInsert.pauseWhileSet();
            }

            // If we have write errors but no unexpected internal errors, then we reach here
            // If we have write errors, we need to return a failed status to ensure the txn client
            // does not try to commit the transaction.
            if (reply->getWriteErrors().has_value() && !reply->getWriteErrors().value().empty()) {
                return SemiFuture<void>::makeReady(
                    Status(ErrorCodes::FLETransactionAbort,
                           "Queryable Encryption write errors on insert"));
            }

            return SemiFuture<void>::makeReady();
        });

    if (!swResult.isOK()) {
        // FLETransactionAbort is used for control flow so it means we have a valid
        // InsertCommandReply with write errors so we should return that.
        if (swResult.getStatus() == ErrorCodes::FLETransactionAbort) {
            return std::pair<FLEBatchResult, write_ops::InsertCommandReply>{
                FLEBatchResult::kProcessed, *reply};
        }

        appendSingleStatusToWriteErrors(swResult.getStatus(), &reply->getWriteCommandReplyBase());
    } else if (!swResult.getValue().getEffectiveStatus().isOK()) {
        appendSingleStatusToWriteErrors(swResult.getValue().getEffectiveStatus(),
                                        &reply->getWriteCommandReplyBase());
    }

    return std::pair<FLEBatchResult, write_ops::InsertCommandReply>{FLEBatchResult::kProcessed,
                                                                    *reply};
}

write_ops::DeleteCommandReply processDelete(OperationContext* opCtx,
                                            const write_ops::DeleteCommandRequest& deleteRequest,
                                            GetTxnCallback getTxns) {
    {
        auto deletes = deleteRequest.getDeletes();
        uassert(6371302, "Only single document deletes are permitted", deletes.size() == 1);

        auto deleteOpEntry = deletes[0];

        uassert(6371303,
                "FLE only supports single document deletes",
                deleteOpEntry.getMulti() == false);
    }

    std::shared_ptr<txn_api::SyncTransactionWithRetries> trun = getTxns(opCtx);

    auto reply = std::make_shared<write_ops::DeleteCommandReply>();

    auto ownedRequest = deleteRequest.serialize({});
    auto ownedDeleteRequest =
        write_ops::DeleteCommandRequest::parse(IDLParserErrorContext("delete"), ownedRequest);
    auto ownedDeleteOpEntry = ownedDeleteRequest.getDeletes()[0];

    auto expCtx = makeExpCtx(opCtx, ownedDeleteRequest, ownedDeleteOpEntry);
    // The function that handles the transaction may outlive this function so we need to use
    // shared_ptrs
    auto deleteBlock = std::make_tuple(ownedDeleteRequest, expCtx);
    auto sharedDeleteBlock = std::make_shared<decltype(deleteBlock)>(deleteBlock);

    auto swResult = trun->runNoThrow(
        opCtx,
        [sharedDeleteBlock, ownedRequest, reply](const txn_api::TransactionClient& txnClient,
                                                 ExecutorPtr txnExec) {
            FLEQueryInterfaceImpl queryImpl(txnClient, getGlobalServiceContext());

            auto [deleteRequest2, expCtx2] = *sharedDeleteBlock.get();

            if (MONGO_unlikely(fleCrudHangPreDelete.shouldFail())) {
                LOGV2(6516702, "Hanging due to fleCrudHangPreDelete fail point");
                fleCrudHangPreDelete.pauseWhileSet();
            }


            *reply = processDelete(&queryImpl, expCtx2, deleteRequest2);

            if (MONGO_unlikely(fleCrudHangDelete.shouldFail())) {
                LOGV2(6371902, "Hanging due to fleCrudHangDelete fail point");
                fleCrudHangDelete.pauseWhileSet();
            }

            // If we have write errors but no unexpected internal errors, then we reach here
            // If we have write errors, we need to return a failed status to ensure the txn client
            // does not try to commit the transaction.
            if (reply->getWriteErrors().has_value() && !reply->getWriteErrors().value().empty()) {
                return SemiFuture<void>::makeReady(
                    Status(ErrorCodes::FLETransactionAbort,
                           "Queryable Encryption write errors on delete"));
            }

            return SemiFuture<void>::makeReady();
        });

    if (!swResult.isOK()) {
        // FLETransactionAbort is used for control flow so it means we have a valid
        // InsertCommandReply with write errors so we should return that.
        if (swResult.getStatus() == ErrorCodes::FLETransactionAbort) {
            return *reply;
        }

        appendSingleStatusToWriteErrors(swResult.getStatus(), &reply->getWriteCommandReplyBase());
    } else if (!swResult.getValue().getEffectiveStatus().isOK()) {
        appendSingleStatusToWriteErrors(swResult.getValue().getEffectiveStatus(),
                                        &reply->getWriteCommandReplyBase());
    }

    return *reply;
}

write_ops::UpdateCommandReply processUpdate(OperationContext* opCtx,
                                            const write_ops::UpdateCommandRequest& updateRequest,
                                            GetTxnCallback getTxns) {

    {
        auto updates = updateRequest.getUpdates();
        uassert(6371502, "Only single document updates are permitted", updates.size() == 1);

        auto updateOpEntry = updates[0];

        uassert(6371503,
                "FLE only supports single document updates",
                updateOpEntry.getMulti() == false);

        // pipeline - is agg specific, delta is oplog, transform is internal (timeseries)
        uassert(6371517,
                "FLE only supports modifier and replacement style updates",
                updateOpEntry.getU().type() == write_ops::UpdateModification::Type::kModifier ||
                    updateOpEntry.getU().type() ==
                        write_ops::UpdateModification::Type::kReplacement);
    }

    std::shared_ptr<txn_api::SyncTransactionWithRetries> trun = getTxns(opCtx);

    // The function that handles the transaction may outlive this function so we need to use
    // shared_ptrs
    auto reply = std::make_shared<write_ops::UpdateCommandReply>();

    auto ownedRequest = updateRequest.serialize({});
    auto ownedUpdateRequest =
        write_ops::UpdateCommandRequest::parse(IDLParserErrorContext("update"), ownedRequest);
    auto ownedUpdateOpEntry = ownedUpdateRequest.getUpdates()[0];

    auto expCtx = makeExpCtx(opCtx, ownedUpdateRequest, ownedUpdateOpEntry);
    auto updateBlock = std::make_tuple(ownedUpdateRequest, expCtx);
    auto sharedupdateBlock = std::make_shared<decltype(updateBlock)>(updateBlock);

    auto swResult = trun->runNoThrow(
        opCtx,
        [sharedupdateBlock, reply, ownedRequest](const txn_api::TransactionClient& txnClient,
                                                 ExecutorPtr txnExec) {
            FLEQueryInterfaceImpl queryImpl(txnClient, getGlobalServiceContext());

            auto [updateRequest2, expCtx2] = *sharedupdateBlock.get();

            if (MONGO_unlikely(fleCrudHangPreUpdate.shouldFail())) {
                LOGV2(6516703, "Hanging due to fleCrudHangPreUpdate fail point");
                fleCrudHangPreUpdate.pauseWhileSet();
            }

            *reply = processUpdate(&queryImpl, expCtx2, updateRequest2);

            if (MONGO_unlikely(fleCrudHangUpdate.shouldFail())) {
                LOGV2(6371901, "Hanging due to fleCrudHangUpdate fail point");
                fleCrudHangUpdate.pauseWhileSet();
            }

            // If we have write errors but no unexpected internal errors, then we reach here
            // If we have write errors, we need to return a failed status to ensure the txn client
            // does not try to commit the transaction.
            if (reply->getWriteErrors().has_value() && !reply->getWriteErrors().value().empty()) {
                return SemiFuture<void>::makeReady(
                    Status(ErrorCodes::FLETransactionAbort,
                           "Queryable Encryption write errors on delete"));
            }

            return SemiFuture<void>::makeReady();
        });

    if (!swResult.isOK()) {
        // FLETransactionAbort is used for control flow so it means we have a valid
        // InsertCommandReply with write errors so we should return that.
        if (swResult.getStatus() == ErrorCodes::FLETransactionAbort) {
            return *reply;
        }

        appendSingleStatusToWriteErrors(swResult.getStatus(), &reply->getWriteCommandReplyBase());
    } else if (!swResult.getValue().getEffectiveStatus().isOK()) {
        appendSingleStatusToWriteErrors(swResult.getValue().getEffectiveStatus(),
                                        &reply->getWriteCommandReplyBase());
    }

    return *reply;
}

namespace {

void processFieldsForInsert(FLEQueryInterface* queryImpl,
                            const NamespaceString& edcNss,
                            std::vector<EDCServerPayloadInfo>& serverPayload,
                            const EncryptedFieldConfig& efc,
                            int32_t* pStmtId,
                            bool bypassDocumentValidation) {

    NamespaceString nssEsc(edcNss.db(), efc.getEscCollection().get());

    auto docCount = queryImpl->countDocuments(nssEsc);

    TxnCollectionReader reader(docCount, queryImpl, nssEsc);

    for (auto& payload : serverPayload) {

        auto escToken = payload.getESCToken();
        auto tagToken = FLETwiceDerivedTokenGenerator::generateESCTwiceDerivedTagToken(escToken);
        auto valueToken =
            FLETwiceDerivedTokenGenerator::generateESCTwiceDerivedValueToken(escToken);

        int position = 1;
        int count = 1;
        auto alpha = ESCCollection::emuBinary(reader, tagToken, valueToken);

        if (alpha.has_value() && alpha.value() == 0) {
            position = 1;
            count = 1;
        } else if (!alpha.has_value()) {
            auto block = ESCCollection::generateId(tagToken, boost::none);

            auto r_esc = reader.getById(block);
            uassert(6371203, "ESC document not found", !r_esc.isEmpty());

            auto escNullDoc =
                uassertStatusOK(ESCCollection::decryptNullDocument(valueToken, r_esc));

            position = escNullDoc.position + 2;
            count = escNullDoc.count + 1;
        } else {
            auto block = ESCCollection::generateId(tagToken, alpha);

            auto r_esc = reader.getById(block);
            uassert(6371204, "ESC document not found", !r_esc.isEmpty());

            auto escDoc = uassertStatusOK(ESCCollection::decryptDocument(valueToken, r_esc));

            position = alpha.value() + 1;
            count = escDoc.count + 1;

            if (escDoc.compactionPlaceholder) {
                uassertStatusOK(Status(ErrorCodes::FLECompactionPlaceholder,
                                       "Found ESC contention placeholder"));
            }
        }

        payload.count = count;

        auto escInsertReply = uassertStatusOK(queryImpl->insertDocument(
            nssEsc,
            ESCCollection::generateInsertDocument(tagToken, valueToken, position, count),
            pStmtId,
            true));
        checkWriteErrors(escInsertReply);


        NamespaceString nssEcoc(edcNss.db(), efc.getEcocCollection().get());

        // TODO - should we make this a batch of ECOC updates?
        auto ecocInsertReply = uassertStatusOK(queryImpl->insertDocument(
            nssEcoc,
            ECOCCollection::generateDocument(payload.fieldPathName,
                                             payload.payload.getEncryptedTokens()),
            pStmtId,
            false,
            bypassDocumentValidation));
        checkWriteErrors(ecocInsertReply);
    }
}

void processRemovedFields(FLEQueryInterface* queryImpl,
                          const NamespaceString& edcNss,
                          const EncryptedFieldConfig& efc,
                          const StringMap<FLEDeleteToken>& tokenMap,
                          const std::vector<EDCIndexedFields>& deletedFields,
                          int32_t* pStmtId) {

    NamespaceString nssEcc(edcNss.db(), efc.getEccCollection().get());


    auto docCount = queryImpl->countDocuments(nssEcc);

    TxnCollectionReader reader(docCount, queryImpl, nssEcc);


    for (const auto& deletedField : deletedFields) {
        // TODO - verify each indexed fields is listed in EncryptionInformation for the
        // schema

        auto it = tokenMap.find(deletedField.fieldPathName);
        uassert(6371304,
                str::stream() << "Could not find delete token for field: "
                              << deletedField.fieldPathName,
                it != tokenMap.end());

        auto deleteToken = it->second;

        auto [encryptedTypeBinding, subCdr] = fromEncryptedConstDataRange(deletedField.value);

        // TODO - add support other types
        uassert(6371305,
                "Ony support deleting equality indexed fields",
                encryptedTypeBinding == EncryptedBinDataType::kFLE2EqualityIndexedValue);

        auto plainTextField = uassertStatusOK(FLE2IndexedEqualityEncryptedValue::decryptAndParse(
            deleteToken.serverEncryptionToken, subCdr));

        auto tagToken =
            FLETwiceDerivedTokenGenerator::generateECCTwiceDerivedTagToken(plainTextField.ecc);
        auto valueToken =
            FLETwiceDerivedTokenGenerator::generateECCTwiceDerivedValueToken(plainTextField.ecc);

        auto alpha = ECCCollection::emuBinary(reader, tagToken, valueToken);

        uint64_t index = 0;
        if (alpha.has_value() && alpha.value() == 0) {
            index = 1;
        } else if (!alpha.has_value()) {
            auto block = ECCCollection::generateId(tagToken, boost::none);

            auto r_ecc = reader.getById(block);
            uassert(6371306, "ECC null document not found", !r_ecc.isEmpty());

            auto eccNullDoc =
                uassertStatusOK(ECCCollection::decryptNullDocument(valueToken, r_ecc));
            index = eccNullDoc.position + 2;
        } else {
            auto block = ECCCollection::generateId(tagToken, alpha);

            auto r_ecc = reader.getById(block);
            uassert(6371307, "ECC document not found", !r_ecc.isEmpty());

            auto eccDoc = uassertStatusOK(ECCCollection::decryptDocument(valueToken, r_ecc));

            if (eccDoc.valueType == ECCValueType::kCompactionPlaceholder) {
                uassertStatusOK(
                    Status(ErrorCodes::FLECompactionPlaceholder, "Found contention placeholder"));
            }

            index = alpha.value() + 1;
        }

        auto eccInsertReply = uassertStatusOK(queryImpl->insertDocument(
            nssEcc,
            ECCCollection::generateDocument(tagToken, valueToken, index, plainTextField.count),
            pStmtId,
            true));
        checkWriteErrors(eccInsertReply);

        NamespaceString nssEcoc(edcNss.db(), efc.getEcocCollection().get());

        // TODO - make this a batch of ECOC updates?
        EncryptedStateCollectionTokens tokens(plainTextField.esc, plainTextField.ecc);
        auto encryptedTokens = uassertStatusOK(tokens.serialize(deleteToken.ecocToken));
        auto ecocInsertReply = uassertStatusOK(queryImpl->insertDocument(
            nssEcoc,
            ECOCCollection::generateDocument(deletedField.fieldPathName, encryptedTokens),
            pStmtId,
            false));
        checkWriteErrors(ecocInsertReply);
    }
}

template <typename ReplyType>
std::shared_ptr<ReplyType> constructDefaultReply() {
    return std::make_shared<ReplyType>();
}

template <>
std::shared_ptr<write_ops::FindAndModifyCommandRequest> constructDefaultReply() {
    return std::make_shared<write_ops::FindAndModifyCommandRequest>(NamespaceString());
}

}  // namespace

template <typename ReplyType>
StatusWith<std::pair<ReplyType, OpMsgRequest>> processFindAndModifyRequest(
    OperationContext* opCtx,
    const write_ops::FindAndModifyCommandRequest& findAndModifyRequest,
    GetTxnCallback getTxns,
    ProcessFindAndModifyCallback<ReplyType> processCallback) {

    // Is this a delete
    bool isDelete = findAndModifyRequest.getRemove().value_or(false);

    // User can only specify either remove = true or update != {}
    uassert(6371401,
            "Must specify either update or remove to findAndModify, not both",
            !(findAndModifyRequest.getUpdate().has_value() && isDelete));

    uassert(6371402,
            "findAndModify with encryption only supports new: false",
            findAndModifyRequest.getNew().value_or(false) == false);

    uassert(6371408,
            "findAndModify fields must be empty",
            findAndModifyRequest.getFields().value_or(BSONObj()).isEmpty());

    // pipeline - is agg specific, delta is oplog, transform is internal (timeseries)
    auto updateModicationType =
        findAndModifyRequest.getUpdate().value_or(write_ops::UpdateModification()).type();
    uassert(6439901,
            "FLE only supports modifier and replacement style updates",
            updateModicationType == write_ops::UpdateModification::Type::kModifier ||
                updateModicationType == write_ops::UpdateModification::Type::kReplacement);

    std::shared_ptr<txn_api::SyncTransactionWithRetries> trun = getTxns(opCtx);

    // The function that handles the transaction may outlive this function so we need to use
    // shared_ptrs
    std::shared_ptr<ReplyType> reply = constructDefaultReply<ReplyType>();

    auto ownedRequest = findAndModifyRequest.serialize({});
    auto ownedFindAndModifyRequest = write_ops::FindAndModifyCommandRequest::parse(
        IDLParserErrorContext("findAndModify"), ownedRequest);

    auto expCtx = makeExpCtx(opCtx, ownedFindAndModifyRequest, ownedFindAndModifyRequest);
    auto findAndModifyBlock = std::make_tuple(ownedFindAndModifyRequest, expCtx);
    auto sharedFindAndModifyBlock =
        std::make_shared<decltype(findAndModifyBlock)>(findAndModifyBlock);

    auto swResult = trun->runNoThrow(
        opCtx,
        [sharedFindAndModifyBlock, ownedRequest, reply, processCallback](
            const txn_api::TransactionClient& txnClient, ExecutorPtr txnExec) {
            FLEQueryInterfaceImpl queryImpl(txnClient, getGlobalServiceContext());

            auto [findAndModifyRequest2, expCtx] = *sharedFindAndModifyBlock.get();

            if (MONGO_unlikely(fleCrudHangPreFindAndModify.shouldFail())) {
                LOGV2(6516704, "Hanging due to fleCrudHangPreFindAndModify fail point");
                fleCrudHangPreFindAndModify.pauseWhileSet();
            }

            *reply = processCallback(expCtx, &queryImpl, findAndModifyRequest2);

            if (MONGO_unlikely(fleCrudHangFindAndModify.shouldFail())) {
                LOGV2(6371900, "Hanging due to fleCrudHangFindAndModify fail point");
                fleCrudHangFindAndModify.pauseWhileSet();
            }

            return SemiFuture<void>::makeReady();
        });

    if (!swResult.isOK()) {
        return swResult.getStatus();
    } else if (!swResult.getValue().getEffectiveStatus().isOK()) {
        return swResult.getValue().getEffectiveStatus();
    }

    return std::pair<ReplyType, OpMsgRequest>{*reply, ownedRequest};
}

template StatusWith<std::pair<write_ops::FindAndModifyCommandReply, OpMsgRequest>>
processFindAndModifyRequest<write_ops::FindAndModifyCommandReply>(
    OperationContext* opCtx,
    const write_ops::FindAndModifyCommandRequest& findAndModifyRequest,
    GetTxnCallback getTxns,
    ProcessFindAndModifyCallback<write_ops::FindAndModifyCommandReply> processCallback);

template StatusWith<std::pair<write_ops::FindAndModifyCommandRequest, OpMsgRequest>>
processFindAndModifyRequest<write_ops::FindAndModifyCommandRequest>(
    OperationContext* opCtx,
    const write_ops::FindAndModifyCommandRequest& findAndModifyRequest,
    GetTxnCallback getTxns,
    ProcessFindAndModifyCallback<write_ops::FindAndModifyCommandRequest> processCallback);

FLEQueryInterface::~FLEQueryInterface() {}

StatusWith<write_ops::InsertCommandReply> processInsert(
    FLEQueryInterface* queryImpl,
    const NamespaceString& edcNss,
    std::vector<EDCServerPayloadInfo>& serverPayload,
    const EncryptedFieldConfig& efc,
    int32_t stmtId,
    BSONObj document,
    bool bypassDocumentValidation) {

    processFieldsForInsert(
        queryImpl, edcNss, serverPayload, efc, &stmtId, bypassDocumentValidation);

    auto finalDoc = EDCServerCollection::finalizeForInsert(document, serverPayload);

    return queryImpl->insertDocument(edcNss, finalDoc, &stmtId, false);
}

write_ops::DeleteCommandReply processDelete(FLEQueryInterface* queryImpl,
                                            boost::intrusive_ptr<ExpressionContext> expCtx,
                                            const write_ops::DeleteCommandRequest& deleteRequest) {

    auto edcNss = deleteRequest.getNamespace();
    auto ei = deleteRequest.getEncryptionInformation().get();

    auto efc = EncryptionInformationHelpers::getAndValidateSchema(edcNss, ei);
    auto tokenMap = EncryptionInformationHelpers::getDeleteTokens(edcNss, ei);
    int32_t stmtId = getStmtIdForWriteAt(deleteRequest, 0);

    auto newDeleteRequest = deleteRequest;

    auto newDeleteOp = newDeleteRequest.getDeletes()[0];
    newDeleteOp.setQ(fle::rewriteEncryptedFilterInsideTxn(
        queryImpl, deleteRequest.getDbName(), efc, expCtx, newDeleteOp.getQ()));
    newDeleteRequest.setDeletes({newDeleteOp});

    newDeleteRequest.getWriteCommandRequestBase().setStmtIds(boost::none);
    newDeleteRequest.getWriteCommandRequestBase().setStmtId(stmtId);
    ++stmtId;

    auto [deleteReply, deletedDocument] =
        queryImpl->deleteWithPreimage(edcNss, ei, newDeleteRequest);
    checkWriteErrors(deleteReply);

    // If the delete did not actually delete anything, we are done
    if (deletedDocument.isEmpty()) {
        write_ops::DeleteCommandReply reply;
        reply.getWriteCommandReplyBase().setN(0);
        return reply;
    }


    auto deletedFields = EDCServerCollection::getEncryptedIndexedFields(deletedDocument);

    processRemovedFields(queryImpl, edcNss, efc, tokenMap, deletedFields, &stmtId);

    return deleteReply;
}

/**
 * Update is the most complicated FLE operation.
 * It is basically an insert followed by a delete, sort of.
 *
 * 1. Process the update for any encrypted fields like insert, update the ESC and get new counters
 * 2. Extend the update $push new tags into the document
 * 3. Run the update with findAndModify to get the pre-image
 * 4. Run a find to get the post-image update with the id from the pre-image
 * -- Fail if we cannot find the new document. This could happen if they updated _id.
 * 5. Find the removed fields and update ECC
 * 6. Remove the stale tags from the original document with a new push
 */
write_ops::UpdateCommandReply processUpdate(FLEQueryInterface* queryImpl,
                                            boost::intrusive_ptr<ExpressionContext> expCtx,
                                            const write_ops::UpdateCommandRequest& updateRequest) {

    auto edcNss = updateRequest.getNamespace();
    auto ei = updateRequest.getEncryptionInformation().get();

    auto efc = EncryptionInformationHelpers::getAndValidateSchema(edcNss, ei);
    auto tokenMap = EncryptionInformationHelpers::getDeleteTokens(edcNss, ei);
    const auto updateOpEntry = updateRequest.getUpdates()[0];

    auto bypassDocumentValidation =
        updateRequest.getWriteCommandRequestBase().getBypassDocumentValidation();

    const auto updateModification = updateOpEntry.getU();

    int32_t stmtId = getStmtIdForWriteAt(updateRequest, 0);

    // Step 1 ----
    std::vector<EDCServerPayloadInfo> serverPayload;
    auto newUpdateOpEntry = updateRequest.getUpdates()[0];

    auto highCardinalityModeAllowed = newUpdateOpEntry.getUpsert()
        ? fle::HighCardinalityModeAllowed::kDisallow
        : fle::HighCardinalityModeAllowed::kAllow;

    newUpdateOpEntry.setQ(fle::rewriteEncryptedFilterInsideTxn(queryImpl,
                                                               updateRequest.getDbName(),
                                                               efc,
                                                               expCtx,
                                                               newUpdateOpEntry.getQ(),
                                                               highCardinalityModeAllowed));

    if (updateModification.type() == write_ops::UpdateModification::Type::kModifier) {
        auto updateModifier = updateModification.getUpdateModifier();
        auto setObject = updateModifier.getObjectField("$set");
        EDCServerCollection::validateEncryptedFieldInfo(setObject, efc, bypassDocumentValidation);
        serverPayload = EDCServerCollection::getEncryptedFieldInfo(setObject);

        processFieldsForInsert(
            queryImpl, edcNss, serverPayload, efc, &stmtId, bypassDocumentValidation);

        // Step 2 ----
        auto pushUpdate = EDCServerCollection::finalizeForUpdate(updateModifier, serverPayload);

        newUpdateOpEntry.setU(mongo::write_ops::UpdateModification(
            pushUpdate, write_ops::UpdateModification::ClassicTag(), false));
    } else {
        auto replacementDocument = updateModification.getUpdateReplacement();
        EDCServerCollection::validateEncryptedFieldInfo(
            replacementDocument, efc, bypassDocumentValidation);
        serverPayload = EDCServerCollection::getEncryptedFieldInfo(replacementDocument);

        processFieldsForInsert(
            queryImpl, edcNss, serverPayload, efc, &stmtId, bypassDocumentValidation);

        // Step 2 ----
        auto safeContentReplace =
            EDCServerCollection::finalizeForInsert(replacementDocument, serverPayload);

        newUpdateOpEntry.setU(mongo::write_ops::UpdateModification(
            safeContentReplace, write_ops::UpdateModification::ClassicTag(), true));
    }

    // Step 3 ----
    auto newUpdateRequest = updateRequest;
    newUpdateRequest.setUpdates({newUpdateOpEntry});
    newUpdateRequest.getWriteCommandRequestBase().setStmtIds(boost::none);
    newUpdateRequest.getWriteCommandRequestBase().setStmtId(stmtId);
    newUpdateRequest.getWriteCommandRequestBase().setBypassDocumentValidation(
        bypassDocumentValidation);
    ++stmtId;

    auto [updateReply, originalDocument] =
        queryImpl->updateWithPreimage(edcNss, ei, newUpdateRequest);
    if (originalDocument.isEmpty()) {
        // if there is no preimage, then we did not update any documents, we are done
        return updateReply;
    }

    // If there are errors, we are done
    if (updateReply.getWriteErrors().has_value() && !updateReply.getWriteErrors().value().empty()) {
        return updateReply;
    }

    // Step 4 ----
    auto idElement = originalDocument.firstElement();
    uassert(6371504,
            "Missing _id field in pre-image document",
            idElement.fieldNameStringData() == "_id"_sd);
    BSONObj newDocument = queryImpl->getById(edcNss, idElement);

    // Fail if we could not find the new document
    uassert(6371505, "Could not find pre-image document by _id", !newDocument.isEmpty());

    // Check the user did not remove/destroy the __safeContent__ array
    FLEClientCrypto::validateTagsArray(newDocument);

    // Step 5 ----
    auto originalFields = EDCServerCollection::getEncryptedIndexedFields(originalDocument);
    auto newFields = EDCServerCollection::getEncryptedIndexedFields(newDocument);
    auto deletedFields = EDCServerCollection::getRemovedTags(originalFields, newFields);

    processRemovedFields(queryImpl, edcNss, efc, tokenMap, deletedFields, &stmtId);

    // Step 6 ----
    BSONObj pullUpdate = EDCServerCollection::generateUpdateToRemoveTags(deletedFields, tokenMap);
    auto pullUpdateOpEntry = write_ops::UpdateOpEntry();
    pullUpdateOpEntry.setUpsert(false);
    pullUpdateOpEntry.setMulti(false);
    pullUpdateOpEntry.setQ(BSON("_id"_sd << idElement));
    pullUpdateOpEntry.setU(mongo::write_ops::UpdateModification(
        pullUpdate, write_ops::UpdateModification::ClassicTag(), false));
    newUpdateRequest.setUpdates({pullUpdateOpEntry});
    newUpdateRequest.getWriteCommandRequestBase().setStmtId(boost::none);
    newUpdateRequest.setLegacyRuntimeConstants(boost::none);
    newUpdateRequest.getWriteCommandRequestBase().setEncryptionInformation(boost::none);
    /* ignore */ queryImpl->update(edcNss, stmtId, newUpdateRequest);

    return updateReply;
}

FLEBatchResult processFLEBatch(OperationContext* opCtx,
                               const BatchedCommandRequest& request,
                               BatchWriteExecStats* stats,
                               BatchedCommandResponse* response,
                               boost::optional<OID> targetEpoch) {

    if (request.getWriteCommandRequestBase().getEncryptionInformation()->getCrudProcessed()) {
        return FLEBatchResult::kNotProcessed;
    }

    // TODO (SERVER-65077): Remove FCV check once 6.0 is released
    uassert(6371209,
            "Queryable Encryption is only supported when FCV supports 6.0",
            gFeatureFlagFLE2.isEnabled(serverGlobalParams.featureCompatibility));

    if (request.getBatchType() == BatchedCommandRequest::BatchType_Insert) {
        auto insertRequest = request.getInsertRequest();

        auto [batchResult, insertReply] =
            processInsert(opCtx, insertRequest, &getTransactionWithRetriesForMongoS);
        if (batchResult == FLEBatchResult::kNotProcessed) {
            return FLEBatchResult::kNotProcessed;
        }

        replyToResponse(opCtx, &insertReply.getWriteCommandReplyBase(), response);

        return FLEBatchResult::kProcessed;
    } else if (request.getBatchType() == BatchedCommandRequest::BatchType_Delete) {

        auto deleteRequest = request.getDeleteRequest();

        auto deleteReply = processDelete(opCtx, deleteRequest, &getTransactionWithRetriesForMongoS);

        replyToResponse(opCtx, &deleteReply.getWriteCommandReplyBase(), response);
        return FLEBatchResult::kProcessed;

    } else if (request.getBatchType() == BatchedCommandRequest::BatchType_Update) {

        auto updateRequest = request.getUpdateRequest();

        auto updateReply = processUpdate(opCtx, updateRequest, &getTransactionWithRetriesForMongoS);

        replyToResponse(opCtx, &updateReply.getWriteCommandReplyBase(), response);

        response->setNModified(updateReply.getNModified());

        if (updateReply.getUpserted().has_value() && updateReply.getUpserted().value().size() > 0) {

            auto upsertReply = updateReply.getUpserted().value()[0];

            BatchedUpsertDetail upsert;
            upsert.setIndex(upsertReply.getIndex());
            upsert.setUpsertedID(upsertReply.get_id().getElement().wrap(""));

            std::vector<BatchedUpsertDetail*> upserts;
            upserts.push_back(&upsert);

            response->setUpsertDetails(upserts);
        }

        return FLEBatchResult::kProcessed;
    }

    MONGO_UNREACHABLE;
}

std::unique_ptr<BatchedCommandRequest> processFLEBatchExplain(
    OperationContext* opCtx, const BatchedCommandRequest& request) {
    invariant(request.hasEncryptionInformation());
    auto getExpCtx = [&](const auto& op) {
        auto expCtx = make_intrusive<ExpressionContext>(
            opCtx,
            fle::collatorFromBSON(opCtx, op.getCollation().value_or(BSONObj())),
            request.getNS(),
            request.getLegacyRuntimeConstants(),
            request.getLet());
        expCtx->stopExpressionCounters();
        return expCtx;
    };

    if (request.getBatchType() == BatchedCommandRequest::BatchType_Delete) {
        auto deleteRequest = request.getDeleteRequest();
        auto newDeleteOp = deleteRequest.getDeletes()[0];
        newDeleteOp.setQ(fle::rewriteQuery(opCtx,
                                           getExpCtx(newDeleteOp),
                                           request.getNS(),
                                           deleteRequest.getEncryptionInformation().get(),
                                           newDeleteOp.getQ(),
                                           &getTransactionWithRetriesForMongoS,
                                           fle::HighCardinalityModeAllowed::kAllow));
        deleteRequest.setDeletes({newDeleteOp});
        deleteRequest.getWriteCommandRequestBase().setEncryptionInformation(boost::none);
        return std::make_unique<BatchedCommandRequest>(deleteRequest);
    } else if (request.getBatchType() == BatchedCommandRequest::BatchType_Update) {
        auto updateRequest = request.getUpdateRequest();
        auto newUpdateOp = updateRequest.getUpdates()[0];
        auto highCardinalityModeAllowed = newUpdateOp.getUpsert()
            ? fle::HighCardinalityModeAllowed::kDisallow
            : fle::HighCardinalityModeAllowed::kAllow;

        newUpdateOp.setQ(fle::rewriteQuery(opCtx,
                                           getExpCtx(newUpdateOp),
                                           request.getNS(),
                                           updateRequest.getEncryptionInformation().get(),
                                           newUpdateOp.getQ(),
                                           &getTransactionWithRetriesForMongoS,
                                           highCardinalityModeAllowed));
        updateRequest.setUpdates({newUpdateOp});
        updateRequest.getWriteCommandRequestBase().setEncryptionInformation(boost::none);
        return std::make_unique<BatchedCommandRequest>(updateRequest);
    }
    MONGO_UNREACHABLE;
}

// See processUpdate for algorithm overview
write_ops::FindAndModifyCommandReply processFindAndModify(
    boost::intrusive_ptr<ExpressionContext> expCtx,
    FLEQueryInterface* queryImpl,
    const write_ops::FindAndModifyCommandRequest& findAndModifyRequest) {

    auto edcNss = findAndModifyRequest.getNamespace();
    auto ei = findAndModifyRequest.getEncryptionInformation().get();

    auto efc = EncryptionInformationHelpers::getAndValidateSchema(edcNss, ei);
    auto tokenMap = EncryptionInformationHelpers::getDeleteTokens(edcNss, ei);
    int32_t stmtId = findAndModifyRequest.getStmtId().value_or(0);

    auto newFindAndModifyRequest = findAndModifyRequest;

    const auto bypassDocumentValidation =
        findAndModifyRequest.getBypassDocumentValidation().value_or(false);

    // Step 0 ----
    // Rewrite filter
    auto highCardinalityModeAllowed = findAndModifyRequest.getUpsert().value_or(false)
        ? fle::HighCardinalityModeAllowed::kDisallow
        : fle::HighCardinalityModeAllowed::kAllow;

    newFindAndModifyRequest.setQuery(
        fle::rewriteEncryptedFilterInsideTxn(queryImpl,
                                             edcNss.db(),
                                             efc,
                                             expCtx,
                                             findAndModifyRequest.getQuery(),
                                             highCardinalityModeAllowed));

    // Make sure not to inherit the command's writeConcern, this should be set at the transaction
    // level.
    newFindAndModifyRequest.setWriteConcern(boost::none);

    // Step 1 ----
    // If we have an update object, we have to process for ESC
    if (findAndModifyRequest.getUpdate().has_value()) {

        std::vector<EDCServerPayloadInfo> serverPayload;
        const auto updateModification = findAndModifyRequest.getUpdate().value();
        write_ops::UpdateModification newUpdateModification;

        if (updateModification.type() == write_ops::UpdateModification::Type::kModifier) {
            auto updateModifier = updateModification.getUpdateModifier();
            auto setObject = updateModifier.getObjectField("$set");
            EDCServerCollection::validateEncryptedFieldInfo(
                setObject, efc, bypassDocumentValidation);
            serverPayload = EDCServerCollection::getEncryptedFieldInfo(setObject);
            processFieldsForInsert(
                queryImpl, edcNss, serverPayload, efc, &stmtId, bypassDocumentValidation);

            auto pushUpdate = EDCServerCollection::finalizeForUpdate(updateModifier, serverPayload);

            // Step 2 ----
            newUpdateModification = write_ops::UpdateModification(
                pushUpdate, write_ops::UpdateModification::ClassicTag(), false);
        } else {
            auto replacementDocument = updateModification.getUpdateReplacement();
            EDCServerCollection::validateEncryptedFieldInfo(
                replacementDocument, efc, bypassDocumentValidation);
            serverPayload = EDCServerCollection::getEncryptedFieldInfo(replacementDocument);

            processFieldsForInsert(
                queryImpl, edcNss, serverPayload, efc, &stmtId, bypassDocumentValidation);

            // Step 2 ----
            auto safeContentReplace =
                EDCServerCollection::finalizeForInsert(replacementDocument, serverPayload);

            newUpdateModification = write_ops::UpdateModification(
                safeContentReplace, write_ops::UpdateModification::ClassicTag(), true);
        }

        // Step 3 ----
        newFindAndModifyRequest.setUpdate(newUpdateModification);
    }

    newFindAndModifyRequest.setNew(false);
    newFindAndModifyRequest.setStmtId(stmtId);
    ++stmtId;

    auto reply = queryImpl->findAndModify(edcNss, ei, newFindAndModifyRequest);
    if (!reply.getValue().has_value() || reply.getValue().value().isEmpty()) {
        // if there is no preimage, then we did not update or delete any documents, we are done
        return reply;
    }

    // Step 4 ----
    BSONObj originalDocument = reply.getValue().value();
    auto idElement = originalDocument.firstElement();
    uassert(6371403,
            "Missing _id field in pre-image document, the fields document must contain _id",
            idElement.fieldNameStringData() == "_id"_sd);

    BSONObj newDocument;
    std::vector<EDCIndexedFields> newFields;

    // Is this a delete
    bool isDelete = findAndModifyRequest.getRemove().value_or(false);

    // Unlike update, there will not always be a new document since users can delete the document
    if (!isDelete) {
        newDocument = queryImpl->getById(edcNss, idElement);

        // Fail if we could not find the new document
        uassert(6371404, "Could not find pre-image document by _id", !newDocument.isEmpty());

        // Check the user did not remove/destroy the __safeContent__ array
        FLEClientCrypto::validateTagsArray(newDocument);

        newFields = EDCServerCollection::getEncryptedIndexedFields(newDocument);
    }

    // Step 5 ----
    auto originalFields = EDCServerCollection::getEncryptedIndexedFields(originalDocument);
    auto deletedFields = EDCServerCollection::getRemovedTags(originalFields, newFields);

    processRemovedFields(queryImpl, edcNss, efc, tokenMap, deletedFields, &stmtId);

    // Step 6 ----
    // We don't need to make a second update in the case of a delete
    if (!isDelete) {
        BSONObj pullUpdate =
            EDCServerCollection::generateUpdateToRemoveTags(deletedFields, tokenMap);
        auto newUpdateRequest =
            write_ops::UpdateCommandRequest(findAndModifyRequest.getNamespace());
        auto pullUpdateOpEntry = write_ops::UpdateOpEntry();
        pullUpdateOpEntry.setUpsert(false);
        pullUpdateOpEntry.setMulti(false);
        pullUpdateOpEntry.setQ(BSON("_id"_sd << idElement));
        pullUpdateOpEntry.setU(mongo::write_ops::UpdateModification(
            pullUpdate, write_ops::UpdateModification::ClassicTag(), false));
        newUpdateRequest.setUpdates({pullUpdateOpEntry});
        newUpdateRequest.setLegacyRuntimeConstants(boost::none);
        newUpdateRequest.getWriteCommandRequestBase().setStmtId(boost::none);
        newUpdateRequest.getWriteCommandRequestBase().setEncryptionInformation(boost::none);

        auto finalUpdateReply = queryImpl->update(edcNss, stmtId, newUpdateRequest);
        checkWriteErrors(finalUpdateReply);
    }

    return reply;
}

write_ops::FindAndModifyCommandRequest processFindAndModifyExplain(
    boost::intrusive_ptr<ExpressionContext> expCtx,
    FLEQueryInterface* queryImpl,
    const write_ops::FindAndModifyCommandRequest& findAndModifyRequest) {

    auto edcNss = findAndModifyRequest.getNamespace();
    auto ei = findAndModifyRequest.getEncryptionInformation().get();

    auto efc = EncryptionInformationHelpers::getAndValidateSchema(edcNss, ei);

    auto newFindAndModifyRequest = findAndModifyRequest;
    auto highCardinalityModeAllowed = findAndModifyRequest.getUpsert().value_or(false)
        ? fle::HighCardinalityModeAllowed::kDisallow
        : fle::HighCardinalityModeAllowed::kAllow;

    newFindAndModifyRequest.setQuery(
        fle::rewriteEncryptedFilterInsideTxn(queryImpl,
                                             edcNss.db(),
                                             efc,
                                             expCtx,
                                             findAndModifyRequest.getQuery(),
                                             highCardinalityModeAllowed));

    newFindAndModifyRequest.setEncryptionInformation(boost::none);
    return newFindAndModifyRequest;
}

FLEBatchResult processFLEFindAndModify(OperationContext* opCtx,
                                       const BSONObj& cmdObj,
                                       BSONObjBuilder& result) {
    // There is no findAndModify parsing in mongos so we need to first parse to decide if it is for
    // FLE2
    auto request = write_ops::FindAndModifyCommandRequest::parse(
        IDLParserErrorContext("findAndModify"), cmdObj);

    if (!request.getEncryptionInformation().has_value()) {
        return FLEBatchResult::kNotProcessed;
    }

    // TODO (SERVER-65077): Remove FCV check once 6.0 is released
    if (!gFeatureFlagFLE2.isEnabled(serverGlobalParams.featureCompatibility)) {
        uasserted(6371405, "Queryable Encryption is only supported when FCV supports 6.0");
    }

    // FLE2 Mongos CRUD operations loopback through MongoS with EncryptionInformation as
    // findAndModify so query can do any necessary transformations. But on the nested call, CRUD
    // does not need to do any more work.
    if (request.getEncryptionInformation()->getCrudProcessed()) {
        return FLEBatchResult::kNotProcessed;
    }

    auto swReply = processFindAndModifyRequest<write_ops::FindAndModifyCommandReply>(
        opCtx, request, &getTransactionWithRetriesForMongoS);

    auto reply = uassertStatusOK(swReply).first;

    reply.serialize(&result);

    return FLEBatchResult::kProcessed;
}

std::pair<write_ops::FindAndModifyCommandRequest, OpMsgRequest>
processFLEFindAndModifyExplainMongos(OperationContext* opCtx,
                                     const write_ops::FindAndModifyCommandRequest& request) {
    tassert(6513400,
            "Missing encryptionInformation for findAndModify",
            request.getEncryptionInformation().has_value());

    return uassertStatusOK(processFindAndModifyRequest<write_ops::FindAndModifyCommandRequest>(
        opCtx, request, &getTransactionWithRetriesForMongoS, processFindAndModifyExplain));
}

BSONObj FLEQueryInterfaceImpl::getById(const NamespaceString& nss, BSONElement element) {
    FindCommandRequest find(nss);
    find.setFilter(BSON("_id" << element));
    find.setSingleBatch(true);

    // Throws on error
    auto docs = _txnClient.exhaustiveFind(find).get();

    if (docs.size() == 0) {
        return BSONObj();
    } else {
        // We only expect one document in the state collection considering that _id is a unique
        // index
        uassert(6371201,
                "Unexpected to find more then one FLE state collection document",
                docs.size() == 1);
        return docs[0];
    }
}

uint64_t FLEQueryInterfaceImpl::countDocuments(const NamespaceString& nss) {
    // Since count() does not work in a transaction, call count() by bypassing the transaction api
    invariant(!haveClient());
    auto client = _serviceContext->makeClient("SEP-int-fle-crud");
    AlternativeClientRegion clientRegion(client);
    auto opCtx = cc().makeOperationContext();
    auto as = AuthorizationSession::get(cc());
    as->grantInternalAuthorization(opCtx.get());

    CountCommandRequest ccr(nss);
    auto opMsgRequest = ccr.serialize(BSONObj());

    DBDirectClient directClient(opCtx.get());
    auto uniqueReply = directClient.runCommand(opMsgRequest);

    auto reply = uniqueReply->getCommandReply();

    auto status = getStatusFromWriteCommandReply(reply);
    uassertStatusOK(status);

    int64_t signedDocCount = reply.getIntField("n"_sd);
    if (signedDocCount < 0) {
        signedDocCount = 0;
    }

    return static_cast<uint64_t>(signedDocCount);
}

StatusWith<write_ops::InsertCommandReply> FLEQueryInterfaceImpl::insertDocument(
    const NamespaceString& nss,
    BSONObj obj,
    StmtId* pStmtId,
    bool translateDuplicateKey,
    bool bypassDocumentValidation) {
    write_ops::InsertCommandRequest insertRequest(nss);
    insertRequest.setDocuments({obj});

    EncryptionInformation encryptionInformation;
    encryptionInformation.setCrudProcessed(true);

    // We need to set an empty BSON object here for the schema.
    encryptionInformation.setSchema(BSONObj());
    insertRequest.getWriteCommandRequestBase().setEncryptionInformation(encryptionInformation);
    insertRequest.getWriteCommandRequestBase().setBypassDocumentValidation(
        bypassDocumentValidation);

    int32_t stmtId = *pStmtId;
    if (stmtId != kUninitializedStmtId) {
        (*pStmtId)++;
    }

    auto response = _txnClient.runCRUDOp(BatchedCommandRequest(insertRequest), {stmtId}).get();

    auto status = response.toStatus();

    write_ops::InsertCommandReply reply;

    responseToReply(response, reply.getWriteCommandReplyBase());

    return {reply};
}

std::pair<write_ops::DeleteCommandReply, BSONObj> FLEQueryInterfaceImpl::deleteWithPreimage(
    const NamespaceString& nss,
    const EncryptionInformation& ei,
    const write_ops::DeleteCommandRequest& deleteRequest) {
    // We only support a single delete
    dassert(deleteRequest.getStmtIds().value_or(std::vector<int32_t>()).empty());

    auto deleteOpEntry = deleteRequest.getDeletes()[0];

    write_ops::FindAndModifyCommandRequest findAndModifyRequest(nss);
    findAndModifyRequest.setQuery(deleteOpEntry.getQ());
    findAndModifyRequest.setHint(deleteOpEntry.getHint());
    findAndModifyRequest.setBatchSize(1);
    findAndModifyRequest.setSingleBatch(true);
    findAndModifyRequest.setRemove(true);
    findAndModifyRequest.setCollation(deleteOpEntry.getCollation());
    findAndModifyRequest.setLet(deleteRequest.getLet());
    findAndModifyRequest.setStmtId(deleteRequest.getStmtId());

    auto ei2 = ei;
    ei2.setCrudProcessed(true);
    findAndModifyRequest.setEncryptionInformation(ei2);

    auto response = _txnClient.runCommand(nss.db(), findAndModifyRequest.toBSON({})).get();
    auto status = getStatusFromWriteCommandReply(response);

    BSONObj returnObj;
    write_ops::DeleteCommandReply deleteReply;

    if (!status.isOK()) {
        deleteReply.getWriteCommandReplyBase().setN(0);
        deleteReply.getWriteCommandReplyBase().setWriteErrors(singleStatusToWriteErrors(status));
    } else {
        auto reply =
            write_ops::FindAndModifyCommandReply::parse(IDLParserErrorContext("reply"), response);

        if (reply.getLastErrorObject().getNumDocs() > 0) {
            deleteReply.getWriteCommandReplyBase().setN(1);
        }

        returnObj = reply.getValue().value_or(BSONObj());
    }

    return {deleteReply, returnObj};
}

std::pair<write_ops::UpdateCommandReply, BSONObj> FLEQueryInterfaceImpl::updateWithPreimage(
    const NamespaceString& nss,
    const EncryptionInformation& ei,
    const write_ops::UpdateCommandRequest& updateRequest) {
    // We only support a single update
    dassert(updateRequest.getStmtIds().value_or(std::vector<int32_t>()).empty());

    auto updateOpEntry = updateRequest.getUpdates()[0];

    write_ops::FindAndModifyCommandRequest findAndModifyRequest(nss);
    findAndModifyRequest.setQuery(updateOpEntry.getQ());
    findAndModifyRequest.setUpdate(updateOpEntry.getU());
    findAndModifyRequest.setBatchSize(1);
    findAndModifyRequest.setUpsert(updateOpEntry.getUpsert());
    findAndModifyRequest.setSingleBatch(true);
    findAndModifyRequest.setRemove(false);
    findAndModifyRequest.setArrayFilters(updateOpEntry.getArrayFilters());
    findAndModifyRequest.setCollation(updateOpEntry.getCollation());
    findAndModifyRequest.setHint(updateOpEntry.getHint());
    findAndModifyRequest.setLet(
        mergeLetAndCVariables(updateRequest.getLet(), updateOpEntry.getC()));
    findAndModifyRequest.setStmtId(updateRequest.getStmtId());
    findAndModifyRequest.setBypassDocumentValidation(updateRequest.getBypassDocumentValidation());

    auto ei2 = ei;
    ei2.setCrudProcessed(true);
    findAndModifyRequest.setEncryptionInformation(ei2);

    auto response = _txnClient.runCommand(nss.db(), findAndModifyRequest.toBSON({})).get();
    auto status = getStatusFromWriteCommandReply(response);
    uassertStatusOK(status);

    auto reply =
        write_ops::FindAndModifyCommandReply::parse(IDLParserErrorContext("reply"), response);

    write_ops::UpdateCommandReply updateReply;

    if (!status.isOK()) {
        updateReply.getWriteCommandReplyBase().setN(0);
        updateReply.getWriteCommandReplyBase().setWriteErrors(singleStatusToWriteErrors(status));
    } else {
        if (reply.getRetriedStmtId().has_value()) {
            updateReply.getWriteCommandReplyBase().setRetriedStmtIds(
                std::vector<std::int32_t>{reply.getRetriedStmtId().value()});
        }
        updateReply.getWriteCommandReplyBase().setN(reply.getLastErrorObject().getNumDocs());

        if (reply.getLastErrorObject().getUpserted().has_value()) {
            write_ops::Upserted upserted;
            upserted.setIndex(0);
            upserted.set_id(reply.getLastErrorObject().getUpserted().value());
            updateReply.setUpserted(std::vector<mongo::write_ops::Upserted>{upserted});
        }

        if (reply.getLastErrorObject().getNumDocs() > 0) {
            updateReply.setNModified(1);
            updateReply.getWriteCommandReplyBase().setN(1);
        }
    }

    return {updateReply, reply.getValue().value_or(BSONObj())};
}

write_ops::UpdateCommandReply FLEQueryInterfaceImpl::update(
    const NamespaceString& nss, int32_t stmtId, write_ops::UpdateCommandRequest& updateRequest) {

    invariant(!updateRequest.getWriteCommandRequestBase().getEncryptionInformation());

    EncryptionInformation encryptionInformation;
    encryptionInformation.setCrudProcessed(true);

    encryptionInformation.setSchema(BSONObj());
    updateRequest.getWriteCommandRequestBase().setEncryptionInformation(encryptionInformation);

    dassert(updateRequest.getStmtIds().value_or(std::vector<int32_t>()).empty());

    auto response = _txnClient.runCRUDOp(BatchedCommandRequest(updateRequest), {stmtId}).get();

    write_ops::UpdateCommandReply reply;

    responseToReply(response, reply.getWriteCommandReplyBase());

    reply.setNModified(response.getNModified());

    return {reply};
}

write_ops::FindAndModifyCommandReply FLEQueryInterfaceImpl::findAndModify(
    const NamespaceString& nss,
    const EncryptionInformation& ei,
    const write_ops::FindAndModifyCommandRequest& findAndModifyRequest) {

    auto newFindAndModifyRequest = findAndModifyRequest;
    auto ei2 = ei;
    ei2.setCrudProcessed(true);
    newFindAndModifyRequest.setEncryptionInformation(ei2);
    // WriteConcern is set at the transaction level so strip it out
    newFindAndModifyRequest.setWriteConcern(boost::none);

    auto response = _txnClient.runCommand(nss.db(), newFindAndModifyRequest.toBSON({})).get();
    auto status = getStatusFromWriteCommandReply(response);
    uassertStatusOK(status);

    return write_ops::FindAndModifyCommandReply::parse(IDLParserErrorContext("reply"), response);
}

std::vector<BSONObj> FLEQueryInterfaceImpl::findDocuments(const NamespaceString& nss,
                                                          BSONObj filter) {
    FindCommandRequest find(nss);
    find.setFilter(filter);

    // Throws on error
    return _txnClient.exhaustiveFind(find).get();
}

void processFLEFindS(OperationContext* opCtx,
                     const NamespaceString& nss,
                     FindCommandRequest* findCommand) {
    fle::processFindCommand(opCtx, nss, findCommand, &getTransactionWithRetriesForMongoS);
}

void processFLECountS(OperationContext* opCtx,
                      const NamespaceString& nss,
                      CountCommandRequest* countCommand) {
    fle::processCountCommand(opCtx, nss, countCommand, &getTransactionWithRetriesForMongoS);
}

std::unique_ptr<Pipeline, PipelineDeleter> processFLEPipelineS(
    OperationContext* opCtx,
    NamespaceString nss,
    const EncryptionInformation& encryptInfo,
    std::unique_ptr<Pipeline, PipelineDeleter> toRewrite) {
    return fle::processPipeline(
        opCtx, nss, encryptInfo, std::move(toRewrite), &getTransactionWithRetriesForMongoS);
}
}  // namespace mongo
