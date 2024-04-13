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

#include "mongo/platform/basic.h"

#include "mongo/db/catalog_raii.h"
#include "mongo/db/repl/tenant_migration_access_blocker_util.h"
#include "mongo/db/serverless/shard_split_donor_op_observer.h"
#include "mongo/db/serverless/shard_split_state_machine_gen.h"
#include "mongo/db/serverless/shard_split_utils.h"

namespace mongo {
namespace {

bool isSecondary(const OperationContext* opCtx) {
    return !opCtx->writesAreReplicated();
}

const auto tenantIdsToDeleteDecoration =
    OperationContext::declareDecoration<boost::optional<std::vector<std::string>>>();

ShardSplitDonorDocument parseAndValidateDonorDocument(const BSONObj& doc) {
    auto donorStateDoc =
        ShardSplitDonorDocument::parse(IDLParserErrorContext("donorStateDoc"), doc);
    const std::string errmsg = "Invalid donor state doc, {}: {}";

    switch (donorStateDoc.getState()) {
        case ShardSplitDonorStateEnum::kUninitialized:
            uassert(ErrorCodes::BadValue,
                    fmt::format(errmsg,
                                "BlockTimeStamp should not be set in data sync state",
                                doc.toString()),
                    !donorStateDoc.getBlockTimestamp());
            uassert(ErrorCodes::BadValue,
                    fmt::format(errmsg,
                                "CommitOrAbortOpTime should not be set in data sync state",
                                doc.toString()),
                    !donorStateDoc.getCommitOrAbortOpTime());
            uassert(ErrorCodes::BadValue,
                    fmt::format(errmsg,
                                "Cannot have abortReason while being in data sync state",
                                doc.toString()),
                    !donorStateDoc.getAbortReason());
            break;
        case ShardSplitDonorStateEnum::kBlocking:
            uassert(ErrorCodes::BadValue,
                    fmt::format(errmsg,
                                "Missing blockTimeStamp while being in blocking state",
                                doc.toString()),
                    donorStateDoc.getBlockTimestamp());
            uassert(
                ErrorCodes::BadValue,
                fmt::format(errmsg,
                            "CommitOrAbortOpTime shouldn't be set while being in blocking state",
                            doc.toString()),
                !donorStateDoc.getCommitOrAbortOpTime());
            uassert(ErrorCodes::BadValue,
                    fmt::format(errmsg,
                                "Cannot have an abortReason while being in blocking state",
                                doc.toString()),
                    !donorStateDoc.getAbortReason());
            break;
        case ShardSplitDonorStateEnum::kCommitted:
            uassert(ErrorCodes::BadValue,
                    fmt::format(errmsg,
                                "Missing blockTimeStamp while being in committed state",
                                doc.toString()),
                    donorStateDoc.getBlockTimestamp());
            uassert(ErrorCodes::BadValue,
                    fmt::format(errmsg,
                                "Missing CommitOrAbortOpTime while being in committed state",
                                doc.toString()),
                    donorStateDoc.getCommitOrAbortOpTime());
            uassert(ErrorCodes::BadValue,
                    fmt::format(errmsg,
                                "Cannot have abortReason while being in committed state",
                                doc.toString()),
                    !donorStateDoc.getAbortReason());
            break;
        case ShardSplitDonorStateEnum::kAborted:
            uassert(ErrorCodes::BadValue,
                    fmt::format(
                        errmsg, "Missing abortReason while being in aborted state", doc.toString()),
                    donorStateDoc.getAbortReason());
            uassert(ErrorCodes::BadValue,
                    fmt::format(errmsg,
                                "Missing CommitOrAbortOpTime while being in aborted state",
                                doc.toString()),
                    donorStateDoc.getCommitOrAbortOpTime());
            break;
        default:
            MONGO_UNREACHABLE;
    }

    return donorStateDoc;
}

/**
 * Initializes the TenantMigrationDonorAccessBlocker for the tenant migration denoted by the given
 * state doc.
 */
void onBlockerInitialization(OperationContext* opCtx,
                             const ShardSplitDonorDocument& donorStateDoc) {
    invariant(donorStateDoc.getState() == ShardSplitDonorStateEnum::kBlocking);
    invariant(donorStateDoc.getBlockTimestamp());

    auto optionalTenants = donorStateDoc.getTenantIds();
    invariant(optionalTenants);

    const auto& tenantIds = optionalTenants.get();

    // The primary create and sets the tenant access blocker to blocking within the
    // ShardSplitDonorService.
    if (isSecondary(opCtx)) {
        auto recipientTagName = donorStateDoc.getRecipientTagName();
        auto recipientSetName = donorStateDoc.getRecipientSetName();
        invariant(recipientTagName);
        invariant(recipientSetName);

        auto config = repl::ReplicationCoordinator::get(cc().getServiceContext())->getConfig();
        auto recipientConnectionString =
            serverless::makeRecipientConnectionString(config, *recipientTagName, *recipientSetName);

        for (const auto& tenantId : tenantIds) {
            auto mtab = std::make_shared<TenantMigrationDonorAccessBlocker>(
                opCtx->getServiceContext(),
                donorStateDoc.getId(),
                tenantId.toString(),
                MigrationProtocolEnum::kMultitenantMigrations,
                recipientConnectionString.toString());

            TenantMigrationAccessBlockerRegistry::get(opCtx->getServiceContext())
                .add(tenantId, mtab);

            // No rollback handler is necessary as the write should not fail on secondaries.
            mtab->startBlockingWrites();
        }
    }

    for (const auto& tenantId : tenantIds) {
        auto mtab = tenant_migration_access_blocker::getTenantMigrationDonorAccessBlocker(
            opCtx->getServiceContext(), tenantId);
        invariant(mtab);

        mtab->startBlockingReadsAfter(donorStateDoc.getBlockTimestamp().get());
    }
}

/**
 * Transitions the TenantMigrationDonorAccessBlocker to the committed state.
 */
void onTransitionToCommitted(OperationContext* opCtx,
                             const ShardSplitDonorDocument& donorStateDoc) {
    invariant(donorStateDoc.getState() == ShardSplitDonorStateEnum::kCommitted);
    invariant(donorStateDoc.getCommitOrAbortOpTime());

    auto tenants = donorStateDoc.getTenantIds();
    invariant(tenants);

    for (const auto& tenantId : tenants.get()) {
        auto mtab = tenant_migration_access_blocker::getTenantMigrationDonorAccessBlocker(
            opCtx->getServiceContext(), tenantId);
        invariant(mtab);

        mtab->setCommitOpTime(opCtx, donorStateDoc.getCommitOrAbortOpTime().get());
    }
}

/**
 * Transitions the TenantMigrationDonorAccessBlocker to the aborted state.
 */
void onTransitionToAborted(OperationContext* opCtx, const ShardSplitDonorDocument& donorStateDoc) {
    invariant(donorStateDoc.getState() == ShardSplitDonorStateEnum::kAborted);
    invariant(donorStateDoc.getCommitOrAbortOpTime());

    auto tenants = donorStateDoc.getTenantIds();
    if (!tenants) {
        // The only case where there can be no tenants is when the instance is created by the abort
        // command. In that case, no tenant migration blockers are created and the state will go
        // straight to abort.
        invariant(donorStateDoc.getState() == ShardSplitDonorStateEnum::kUninitialized);
        return;
    }

    for (const auto& tenantId : tenants.get()) {
        auto mtab = tenant_migration_access_blocker::getTenantMigrationDonorAccessBlocker(
            opCtx->getServiceContext(), tenantId);
        invariant(mtab);

        mtab->setAbortOpTime(opCtx, donorStateDoc.getCommitOrAbortOpTime().get());
    }
}

/**
 * Used to update the TenantMigrationDonorAccessBlocker for the migration denoted by the donor's
 * state doc once the write for updating the doc is committed.
 */
class TenantMigrationDonorCommitOrAbortHandler final : public RecoveryUnit::Change {
public:
    TenantMigrationDonorCommitOrAbortHandler(OperationContext* opCtx,
                                             ShardSplitDonorDocument donorStateDoc)
        : _opCtx(opCtx), _donorStateDoc(std::move(donorStateDoc)) {}

    void commit(boost::optional<Timestamp>) override {
        if (_donorStateDoc.getExpireAt()) {
            if (_donorStateDoc.getTenantIds()) {
                auto tenantIds = _donorStateDoc.getTenantIds().get();
                for (auto tenantId : tenantIds) {
                    auto mtab =
                        tenant_migration_access_blocker::getTenantMigrationDonorAccessBlocker(
                            _opCtx->getServiceContext(), tenantId);

                    if (!mtab) {
                        // The state doc and TenantMigrationDonorAccessBlocker for this migration
                        // were removed immediately after expireAt was set. This is unlikely to
                        // occur in production where the garbage collection delay should be
                        // sufficiently large.
                        continue;
                    }

                    if (!_opCtx->writesAreReplicated()) {
                        // Setting expireAt implies that the TenantMigrationDonorAccessBlocker for
                        // this migration will be removed shortly after this. However, a lagged
                        // secondary might not manage to advance its majority commit point past the
                        // migration commit or abort opTime and consequently transition out of the
                        // blocking state before the TenantMigrationDonorAccessBlocker is removed.
                        // When this occurs, blocked reads or writes will be left waiting for the
                        // migration decision indefinitely. To avoid that, notify the
                        // TenantMigrationDonorAccessBlocker here that the commit or abort opTime
                        // has been majority committed (guaranteed to be true since by design the
                        // donor never marks its state doc as garbage collectable before the
                        // migration decision is majority committed).
                        mtab->onMajorityCommitPointUpdate(
                            _donorStateDoc.getCommitOrAbortOpTime().get());
                    }

                    if (_donorStateDoc.getState() == ShardSplitDonorStateEnum::kAborted) {
                        invariant(mtab->inStateAborted());
                        // The migration durably aborted and is now marked as garbage collectable,
                        // remove its TenantMigrationDonorAccessBlocker right away to allow
                        // back-to-back migration retries.
                        TenantMigrationAccessBlockerRegistry::get(_opCtx->getServiceContext())
                            .remove(tenantId, TenantMigrationAccessBlocker::BlockerType::kDonor);
                    }
                }
            }
            return;
        }

        switch (_donorStateDoc.getState()) {
            case ShardSplitDonorStateEnum::kCommitted:
                onTransitionToCommitted(_opCtx, _donorStateDoc);
                break;
            case ShardSplitDonorStateEnum::kAborted:
                onTransitionToAborted(_opCtx, _donorStateDoc);
                break;
            default:
                MONGO_UNREACHABLE;
        }
    }

    void rollback() override {}

private:
    OperationContext* _opCtx;
    const ShardSplitDonorDocument _donorStateDoc;
};

}  // namespace

void ShardSplitDonorOpObserver::onInserts(OperationContext* opCtx,
                                          const NamespaceString& nss,
                                          const UUID& uuid,
                                          std::vector<InsertStatement>::const_iterator first,
                                          std::vector<InsertStatement>::const_iterator last,
                                          bool fromMigrate) {
    if (nss != NamespaceString::kTenantSplitDonorsNamespace ||
        tenant_migration_access_blocker::inRecoveryMode(opCtx)) {
        return;
    }

    for (auto it = first; it != last; it++) {
        auto donorStateDoc = parseAndValidateDonorDocument(it->doc);
        switch (donorStateDoc.getState()) {
            case ShardSplitDonorStateEnum::kBlocking:
                onBlockerInitialization(opCtx, donorStateDoc);
                break;
            case ShardSplitDonorStateEnum::kAborted:
                // If the operation starts aborted, do not do anything.
                break;
            case ShardSplitDonorStateEnum::kUninitialized:
            case ShardSplitDonorStateEnum::kCommitted:
                uasserted(ErrorCodes::IllegalOperation,
                          "cannot insert a donor's state doc with 'state' other than 'kAborted' or "
                          "'kBlocking'");
                break;
            default:
                MONGO_UNREACHABLE;
        }
    }
}

void ShardSplitDonorOpObserver::onUpdate(OperationContext* opCtx,
                                         const OplogUpdateEntryArgs& args) {
    if (args.nss != NamespaceString::kTenantSplitDonorsNamespace ||
        tenant_migration_access_blocker::inRecoveryMode(opCtx)) {
        return;
    }

    auto donorStateDoc = parseAndValidateDonorDocument(args.updateArgs->updatedDoc);
    switch (donorStateDoc.getState()) {
        case ShardSplitDonorStateEnum::kCommitted:
        case ShardSplitDonorStateEnum::kAborted:
            opCtx->recoveryUnit()->registerChange(
                std::make_unique<TenantMigrationDonorCommitOrAbortHandler>(opCtx, donorStateDoc));
            break;
        case ShardSplitDonorStateEnum::kBlocking:
            uasserted(ErrorCodes::IllegalOperation,
                      "The state document should be inserted as blocking and never transition to "
                      "blocking");
            break;
        default:
            MONGO_UNREACHABLE;
    }
}

void ShardSplitDonorOpObserver::aboutToDelete(OperationContext* opCtx,
                                              NamespaceString const& nss,
                                              const UUID& uuid,
                                              BSONObj const& doc) {
    if (nss != NamespaceString::kTenantSplitDonorsNamespace ||
        tenant_migration_access_blocker::inRecoveryMode(opCtx)) {
        return;
    }

    auto donorStateDoc = parseAndValidateDonorDocument(doc);

    uassert(ErrorCodes::IllegalOperation,
            str::stream() << "cannot delete a donor's state document " << doc
                          << " since it has not been marked as garbage collectable and is not a"
                          << " recipient garbage collectable.",
            donorStateDoc.getExpireAt() ||
                serverless::shouldRemoveStateDocumentOnRecipient(opCtx, donorStateDoc));

    if (donorStateDoc.getTenantIds()) {
        auto tenantIds = *donorStateDoc.getTenantIds();
        std::vector<std::string> result;
        result.reserve(tenantIds.size());
        for (const auto& tenantId : tenantIds) {
            result.emplace_back(tenantId.toString());
        }

        tenantIdsToDeleteDecoration(opCtx) = boost::make_optional(result);
    }
}

void ShardSplitDonorOpObserver::onDelete(OperationContext* opCtx,
                                         const NamespaceString& nss,
                                         const UUID& uuid,
                                         StmtId stmtId,
                                         const OplogDeleteEntryArgs& args) {
    if (nss != NamespaceString::kTenantSplitDonorsNamespace ||
        !tenantIdsToDeleteDecoration(opCtx) ||
        tenant_migration_access_blocker::inRecoveryMode(opCtx)) {
        return;
    }

    opCtx->recoveryUnit()->onCommit([opCtx](boost::optional<Timestamp>) {
        auto& registry = TenantMigrationAccessBlockerRegistry::get(opCtx->getServiceContext());
        for (auto& tenantId : *tenantIdsToDeleteDecoration(opCtx)) {
            registry.remove(tenantId, TenantMigrationAccessBlocker::BlockerType::kDonor);
        }
    });
}

repl::OpTime ShardSplitDonorOpObserver::onDropCollection(OperationContext* opCtx,
                                                         const NamespaceString& collectionName,
                                                         const UUID& uuid,
                                                         std::uint64_t numRecords,
                                                         const CollectionDropType dropType) {
    if (collectionName == NamespaceString::kTenantSplitDonorsNamespace) {
        opCtx->recoveryUnit()->onCommit([opCtx](boost::optional<Timestamp>) {
            TenantMigrationAccessBlockerRegistry::get(opCtx->getServiceContext())
                .removeAll(TenantMigrationAccessBlocker::BlockerType::kDonor);
        });
    }

    return {};
}

void ShardSplitDonorOpObserver::onMajorityCommitPointUpdate(ServiceContext* service,
                                                            const repl::OpTime& newCommitPoint) {
    TenantMigrationAccessBlockerRegistry::get(service).onMajorityCommitPointUpdate(newCommitPoint);
}

}  // namespace mongo
