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

#include <utility>

#include "mongo/db/repl/replication_coordinator_mock.h"
#include "mongo/db/repl/storage_interface_mock.h"
#include "mongo/db/repl/tenant_migration_access_blocker_util.h"
#include "mongo/db/serverless/shard_split_donor_op_observer.h"
#include "mongo/db/serverless/shard_split_state_machine_gen.h"
#include "mongo/db/serverless/shard_split_test_utils.h"
#include "mongo/db/service_context_d_test_fixture.h"
#include "mongo/dbtests/mock/mock_replica_set.h"

namespace mongo {
namespace {

void startBlockingReadsAfter(
    std::vector<std::shared_ptr<TenantMigrationDonorAccessBlocker>>& blockers,
    Timestamp blockingReads) {
    for (auto& blocker : blockers) {
        blocker->startBlockingReadsAfter(blockingReads);
    }
}

class ShardSplitDonorOpObserverTest : public ServiceContextMongoDTest {
public:
    void setUp() override {
        ServiceContextMongoDTest::setUp();

        auto service = getServiceContext();
        {
            auto opCtx = cc().makeOperationContext();
            repl::StorageInterface::set(service, std::make_unique<repl::StorageInterfaceMock>());

            // Set up ReplicationCoordinator and create oplog.
            auto coordinatorMock =
                std::make_unique<repl::ReplicationCoordinatorMock>(service, createReplSettings());
            _replicationCoordinatorMock = coordinatorMock.get();

            repl::ReplicationCoordinator::set(service, std::move(coordinatorMock));
            repl::createOplog(opCtx.get());

            // Ensure that we are primary.
            auto replCoord = repl::ReplicationCoordinator::get(opCtx.get());
            ASSERT_OK(replCoord->setFollowerMode(repl::MemberState::RS_PRIMARY));
        }

        _observer = std::make_unique<ShardSplitDonorOpObserver>();
        _opCtx = makeOperationContext();
        _oplogSlot = 0;
    }

    void tearDown() override {
        _observer.reset();
        _opCtx.reset();

        ServiceContextMongoDTest::tearDown();
    }

protected:
    void runInsertTestCase(
        ShardSplitDonorDocument stateDocument,
        const std::vector<std::string>& tenants,
        std::function<void(std::shared_ptr<TenantMigrationAccessBlocker>)> mtabVerifier) {

        std::vector<InsertStatement> inserts;
        inserts.emplace_back(_oplogSlot++, stateDocument.toBSON());

        WriteUnitOfWork wow(_opCtx.get());
        _observer->onInserts(_opCtx.get(), _nss, _uuid, inserts.begin(), inserts.end(), false);
        wow.commit();

        verifyAndRemoveMtab(tenants, mtabVerifier);
    }

    void runUpdateTestCase(
        ShardSplitDonorDocument stateDocument,
        const std::vector<std::string>& tenants,
        std::function<void(std::shared_ptr<TenantMigrationAccessBlocker>)> mtabVerifier) {

        // If there's an exception, aborting without removing the access blocker will trigger an
        // invariant. This creates a confusing error log in the test output.
        test::shard_split::ScopedTenantAccessBlocker scopedTenants(_tenantIds, _opCtx.get());

        CollectionUpdateArgs updateArgs;
        updateArgs.stmtIds = {};
        updateArgs.updatedDoc = stateDocument.toBSON();
        updateArgs.update =
            BSON("$set" << BSON(ShardSplitDonorDocument::kStateFieldName
                                << ShardSplitDonorState_serializer(stateDocument.getState())));
        updateArgs.criteria = BSON("_id" << stateDocument.getId());
        OplogUpdateEntryArgs update(&updateArgs, _nss, stateDocument.getId());

        WriteUnitOfWork wuow(_opCtx.get());
        _observer->onUpdate(_opCtx.get(), update);
        wuow.commit();

        verifyAndRemoveMtab(tenants, mtabVerifier);
        scopedTenants.dismiss();
    }

    std::vector<std::shared_ptr<TenantMigrationDonorAccessBlocker>>
    createBlockersAndStartBlockingWrites(const std::vector<std::string>& tenants,
                                         OperationContext* opCtx,
                                         const std::string& connectionStr) {
        auto uuid = UUID::gen();
        std::vector<std::shared_ptr<TenantMigrationDonorAccessBlocker>> blockers;
        for (const auto& tenant : tenants) {
            auto mtab = std::make_shared<TenantMigrationDonorAccessBlocker>(
                _opCtx->getServiceContext(),
                uuid,
                tenant,
                MigrationProtocolEnum::kMultitenantMigrations,
                _connectionStr);

            blockers.push_back(mtab);
            mtab->startBlockingWrites();
            TenantMigrationAccessBlockerRegistry::get(opCtx->getServiceContext()).add(tenant, mtab);
        }

        return blockers;
    }

    ShardSplitDonorDocument defaultStateDocument() const {
        return ShardSplitDonorDocument::parse(
            {"donor.document"},
            BSON("_id" << _uuid << "tenantIds" << _tenantIds << "recipientTagName"
                       << _recipientTagName << "recipientSetName" << _recipientSetName));
    }

protected:
    MockReplicaSet _replSet =
        MockReplicaSet("donorSet", 3, true /* hasPrimary */, true /* dollarPrefixHosts */);
    MockReplicaSet _recipientReplSet =
        MockReplicaSet("recipientSet", 3, true /* hasPrimary */, true /* dollarPrefixHosts */);
    const NamespaceString _nss = NamespaceString::kTenantSplitDonorsNamespace;
    std::vector<std::string> _tenantIds = {"tenant1", "tenantAB"};
    std::string _connectionStr = _replSet.getConnectionString();
    UUID _uuid = UUID::gen();
    std::string _recipientTagName{"$recipientNode"};
    std::string _recipientSetName{_replSet.getURI().getSetName()};

    std::unique_ptr<ShardSplitDonorOpObserver> _observer;
    std::shared_ptr<OperationContext> _opCtx;
    repl::ReplicationCoordinatorMock* _replicationCoordinatorMock;
    int _oplogSlot;

private:
    void verifyAndRemoveMtab(
        const std::vector<std::string>& tenants,
        const std::function<void(std::shared_ptr<TenantMigrationAccessBlocker>)>& mtabVerifier) {
        for (const auto& tenantId : tenants) {
            auto mtab = TenantMigrationAccessBlockerRegistry::get(_opCtx->getServiceContext())
                            .getTenantMigrationAccessBlockerForTenantId(
                                tenantId, TenantMigrationAccessBlocker::BlockerType::kDonor);
            mtabVerifier(mtab);
        }

        for (const auto& tenantId : tenants) {
            TenantMigrationAccessBlockerRegistry::get(_opCtx->getServiceContext())
                .remove(tenantId, TenantMigrationAccessBlocker::BlockerType::kDonor);
        }
    }
    // Creates a reasonable set of ReplSettings for most tests.  We need to be able to
    // override this to create a larger oplog.
    virtual repl::ReplSettings createReplSettings() {
        repl::ReplSettings settings;
        settings.setOplogSizeBytes(5 * 1024 * 1024);
        settings.setReplSetString("mySet/node1:12345");
        return settings;
    }
};

TEST_F(ShardSplitDonorOpObserverTest, InsertWrongType) {
    std::vector<InsertStatement> inserts1;
    inserts1.emplace_back(0,
                          BSON("_id" << 0 << "data"
                                     << "x"));
    inserts1.emplace_back(1,
                          BSON("_id" << 1 << "data"
                                     << "y"));
    ASSERT_THROWS_CODE(
        _observer->onInserts(_opCtx.get(), _nss, _uuid, inserts1.begin(), inserts1.end(), false),
        DBException,
        ErrorCodes::TypeMismatch);
}

TEST_F(ShardSplitDonorOpObserverTest, InitialInsertInvalidState) {
    std::vector<ShardSplitDonorStateEnum> states = {ShardSplitDonorStateEnum::kAborted,
                                                    ShardSplitDonorStateEnum::kBlocking,
                                                    ShardSplitDonorStateEnum::kUninitialized,
                                                    ShardSplitDonorStateEnum::kCommitted};

    for (auto state : states) {
        auto stateDocument = defaultStateDocument();
        stateDocument.setState(state);

        auto mtabVerifier = [](std::shared_ptr<TenantMigrationAccessBlocker>) {};

        ASSERT_THROWS(runInsertTestCase(stateDocument, _tenantIds, mtabVerifier), DBException);
    }
}

TEST_F(ShardSplitDonorOpObserverTest, InsertValidAbortedDocument) {
    auto stateDocument = defaultStateDocument();
    stateDocument.setState(ShardSplitDonorStateEnum::kAborted);

    Status status(ErrorCodes::CallbackCanceled, "Split has been aborted");
    BSONObjBuilder bob;
    status.serializeErrorToBSON(&bob);
    stateDocument.setAbortReason(bob.obj());

    stateDocument.setCommitOrAbortOpTime(repl::OpTime(Timestamp(1), 1));

    std::vector<InsertStatement> inserts;
    inserts.emplace_back(_oplogSlot++, stateDocument.toBSON());

    WriteUnitOfWork wow(_opCtx.get());
    _observer->onInserts(_opCtx.get(), _nss, _uuid, inserts.begin(), inserts.end(), false);
    wow.commit();

    for (const auto& tenant : _tenantIds) {
        ASSERT_FALSE(TenantMigrationAccessBlockerRegistry::get(_opCtx->getServiceContext())
                         .getTenantMigrationAccessBlockerForTenantId(
                             tenant, TenantMigrationAccessBlocker::BlockerType::kDonor));
    }
}

TEST_F(ShardSplitDonorOpObserverTest, InsertBlockingDocumentPrimary) {
    test::shard_split::reconfigToAddRecipientNodes(
        getServiceContext(), _recipientTagName, _replSet.getHosts(), _recipientReplSet.getHosts());

    createBlockersAndStartBlockingWrites(_tenantIds, _opCtx.get(), _connectionStr);

    auto stateDocument = defaultStateDocument();
    stateDocument.setState(ShardSplitDonorStateEnum::kBlocking);
    stateDocument.setBlockTimestamp(Timestamp(1, 1));

    auto mtabVerifier = [opCtx = _opCtx.get()](std::shared_ptr<TenantMigrationAccessBlocker> mtab) {
        ASSERT_TRUE(mtab);
        // The OpObserver does not set the mtab to blocking for primaries.
        ASSERT_EQ(mtab->checkIfCanWrite(Timestamp(1, 1)).code(),
                  ErrorCodes::TenantMigrationConflict);
        ASSERT_EQ(mtab->checkIfCanWrite(Timestamp(1, 3)).code(),
                  ErrorCodes::TenantMigrationConflict);
        ASSERT_OK(mtab->checkIfLinearizableReadWasAllowed(opCtx));
        ASSERT_EQ(mtab->checkIfCanBuildIndex().code(), ErrorCodes::TenantMigrationConflict);
    };

    runInsertTestCase(stateDocument, _tenantIds, mtabVerifier);
}

TEST_F(ShardSplitDonorOpObserverTest, InsertBlockingDocumentSecondary) {
    test::shard_split::reconfigToAddRecipientNodes(
        getServiceContext(), _recipientTagName, _replSet.getHosts(), _recipientReplSet.getHosts());

    // This indicates the instance is secondary for the OpObserver.
    repl::UnreplicatedWritesBlock setSecondary(_opCtx.get());

    auto stateDocument = defaultStateDocument();
    stateDocument.setState(ShardSplitDonorStateEnum::kBlocking);
    stateDocument.setBlockTimestamp(Timestamp(1, 1));

    auto mtabVerifier = [opCtx = _opCtx.get()](std::shared_ptr<TenantMigrationAccessBlocker> mtab) {
        ASSERT_TRUE(mtab);
        // The OpObserver does not set the mtab to blocking for primaries.
        ASSERT_EQ(mtab->checkIfCanWrite(Timestamp(1, 1)).code(),
                  ErrorCodes::TenantMigrationConflict);
        ASSERT_EQ(mtab->checkIfCanWrite(Timestamp(1, 3)).code(),
                  ErrorCodes::TenantMigrationConflict);
        ASSERT_OK(mtab->checkIfLinearizableReadWasAllowed(opCtx));
        ASSERT_EQ(mtab->checkIfCanBuildIndex().code(), ErrorCodes::TenantMigrationConflict);
    };

    runInsertTestCase(stateDocument, _tenantIds, mtabVerifier);
}


TEST_F(ShardSplitDonorOpObserverTest, TransitionToBlockingFail) {
    // This indicates the instance is secondary for the OpObserver.
    repl::UnreplicatedWritesBlock setSecondary(_opCtx.get());

    auto stateDocument = defaultStateDocument();
    stateDocument.setState(ShardSplitDonorStateEnum::kBlocking);
    stateDocument.setBlockTimestamp(Timestamp(1, 1));


    CollectionUpdateArgs updateArgs;
    updateArgs.stmtIds = {};
    updateArgs.updatedDoc = stateDocument.toBSON();
    updateArgs.update =
        BSON("$set" << BSON(ShardSplitDonorDocument::kStateFieldName
                            << ShardSplitDonorState_serializer(stateDocument.getState())));
    updateArgs.criteria = BSON("_id" << stateDocument.getId());
    OplogUpdateEntryArgs update(&updateArgs, _nss, stateDocument.getId());

    auto update_lambda = [&]() {
        WriteUnitOfWork wuow(_opCtx.get());
        _observer->onUpdate(_opCtx.get(), update);
        wuow.commit();
    };

    ASSERT_THROWS_CODE(update_lambda(), DBException, ErrorCodes::IllegalOperation);
}

TEST_F(ShardSplitDonorOpObserverTest, TransitionToCommit) {
    // Transition to commit needs a commitOpTime in the OpLog
    auto commitOpTime = mongo::repl::OpTime(Timestamp(1, 3), 2);
    _replicationCoordinatorMock->setCurrentCommittedSnapshotOpTime(commitOpTime);

    auto stateDocument = defaultStateDocument();
    stateDocument.setState(ShardSplitDonorStateEnum::kCommitted);
    stateDocument.setBlockTimestamp(Timestamp(1, 2));
    stateDocument.setCommitOrAbortOpTime(commitOpTime);

    auto blockers = createBlockersAndStartBlockingWrites(_tenantIds, _opCtx.get(), _connectionStr);
    startBlockingReadsAfter(blockers, Timestamp(1));

    auto mtabVerifier = [opCtx = _opCtx.get()](std::shared_ptr<TenantMigrationAccessBlocker> mtab) {
        ASSERT_TRUE(mtab);
        // For primary instance, the ShardSplitDonorService will set the mtab to blocking, not
        // the OpObserver
        ASSERT_EQ(mtab->checkIfCanWrite(Timestamp(1)).code(), ErrorCodes::TenantMigrationCommitted);
        ASSERT_EQ(mtab->checkIfCanWrite(Timestamp(3)).code(), ErrorCodes::TenantMigrationCommitted);
        ASSERT_EQ(mtab->checkIfLinearizableReadWasAllowed(opCtx),
                  ErrorCodes::TenantMigrationCommitted);
        ASSERT_EQ(mtab->checkIfCanBuildIndex().code(), ErrorCodes::TenantMigrationCommitted);
    };

    runUpdateTestCase(stateDocument, _tenantIds, mtabVerifier);
}

TEST_F(ShardSplitDonorOpObserverTest, TransitionToAbort) {
    // Transition to commit needs a commitOpTime in the OpLog
    auto commitOpTime = mongo::repl::OpTime(Timestamp(1, 3), 2);
    _replicationCoordinatorMock->setCurrentCommittedSnapshotOpTime(commitOpTime);

    Status status(ErrorCodes::CallbackCanceled, "Split has been aborted");
    BSONObjBuilder bob;
    status.serializeErrorToBSON(&bob);

    auto stateDocument = defaultStateDocument();
    stateDocument.setState(ShardSplitDonorStateEnum::kAborted);
    stateDocument.setBlockTimestamp(Timestamp(1, 2));
    stateDocument.setCommitOrAbortOpTime(commitOpTime);
    stateDocument.setAbortReason(bob.obj());

    auto blockers = createBlockersAndStartBlockingWrites(_tenantIds, _opCtx.get(), _connectionStr);
    startBlockingReadsAfter(blockers, Timestamp(1));

    auto mtabVerifier = [opCtx = _opCtx.get()](std::shared_ptr<TenantMigrationAccessBlocker> mtab) {
        ASSERT_TRUE(mtab);
        // For primary instance, the ShardSplitDonorService will set the mtab to blocking, not
        // the OpObserver
        ASSERT_OK(mtab->checkIfCanWrite(Timestamp(1)).code());
        ASSERT_OK(mtab->checkIfCanWrite(Timestamp(3)).code());
        ASSERT_OK(mtab->checkIfLinearizableReadWasAllowed(opCtx));
        ASSERT_OK(mtab->checkIfCanBuildIndex().code());
    };

    runUpdateTestCase(stateDocument, _tenantIds, mtabVerifier);
}

}  // namespace
}  // namespace mongo
