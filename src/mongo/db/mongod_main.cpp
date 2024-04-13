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

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kControl

#include "mongo/platform/basic.h"

#include "mongo/db/mongod_main.h"

#include <boost/filesystem/operations.hpp>
#include <boost/optional.hpp>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <signal.h>
#include <string>

#include "mongo/base/init.h"
#include "mongo/base/initializer.h"
#include "mongo/base/status.h"
#include "mongo/client/global_conn_pool.h"
#include "mongo/client/replica_set_monitor.h"
#include "mongo/config.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/auth_op_observer.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/sasl_options.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/collection_catalog.h"
#include "mongo/db/catalog/collection_impl.h"
#include "mongo/db/catalog/create_collection.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/catalog/database_holder_impl.h"
#include "mongo/db/catalog/health_log.h"
#include "mongo/db/catalog/index_catalog.h"
#include "mongo/db/catalog/index_key_validate.h"
#include "mongo/db/change_stream_options_manager.h"
#include "mongo/db/client.h"
#include "mongo/db/client_metadata_propagation_egress_hook.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/commands/feature_compatibility_version.h"
#include "mongo/db/commands/feature_compatibility_version_gen.h"
#include "mongo/db/commands/shutdown.h"
#include "mongo/db/commands/test_commands.h"
#include "mongo/db/commands/test_commands_enabled.h"
#include "mongo/db/concurrency/d_concurrency.h"
#include "mongo/db/concurrency/flow_control_ticketholder.h"
#include "mongo/db/concurrency/lock_state.h"
#include "mongo/db/concurrency/replication_state_transition_lock_guard.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/dbmessage.h"
#include "mongo/db/exec/working_set_common.h"
#include "mongo/db/fcv_op_observer.h"
#include "mongo/db/fle_crud.h"
#include "mongo/db/ftdc/ftdc_mongod.h"
#include "mongo/db/ftdc/util.h"
#include "mongo/db/global_settings.h"
#include "mongo/db/index_builds_coordinator_mongod.h"
#include "mongo/db/index_names.h"
#include "mongo/db/initialize_server_global_state.h"
#include "mongo/db/initialize_snmp.h"
#include "mongo/db/internal_transactions_reap_service.h"
#include "mongo/db/introspect.h"
#include "mongo/db/json.h"
#include "mongo/db/keys_collection_client_direct.h"
#include "mongo/db/keys_collection_client_sharded.h"
#include "mongo/db/keys_collection_manager.h"
#include "mongo/db/kill_sessions.h"
#include "mongo/db/kill_sessions_local.h"
#include "mongo/db/log_process_details.h"
#include "mongo/db/logical_session_cache.h"
#include "mongo/db/logical_session_cache_factory_mongod.h"
#include "mongo/db/logical_time_validator.h"
#include "mongo/db/mirror_maestro.h"
#include "mongo/db/mongod_options.h"
#include "mongo/db/mongod_options_storage_gen.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/op_observer_impl.h"
#include "mongo/db/op_observer_registry.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/periodic_runner_job_abort_expired_transactions.h"
#include "mongo/db/pipeline/change_stream_expired_pre_image_remover.h"
#include "mongo/db/pipeline/process_interface/replica_set_node_process_interface.h"
#include "mongo/db/query/internal_plans.h"
#include "mongo/db/read_write_concern_defaults_cache_lookup_mongod.h"
#include "mongo/db/repl/drop_pending_collection_reaper.h"
#include "mongo/db/repl/initial_syncer_factory.h"
#include "mongo/db/repl/oplog.h"
#include "mongo/db/repl/primary_only_service.h"
#include "mongo/db/repl/primary_only_service_op_observer.h"
#include "mongo/db/repl/repl_settings.h"
#include "mongo/db/repl/replica_set_aware_service.h"
#include "mongo/db/repl/replication_consistency_markers_impl.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/repl/replication_coordinator_external_state_impl.h"
#include "mongo/db/repl/replication_coordinator_impl.h"
#include "mongo/db/repl/replication_coordinator_impl_gen.h"
#include "mongo/db/repl/replication_process.h"
#include "mongo/db/repl/replication_recovery.h"
#include "mongo/db/repl/storage_interface_impl.h"
#include "mongo/db/repl/tenant_migration_donor_op_observer.h"
#include "mongo/db/repl/tenant_migration_donor_service.h"
#include "mongo/db/repl/tenant_migration_recipient_op_observer.h"
#include "mongo/db/repl/tenant_migration_recipient_service.h"
#include "mongo/db/repl/topology_coordinator.h"
#include "mongo/db/repl/wait_for_majority_service.h"
#include "mongo/db/repl_set_member_in_standalone_mode.h"
#include "mongo/db/s/collection_sharding_state_factory_shard.h"
#include "mongo/db/s/collection_sharding_state_factory_standalone.h"
#include "mongo/db/s/config/configsvr_coordinator_service.h"
#include "mongo/db/s/config/sharding_catalog_manager.h"
#include "mongo/db/s/config_server_op_observer.h"
#include "mongo/db/s/migration_util.h"
#include "mongo/db/s/op_observer_sharding_impl.h"
#include "mongo/db/s/periodic_sharded_index_consistency_checker.h"
#include "mongo/db/s/rename_collection_participant_service.h"
#include "mongo/db/s/resharding/resharding_coordinator_service.h"
#include "mongo/db/s/resharding/resharding_donor_service.h"
#include "mongo/db/s/resharding/resharding_op_observer.h"
#include "mongo/db/s/resharding/resharding_recipient_service.h"
#include "mongo/db/s/shard_server_op_observer.h"
#include "mongo/db/s/sharding_ddl_coordinator_service.h"
#include "mongo/db/s/sharding_initialization_mongod.h"
#include "mongo/db/s/sharding_state_recovery.h"
#include "mongo/db/s/transaction_coordinator_service.h"
#include "mongo/db/server_options.h"
#include "mongo/db/serverless/shard_split_donor_op_observer.h"
#include "mongo/db/serverless/shard_split_donor_service.h"
#include "mongo/db/service_context.h"
#include "mongo/db/service_entry_point_mongod.h"
#include "mongo/db/session_catalog.h"
#include "mongo/db/session_killer.h"
#include "mongo/db/startup_recovery.h"
#include "mongo/db/startup_warnings_mongod.h"
#include "mongo/db/stats/counters.h"
#include "mongo/db/storage/backup_cursor_hooks.h"
#include "mongo/db/storage/control/storage_control.h"
#include "mongo/db/storage/durable_history_pin.h"
#include "mongo/db/storage/encryption_hooks.h"
#include "mongo/db/storage/flow_control.h"
#include "mongo/db/storage/flow_control_parameters_gen.h"
#include "mongo/db/storage/storage_engine.h"
#include "mongo/db/storage/storage_engine_init.h"
#include "mongo/db/storage/storage_engine_lock_file.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/db/storage/storage_parameters_gen.h"
#include "mongo/db/system_index.h"
#include "mongo/db/transaction_participant.h"
#include "mongo/db/ttl.h"
#include "mongo/db/user_write_block_mode_op_observer.h"
#include "mongo/db/vector_clock_metadata_hook.h"
#include "mongo/db/wire_version.h"
#include "mongo/executor/network_connection_hook.h"
#include "mongo/executor/network_interface_factory.h"
#include "mongo/executor/network_interface_thread_pool.h"
#include "mongo/executor/thread_pool_task_executor.h"
#include "mongo/idl/cluster_server_parameter_gen.h"
#include "mongo/idl/cluster_server_parameter_op_observer.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/process_id.h"
#include "mongo/platform/random.h"
#include "mongo/rpc/metadata/egress_metadata_hook_list.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/grid.h"
#include "mongo/scripting/dbdirectclient_factory.h"
#include "mongo/scripting/engine.h"
#include "mongo/stdx/future.h"
#include "mongo/stdx/thread.h"
#include "mongo/transport/transport_layer_manager.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/background.h"
#include "mongo/util/cmdline_utils/censor_cmdline.h"
#include "mongo/util/concurrency/idle_thread_block.h"
#include "mongo/util/concurrency/thread_name.h"
#include "mongo/util/exception_filter_win32.h"
#include "mongo/util/exit.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/fast_clock_source_factory.h"
#include "mongo/util/latch_analyzer.h"
#include "mongo/util/net/ocsp/ocsp_manager.h"
#include "mongo/util/net/private/ssl_expiration.h"
#include "mongo/util/net/socket_utils.h"
#include "mongo/util/net/ssl_manager.h"
#include "mongo/util/ntservice.h"
#include "mongo/util/options_parser/startup_options.h"
#include "mongo/util/periodic_runner.h"
#include "mongo/util/periodic_runner_factory.h"
#include "mongo/util/quick_exit.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/sequence_util.h"
#include "mongo/util/signal_handlers.h"
#include "mongo/util/stacktrace.h"
#include "mongo/util/text.h"
#include "mongo/util/time_support.h"
#include "mongo/util/version.h"
#include "mongo/watchdog/watchdog_mongod.h"

#ifdef MONGO_CONFIG_SSL
#include "mongo/util/net/ssl_options.h"
#endif

#if !defined(_WIN32)
#include <sys/file.h>
#endif

#ifdef __linux__
#include <sys/prctl.h>
#endif

namespace mongo {

using logv2::LogComponent;
using std::endl;

namespace {

MONGO_FAIL_POINT_DEFINE(hangDuringQuiesceMode);
MONGO_FAIL_POINT_DEFINE(pauseWhileKillingOperationsAtShutdown);
MONGO_FAIL_POINT_DEFINE(hangBeforeShutdown);

const NamespaceString startupLogCollectionName("local.startup_log");

#ifdef _WIN32
const ntservice::NtServiceDefaultStrings defaultServiceStrings = {
    L"MongoDB", L"MongoDB", L"MongoDB Server"};
#endif

void logStartup(OperationContext* opCtx) {
    BSONObjBuilder toLog;
    std::stringstream id;
    id << getHostNameCached() << "-" << jsTime().asInt64();
    toLog.append("_id", id.str());
    toLog.append("hostname", getHostNameCached());

    toLog.appendTimeT("startTime", time(nullptr));
    toLog.append("startTimeLocal", dateToCtimeString(Date_t::now()));

    toLog.append("cmdLine", serverGlobalParams.parsedOpts);
    toLog.append("pid", ProcessId::getCurrent().asLongLong());


    BSONObjBuilder buildinfo(toLog.subobjStart("buildinfo"));
    VersionInfoInterface::instance().appendBuildInfo(&buildinfo);
    appendStorageEngineList(opCtx->getServiceContext(), &buildinfo);
    buildinfo.doneFast();

    BSONObj o = toLog.obj();

    Lock::GlobalWrite lk(opCtx);
    AutoGetDb autoDb(opCtx, startupLogCollectionName.db(), mongo::MODE_X);
    auto db = autoDb.ensureDbExists(opCtx);
    CollectionPtr collection =
        CollectionCatalog::get(opCtx)->lookupCollectionByNamespace(opCtx, startupLogCollectionName);
    WriteUnitOfWork wunit(opCtx);
    if (!collection) {
        BSONObj options = BSON("capped" << true << "size" << 10 * 1024 * 1024);
        repl::UnreplicatedWritesBlock uwb(opCtx);
        CollectionOptions collectionOptions = uassertStatusOK(
            CollectionOptions::parse(options, CollectionOptions::ParseKind::parseForCommand));
        uassertStatusOK(db->userCreateNS(opCtx, startupLogCollectionName, collectionOptions));
        collection = CollectionCatalog::get(opCtx)->lookupCollectionByNamespace(
            opCtx, startupLogCollectionName);
    }
    invariant(collection);

    OpDebug* const nullOpDebug = nullptr;
    uassertStatusOK(collection->insertDocument(opCtx, InsertStatement(o), nullOpDebug, false));
    wunit.commit();
}

MONGO_INITIALIZER_WITH_PREREQUISITES(WireSpec, ("EndStartupOptionHandling"))(InitializerContext*) {
    // The featureCompatibilityVersion behavior defaults to the downgrade behavior while the
    // in-memory version is unset.
    WireSpec::Specification spec;
    spec.incomingInternalClient.minWireVersion = RELEASE_2_4_AND_BEFORE;
    spec.incomingInternalClient.maxWireVersion = LATEST_WIRE_VERSION;
    spec.outgoing.minWireVersion = SUPPORTS_OP_MSG;
    spec.outgoing.maxWireVersion = LATEST_WIRE_VERSION;
    spec.isInternalClient = true;

    WireSpec::instance().initialize(std::move(spec));
}

void initializeCommandHooks(ServiceContext* serviceContext) {
    class MongodCommandInvocationHooks final : public CommandInvocationHooks {
        void onBeforeRun(OperationContext*, const OpMsgRequest&, CommandInvocation*) {}

        void onAfterRun(OperationContext* opCtx, const OpMsgRequest&, CommandInvocation*) {
            MirrorMaestro::tryMirrorRequest(opCtx);
        }
    };

    MirrorMaestro::init(serviceContext);
    CommandInvocationHooks::set(serviceContext, std::make_unique<MongodCommandInvocationHooks>());
}

void registerPrimaryOnlyServices(ServiceContext* serviceContext) {
    auto registry = repl::PrimaryOnlyServiceRegistry::get(serviceContext);

    std::vector<std::unique_ptr<repl::PrimaryOnlyService>> services;

    if (serverGlobalParams.clusterRole == ClusterRole::ConfigServer) {
        services.push_back(std::make_unique<ReshardingCoordinatorService>(serviceContext));
        services.push_back(std::make_unique<ConfigsvrCoordinatorService>(serviceContext));
    } else if (serverGlobalParams.clusterRole == ClusterRole::ShardServer) {
        services.push_back(std::make_unique<RenameCollectionParticipantService>(serviceContext));
        services.push_back(std::make_unique<ShardingDDLCoordinatorService>(serviceContext));
        services.push_back(std::make_unique<ReshardingDonorService>(serviceContext));
        services.push_back(std::make_unique<ReshardingRecipientService>(serviceContext));
        services.push_back(std::make_unique<TenantMigrationDonorService>(serviceContext));
        services.push_back(std::make_unique<repl::TenantMigrationRecipientService>(serviceContext));
    } else {
        services.push_back(std::make_unique<TenantMigrationDonorService>(serviceContext));
        services.push_back(std::make_unique<repl::TenantMigrationRecipientService>(serviceContext));
        services.push_back(std::make_unique<ShardSplitDonorService>(serviceContext));
    }

    for (auto& service : services) {
        registry->registerService(std::move(service));
    }
}

MONGO_FAIL_POINT_DEFINE(shutdownAtStartup);

void logMongodStartupTimeElapsedStatistics(ServiceContext* serviceContext,
                                           Date_t beginInitAndListen,
                                           BSONObjBuilder* startupTimeElapsedBuilder,
                                           BSONObjBuilder* startupInfoBuilder,
                                           StorageEngine::LastShutdownState lastShutdownState) {
    mongo::Milliseconds elapsedInitAndListen =
        serviceContext->getFastClockSource()->now() - beginInitAndListen;
    startupTimeElapsedBuilder->append("_initAndListen total elapsed time",
                                      elapsedInitAndListen.toString());
    startupInfoBuilder->append("Startup from clean shutdown?",
                               lastShutdownState == StorageEngine::LastShutdownState::kClean);
    startupInfoBuilder->append("Statistics", startupTimeElapsedBuilder->obj());
    LOGV2_INFO(8423403,
               "mongod startup complete",
               "Summary of time elapsed"_attr = startupInfoBuilder->obj());
}

// Important:
// _initAndListen among its other tasks initializes the storage subsystem.
// File Copy Based Initial Sync will restart the storage subsystem and may need to repeat some
// of the initialization steps within.  If you add or change any of these steps, make sure
// any necessary changes are also made to File Copy Based Initial Sync.
ExitCode _initAndListen(ServiceContext* serviceContext, int listenPort) {
    Client::initThread("initandlisten");

    serviceContext->setFastClockSource(FastClockSourceFactory::create(Milliseconds(10)));

    BSONObjBuilder startupTimeElapsedBuilder;
    BSONObjBuilder startupInfoBuilder;

    Date_t beginInitAndListen = serviceContext->getFastClockSource()->now();

    DBDirectClientFactory::get(serviceContext).registerImplementation([](OperationContext* opCtx) {
        return std::unique_ptr<DBClientBase>(new DBDirectClient(opCtx));
    });

    const repl::ReplSettings& replSettings =
        repl::ReplicationCoordinator::get(serviceContext)->getSettings();

    {
        ProcessId pid = ProcessId::getCurrent();
        const bool is32bit = sizeof(int*) == 4;
        LOGV2(4615611,
              "MongoDB starting : pid={pid} port={port} dbpath={dbPath} {architecture} host={host}",
              "MongoDB starting",
              "pid"_attr = pid.toNative(),
              "port"_attr = serverGlobalParams.port,
              "dbPath"_attr = boost::filesystem::path(storageGlobalParams.dbpath).generic_string(),
              "architecture"_attr = (is32bit ? "32-bit" : "64-bit"),
              "host"_attr = getHostNameCached());
    }

    if (kDebugBuild)
        LOGV2(20533, "DEBUG build (which is slower)");

#if defined(_WIN32)
    VersionInfoInterface::instance().logTargetMinOS();
#endif

    logProcessDetails(nullptr);
    audit::logStartupOptions(Client::getCurrent(), serverGlobalParams.parsedOpts);

    serviceContext->setServiceEntryPoint(std::make_unique<ServiceEntryPointMongod>(serviceContext));

    // Set up the periodic runner for background job execution. This is required to be running
    // before both the storage engine or the transport layer are initialized.
    auto runner = makePeriodicRunner(serviceContext);
    serviceContext->setPeriodicRunner(std::move(runner));

#ifdef MONGO_CONFIG_SSL
    OCSPManager::start(serviceContext);
    CertificateExpirationMonitor::get()->start(serviceContext);
#endif

    if (!storageGlobalParams.repair) {
        TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                  "Transport layer setup",
                                                  &startupTimeElapsedBuilder);
        auto tl =
            transport::TransportLayerManager::createWithConfig(&serverGlobalParams, serviceContext);
        auto res = tl->setup();
        if (!res.isOK()) {
            LOGV2_ERROR(20568,
                        "Error setting up listener: {error}",
                        "Error setting up listener",
                        "error"_attr = res);
            return EXIT_NET_ERROR;
        }
        serviceContext->setTransportLayer(std::move(tl));
    }

    FlowControl::set(serviceContext,
                     std::make_unique<FlowControl>(
                         serviceContext, repl::ReplicationCoordinator::get(serviceContext)));

    // If a crash occurred during file-copy based initial sync, we may need to finish or clean up.
    {
        TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                  "Run initial syncer crash recovery",
                                                  &startupTimeElapsedBuilder);
        repl::InitialSyncerFactory::get(serviceContext)->runCrashRecovery();
    }

    // Creating the operation context before initializing the storage engine allows the storage
    // engine initialization to make use of the lock manager. As the storage engine is not yet
    // initialized, a noop recovery unit is used until the initialization is complete.
    auto startupOpCtx = serviceContext->makeOperationContext(&cc());

    auto lastShutdownState = initializeStorageEngine(
        startupOpCtx.get(), StorageEngineInitFlags{}, &startupTimeElapsedBuilder);
    StorageControl::startStorageControls(serviceContext);

    ScopeGuard logStartupStats([serviceContext,
                                beginInitAndListen,
                                &startupTimeElapsedBuilder,
                                &startupInfoBuilder,
                                lastShutdownState] {
        logMongodStartupTimeElapsedStatistics(serviceContext,
                                              beginInitAndListen,
                                              &startupTimeElapsedBuilder,
                                              &startupInfoBuilder,
                                              lastShutdownState);
    });

#ifdef MONGO_CONFIG_WIREDTIGER_ENABLED
    if (EncryptionHooks::get(serviceContext)->restartRequired()) {
        exitCleanly(EXIT_CLEAN);
    }
#endif

    // Warn if we detect configurations for multiple registered storage engines in the same
    // configuration file/environment.
    if (serverGlobalParams.parsedOpts.hasField("storage")) {
        BSONElement storageElement = serverGlobalParams.parsedOpts.getField("storage");
        invariant(storageElement.isABSONObj());
        for (auto&& e : storageElement.Obj()) {
            // Ignore if field name under "storage" matches current storage engine.
            if (storageGlobalParams.engine == e.fieldName()) {
                continue;
            }

            // Warn if field name matches non-active registered storage engine.
            if (isRegisteredStorageEngine(serviceContext, e.fieldName())) {
                LOGV2_WARNING(20566,
                              "Detected configuration for non-active storage engine {fieldName} "
                              "when current storage engine is {storageEngine}",
                              "Detected configuration for non-active storage engine",
                              "fieldName"_attr = e.fieldName(),
                              "storageEngine"_attr = storageGlobalParams.engine);
            }
        }
    }

    // Disallow running a storage engine that doesn't support capped collections with --profile
    if (!serviceContext->getStorageEngine()->supportsCappedCollections() &&
        serverGlobalParams.defaultProfile != 0) {
        LOGV2_ERROR(20534,
                    "Running {storageEngine} with profiling is not supported. Make sure you "
                    "are not using --profile",
                    "Running the selected storage engine with profiling is not supported",
                    "storageEngine"_attr = storageGlobalParams.engine);
        exitCleanly(EXIT_BADOPTIONS);
    }

    // Disallow running WiredTiger with --nojournal in a replica set
    if (storageGlobalParams.engine == "wiredTiger" && !storageGlobalParams.dur &&
        replSettings.usingReplSets()) {
        LOGV2_ERROR(
            20535,
            "Running wiredTiger without journaling in a replica set is not supported. Make sure "
            "you are not using --nojournal and that storage.journal.enabled is not set to "
            "'false'");
        exitCleanly(EXIT_BADOPTIONS);
    }

    if (storageGlobalParams.repair && replSettings.usingReplSets()) {
        LOGV2_ERROR(5019200,
                    "Cannot specify both repair and replSet at the same time (remove --replSet to "
                    "be able to --repair)");
        exitCleanly(EXIT_BADOPTIONS);
    }

    if (gAllowDocumentsGreaterThanMaxUserSize && replSettings.usingReplSets()) {
        LOGV2_ERROR(8472200,
                    "allowDocumentsGreaterThanMaxUserSize can only be used in standalone mode");
        exitCleanly(EXIT_BADOPTIONS);
    }

    logMongodStartupWarnings(storageGlobalParams, serverGlobalParams, serviceContext);

    {
        std::stringstream ss;
        ss << endl;
        ss << "*********************************************************************" << endl;
        ss << " ERROR: dbpath (" << storageGlobalParams.dbpath << ") does not exist." << endl;
        ss << " Create this directory or give existing directory in --dbpath." << endl;
        ss << " See http://dochub.mongodb.org/core/startingandstoppingmongo" << endl;
        ss << "*********************************************************************" << endl;
        uassert(10296, ss.str().c_str(), boost::filesystem::exists(storageGlobalParams.dbpath));
    }

    startWatchdog(serviceContext);

    try {
        startup_recovery::repairAndRecoverDatabases(
            startupOpCtx.get(), lastShutdownState, &startupTimeElapsedBuilder);
    } catch (const ExceptionFor<ErrorCodes::MustDowngrade>& error) {
        LOGV2_FATAL_OPTIONS(
            20573,
            logv2::LogOptions(logv2::LogComponent::kControl, logv2::FatalMode::kContinue),
            "** IMPORTANT: {error}",
            "Wrong mongod version",
            "error"_attr = error.toStatus().reason());
        exitCleanly(EXIT_NEED_DOWNGRADE);
    }

    // Ensure FCV document exists and is initialized in-memory. Fatally asserts if there is an
    // error.
    FeatureCompatibilityVersion::fassertInitializedAfterStartup(startupOpCtx.get());

    if (gFlowControlEnabled.load()) {
        LOGV2(20536, "Flow Control is enabled on this deployment");
    }

    {
        Lock::GlobalWrite globalLk(startupOpCtx.get());
        DurableHistoryRegistry::get(serviceContext)->reconcilePins(startupOpCtx.get());
    }

    // Notify the storage engine that startup is completed before repair exits below, as repair sets
    // the upgrade flag to true.
    auto storageEngine = serviceContext->getStorageEngine();
    invariant(storageEngine);
    storageEngine->notifyStartupComplete();

    BackupCursorHooks::initialize(serviceContext);

    startMongoDFTDC();

    initializeSNMP();

    if (mongodGlobalParams.scriptingEnabled) {
        ScriptEngine::setup();
    }

    if (storageGlobalParams.upgrade) {
        LOGV2(20537, "Finished checking dbs");
        exitCleanly(EXIT_CLEAN);
    }

    // Start up health log writer thread.
    HealthLogInterface::set(serviceContext, std::make_unique<HealthLog>());
    HealthLogInterface::get(startupOpCtx.get())->startup();

    auto const globalAuthzManager = AuthorizationManager::get(serviceContext);
    {
        TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                  "Build user and roles graph",
                                                  &startupTimeElapsedBuilder);
        uassertStatusOK(globalAuthzManager->initialize(startupOpCtx.get()));
    }

    if (audit::initializeManager) {
        audit::initializeManager(startupOpCtx.get());
    }

    // This is for security on certain platforms (nonce generation)
    srand((unsigned)(curTimeMicros64()) ^ (unsigned(uintptr_t(&startupOpCtx))));

    if (globalAuthzManager->shouldValidateAuthSchemaOnStartup()) {
        Status status = verifySystemIndexes(startupOpCtx.get(), &startupTimeElapsedBuilder);
        if (!status.isOK()) {
            LOGV2_WARNING(20538,
                          "Unable to verify system indexes: {error}",
                          "Unable to verify system indexes",
                          "error"_attr = redact(status));
            if (status == ErrorCodes::AuthSchemaIncompatible) {
                exitCleanly(EXIT_NEED_UPGRADE);
            } else if (status == ErrorCodes::NotWritablePrimary) {
                // Try creating the indexes if we become primary.  If we do not become primary,
                // the master will create the indexes and we will replicate them.
            } else {
                quickExit(EXIT_FAILURE);
            }
        }

        // SERVER-14090: Verify that auth schema version is schemaVersion26Final.
        int foundSchemaVersion;
        status =
            globalAuthzManager->getAuthorizationVersion(startupOpCtx.get(), &foundSchemaVersion);
        if (!status.isOK()) {
            LOGV2_ERROR(
                20539,
                "Auth schema version is incompatible: User and role management commands require "
                "auth data to have at least schema version {minSchemaVersion} but startup could "
                "not verify schema version: {error}",
                "Failed to verify auth schema version",
                "minSchemaVersion"_attr = AuthorizationManager::schemaVersion26Final,
                "error"_attr = status);
            LOGV2(20540,
                  "To manually repair the 'authSchema' document in the admin.system.version "
                  "collection, start up with --setParameter "
                  "startupAuthSchemaValidation=false to disable validation");
            exitCleanly(EXIT_NEED_UPGRADE);
        }

        if (foundSchemaVersion <= AuthorizationManager::schemaVersion26Final) {
            LOGV2_ERROR(
                20541,
                "This server is using MONGODB-CR, an authentication mechanism which has been "
                "removed from MongoDB 4.0. In order to upgrade the auth schema, first downgrade "
                "MongoDB binaries to version 3.6 and then run the authSchemaUpgrade command. See "
                "http://dochub.mongodb.org/core/3.0-upgrade-to-scram-sha-1");
            exitCleanly(EXIT_NEED_UPGRADE);
        }
    } else if (globalAuthzManager->isAuthEnabled()) {
        LOGV2_ERROR(20569, "Auth must be disabled when starting without auth schema validation");
        exitCleanly(EXIT_BADOPTIONS);
    } else {
        // If authSchemaValidation is disabled and server is running without auth,
        // warn the user and continue startup without authSchema metadata checks.
        LOGV2_WARNING_OPTIONS(
            20543,
            {logv2::LogTag::kStartupWarnings},
            "** WARNING: Startup auth schema validation checks are disabled for the database");
        LOGV2_WARNING_OPTIONS(
            20544,
            {logv2::LogTag::kStartupWarnings},
            "**          This mode should only be used to manually repair corrupted auth data");
    }

    {
        TimeElapsedBuilderScopedTimer scopedTimer(
            serviceContext->getFastClockSource(),
            "Set up the background thread pool responsible for "
            "waiting for opTimes to be majority committed",
            &startupTimeElapsedBuilder);
        WaitForMajorityService::get(serviceContext).startup(serviceContext);
    }

    // This function may take the global lock.
    auto shardingInitialized = ShardingInitializationMongoD::get(startupOpCtx.get())
                                   ->initializeShardingAwarenessIfNeeded(startupOpCtx.get());
    if (shardingInitialized) {
        auto status = waitForShardRegistryReload(startupOpCtx.get());
        if (!status.isOK()) {
            LOGV2(20545,
                  "Error loading shard registry at startup {error}",
                  "Error loading shard registry at startup",
                  "error"_attr = redact(status));
        }
    }

    try {
        if (serverGlobalParams.clusterRole != ClusterRole::ShardServer &&
            replSettings.usingReplSets()) {
            ReadWriteConcernDefaults::get(startupOpCtx.get()->getServiceContext())
                .refreshIfNecessary(startupOpCtx.get());
        }
    } catch (const DBException& ex) {
        LOGV2_WARNING(20567,
                      "Error loading read and write concern defaults at startup",
                      "error"_attr = redact(ex));
    }
    readWriteConcernDefaultsMongodStartupChecks(startupOpCtx.get(), replSettings.usingReplSets());

    // Perform replication recovery for queryable backup mode if needed.
    if (storageGlobalParams.readOnly) {
        uassert(ErrorCodes::BadValue,
                str::stream() << "Cannot specify both queryableBackupMode and "
                              << "recoverFromOplogAsStandalone at the same time",
                !replSettings.shouldRecoverFromOplogAsStandalone());
        uassert(
            ErrorCodes::BadValue,
            str::stream()
                << "Cannot take an unstable checkpoint on shutdown while using queryableBackupMode",
            !gTakeUnstableCheckpointOnShutdown);
        uassert(5576603,
                str::stream() << "Cannot specify both queryableBackupMode and "
                              << "startupRecoveryForRestore at the same time",
                !repl::startupRecoveryForRestore);

        auto replCoord = repl::ReplicationCoordinator::get(startupOpCtx.get());
        invariant(replCoord);
        uassert(ErrorCodes::BadValue,
                str::stream() << "Cannot use queryableBackupMode in a replica set",
                !replCoord->isReplEnabled());
        TimeElapsedBuilderScopedTimer scopedTimer(
            serviceContext->getFastClockSource(),
            "Start up the replication coordinator for queryable backup mode",
            &startupTimeElapsedBuilder);
        replCoord->startup(startupOpCtx.get(), lastShutdownState);
    }

    if (!storageGlobalParams.readOnly) {

        if (storageEngine->supportsCappedCollections()) {
            logStartup(startupOpCtx.get());
        }

        auto replCoord = repl::ReplicationCoordinator::get(startupOpCtx.get());
        invariant(replCoord);

        if (serverGlobalParams.clusterRole == ClusterRole::ShardServer) {
            // Note: For replica sets, ShardingStateRecovery happens on transition to primary.
            if (!replCoord->isReplEnabled()) {
                if (ShardingState::get(startupOpCtx.get())->enabled()) {
                    uassertStatusOK(ShardingStateRecovery::recover(startupOpCtx.get()));
                }
            }
        } else if (serverGlobalParams.clusterRole == ClusterRole::ConfigServer) {
            TimeElapsedBuilderScopedTimer scopedTimer(
                serviceContext->getFastClockSource(),
                "Initialize the sharding components for a config server",
                &startupTimeElapsedBuilder);

            initializeGlobalShardingStateForMongoD(
                startupOpCtx.get(), ShardId::kConfigServerId, ConnectionString::forLocal());

            ShardingCatalogManager::create(
                startupOpCtx->getServiceContext(),
                makeShardingTaskExecutor(executor::makeNetworkInterface("AddShard-TaskExecutor")));

            Grid::get(startupOpCtx.get())->setShardingInitialized();
        }

        if (replSettings.usingReplSets() &&
            (serverGlobalParams.clusterRole == ClusterRole::None ||
             !Grid::get(startupOpCtx.get())->isShardingInitialized())) {
            // If this is a mongod in a standalone replica set or a shardsvr replica set that has
            // not initialized its sharding identity, start up the cluster time keys manager with a
            // local/direct keys client. The keys client must use local read concern if the storage
            // engine can't support majority read concern. If this is a mongod in a configsvr or
            // shardsvr replica set that has initialized its sharding identity, the keys manager is
            // by design initialized separately with a sharded keys client when the sharding state
            // is initialized.
            TimeElapsedBuilderScopedTimer scopedTimer(
                serviceContext->getFastClockSource(),
                "Start up cluster time keys manager with a local/direct keys client",
                &startupTimeElapsedBuilder);
            auto keysCollectionClient = std::make_unique<KeysCollectionClientDirect>();
            auto keyManager = std::make_shared<KeysCollectionManager>(
                KeysCollectionManager::kKeyManagerPurposeString,
                std::move(keysCollectionClient),
                Seconds(KeysRotationIntervalSec));
            keyManager->startMonitoring(startupOpCtx->getServiceContext());

            LogicalTimeValidator::set(startupOpCtx->getServiceContext(),
                                      std::make_unique<LogicalTimeValidator>(keyManager));
        }

        if (replSettings.usingReplSets() && serverGlobalParams.clusterRole == ClusterRole::None) {
            ReplicaSetNodeProcessInterface::getReplicaSetNodeExecutor(serviceContext)->startup();
        }

        {
            TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                      "Start up the replication coordinator",
                                                      &startupTimeElapsedBuilder);
            replCoord->startup(startupOpCtx.get(), lastShutdownState);
        }

        // 'getOldestActiveTimestamp', which is called in the background by the checkpoint thread,
        // requires a read on 'config.transactions' at the stableTimestamp. If this read occurs
        // while applying prepared transactions at the end of replication recovery, it's possible to
        // prepare a transaction at timestamp earlier than the stableTimestamp. This will result in
        // a WiredTiger invariant. Register the callback after the call to 'startup' to ensure we've
        // finished applying prepared transactions.
        if (replCoord->isReplEnabled()) {
            storageEngine->setOldestActiveTransactionTimestampCallback(
                TransactionParticipant::getOldestActiveTimestamp);
        }

        if (getReplSetMemberInStandaloneMode(serviceContext)) {
            LOGV2_WARNING_OPTIONS(
                20547,
                {logv2::LogTag::kStartupWarnings},
                "Document(s) exist in 'system.replset', but started without --replSet. Database "
                "contents may appear inconsistent with the writes that were visible when this node "
                "was running as part of a replica set. Restart with --replSet unless you are doing "
                "maintenance and no other clients are connected. The TTL collection monitor will "
                "not start because of this. For more info see "
                "http://dochub.mongodb.org/core/ttlcollections");
        } else {
            startTTLMonitor(serviceContext);
        }

        if (replSettings.usingReplSets() || !gInternalValidateFeaturesAsPrimary) {
            serverGlobalParams.validateFeaturesAsPrimary.store(false);
        }

        if (replSettings.usingReplSets()) {
            TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                      "Create an oplog view for tenant migrations",
                                                      &startupTimeElapsedBuilder);
            Lock::GlobalWrite lk(startupOpCtx.get());
            OldClientContext ctx(startupOpCtx.get(), NamespaceString::kRsOplogNamespace.ns());
            tenant_migration_util::createOplogViewForTenantMigrations(startupOpCtx.get(), ctx.db());
        }

        storageEngine->startTimestampMonitor();

        startFLECrud(serviceContext);
    }

    startClientCursorMonitor();

    PeriodicTask::startRunningPeriodicTasks();

    SessionKiller::set(serviceContext,
                       std::make_shared<SessionKiller>(serviceContext, killSessionsLocal));

    // Start up a background task to periodically check for and kill expired transactions; and a
    // background task to periodically check for and decrease cache pressure by decreasing the
    // target size setting for the storage engine's window of available snapshots.
    //
    // Only do this on storage engines supporting snapshot reads, which hold resources we wish to
    // release periodically in order to avoid storage cache pressure build up.
    if (storageEngine->supportsReadConcernSnapshot()) {
        try {
            PeriodicThreadToAbortExpiredTransactions::get(serviceContext)->start();
        } catch (ExceptionFor<ErrorCodes::PeriodicJobIsStopped>&) {
            LOGV2_WARNING(4747501, "Not starting periodic jobs as shutdown is in progress");
            // Shutdown has already started before initialization is complete. Wait for the
            // shutdown task to complete and return.

            logStartupStats.dismiss();
            logMongodStartupTimeElapsedStatistics(serviceContext,
                                                  beginInitAndListen,
                                                  &startupTimeElapsedBuilder,
                                                  &startupInfoBuilder,
                                                  lastShutdownState);

            MONGO_IDLE_THREAD_BLOCK;
            return waitForShutdown();
        }
    }

    // Start a background task to periodically remove expired pre-images from the 'system.preimages'
    // collection if not in standalone mode.
    const auto isStandalone =
        repl::ReplicationCoordinator::get(serviceContext)->getReplicationMode() ==
        repl::ReplicationCoordinator::modeNone;
    if (!isStandalone) {
        startChangeStreamExpiredPreImagesRemover(serviceContext);
    }

    // Set up the logical session cache
    LogicalSessionCacheServer kind = LogicalSessionCacheServer::kStandalone;
    if (serverGlobalParams.clusterRole == ClusterRole::ShardServer) {
        kind = LogicalSessionCacheServer::kSharded;
    } else if (serverGlobalParams.clusterRole == ClusterRole::ConfigServer) {
        kind = LogicalSessionCacheServer::kConfigServer;
    } else if (replSettings.usingReplSets()) {
        kind = LogicalSessionCacheServer::kReplicaSet;
    }

    LogicalSessionCache::set(serviceContext, makeLogicalSessionCacheD(kind));

    initializeCommandHooks(serviceContext);

    // MessageServer::run will return when exit code closes its socket and we don't need the
    // operation context anymore
    startupOpCtx.reset();

    auto start = serviceContext->getServiceEntryPoint()->start();
    if (!start.isOK()) {
        LOGV2_ERROR(20571,
                    "Error starting service entry point: {error}",
                    "Error starting service entry point",
                    "error"_attr = start);
        return EXIT_NET_ERROR;
    }

    if (!storageGlobalParams.repair) {
        TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                  "Start transport layer",
                                                  &startupTimeElapsedBuilder);
        start = serviceContext->getTransportLayer()->start();
        if (!start.isOK()) {
            LOGV2_ERROR(20572,
                        "Error starting listener: {error}",
                        "Error starting listener",
                        "error"_attr = start);
            return EXIT_NET_ERROR;
        }
    }

    serviceContext->notifyStartupComplete();

#ifndef _WIN32
    mongo::signalForkSuccess();
#else
    if (ntservice::shouldStartService()) {
        ntservice::reportStatus(SERVICE_RUNNING);
        LOGV2(20555, "Service running");
    }
#endif

    if (MONGO_unlikely(shutdownAtStartup.shouldFail())) {
        LOGV2(20556, "Starting clean exit via failpoint");
        exitCleanly(EXIT_CLEAN);
    }

    logStartupStats.dismiss();
    logMongodStartupTimeElapsedStatistics(serviceContext,
                                          beginInitAndListen,
                                          &startupTimeElapsedBuilder,
                                          &startupInfoBuilder,
                                          lastShutdownState);

    MONGO_IDLE_THREAD_BLOCK;
    return waitForShutdown();
}

ExitCode initAndListen(ServiceContext* service, int listenPort) {
    try {
        return _initAndListen(service, listenPort);
    } catch (DBException& e) {
        LOGV2_ERROR(20557,
                    "Exception in initAndListen: {error}, terminating",
                    "DBException in initAndListen, terminating",
                    "error"_attr = e.toString());
        return EXIT_UNCAUGHT;
    } catch (std::exception& e) {
        LOGV2_ERROR(20558,
                    "Exception in initAndListen std::exception: {error}, terminating",
                    "std::exception in initAndListen, terminating",
                    "error"_attr = e.what());
        return EXIT_UNCAUGHT;
    } catch (int& n) {
        LOGV2_ERROR(20559,
                    "Exception in initAndListen int: {reason}, terminating",
                    "Exception in initAndListen, terminating",
                    "reason"_attr = n);
        return EXIT_UNCAUGHT;
    } catch (...) {
        LOGV2_ERROR(20560, "Exception in initAndListen, terminating");
        return EXIT_UNCAUGHT;
    }
}

#if defined(_WIN32)
ExitCode initService() {
    return initAndListen(getGlobalServiceContext(), serverGlobalParams.port);
}
#endif

MONGO_INITIALIZER_GENERAL(ForkServer, ("EndStartupOptionHandling"), ("default"))
(InitializerContext* context) {
    mongo::forkServerOrDie();
}

#ifdef __linux__
/**
 * Read the pid file from the dbpath for the process ID used by this instance of the server.
 * Use that process number to kill the running server.
 *
 * Equivalent to: `kill -SIGTERM $(cat $DBPATH/mongod.lock)`
 *
 * Performs additional checks to make sure the PID as read is reasonable (>= 1)
 * and can be found in the /proc filesystem.
 */
Status shutdownProcessByDBPathPidFile(const std::string& dbpath) {
    auto pidfile = (boost::filesystem::path(dbpath) / kLockFileBasename.toString()).string();
    if (!boost::filesystem::exists(pidfile)) {
        return {ErrorCodes::OperationFailed,
                str::stream() << "There doesn't seem to be a server running with dbpath: "
                              << dbpath};
    }

    pid_t pid;
    try {
        std::ifstream f(pidfile.c_str());
        f >> pid;
    } catch (const std::exception& ex) {
        return {ErrorCodes::OperationFailed,
                str::stream() << "Error reading pid from lock file [" << pidfile
                              << "]: " << ex.what()};
    }

    if (pid <= 0) {
        return {ErrorCodes::OperationFailed,
                str::stream() << "Invalid process ID '" << pid
                              << "' read from pidfile: " << pidfile};
    }

    std::string procPath = str::stream() << "/proc/" << pid;
    if (!boost::filesystem::exists(procPath)) {
        return {ErrorCodes::OperationFailed,
                str::stream() << "Process ID '" << pid << "' read from pidfile '" << pidfile
                              << "' does not appear to be running"};
    }

    std::cout << "Killing process with pid: " << pid << std::endl;
    int ret = kill(pid, SIGTERM);
    if (ret) {
        int e = errno;
        return {ErrorCodes::OperationFailed,
                str::stream() << "Failed to kill process: " << errnoWithDescription(e)};
    }

    // Wait for process to terminate.
    for (;;) {
        std::uintmax_t pidsize = boost::filesystem::file_size(pidfile);
        if (pidsize == 0) {
            // File empty.
            break;
        }
        if (pidsize == static_cast<decltype(pidsize)>(-1)) {
            // File does not exist.
            break;
        }
        sleepsecs(1);
    }

    return Status::OK();
}
#endif  // __linux__

/*
 * This function should contain the startup "actions" that we take based on the startup config.
 * It is intended to separate the actions from "storage" and "validation" of our startup
 * configuration.
 */
void startupConfigActions(const std::vector<std::string>& args) {
    // The "command" option is deprecated.  For backward compatibility, still support the "run"
    // and "dbppath" command.  The "run" command is the same as just running mongod, so just
    // falls through.
    if (moe::startupOptionsParsed.count("command")) {
        const auto command = moe::startupOptionsParsed["command"].as<std::vector<std::string>>();

        if (command[0].compare("dbpath") == 0) {
            std::cout << storageGlobalParams.dbpath << endl;
            quickExit(EXIT_SUCCESS);
        }

        if (command[0].compare("run") != 0) {
            std::cout << "Invalid command: " << command[0] << endl;
            printMongodHelp(moe::startupOptions);
            quickExit(EXIT_FAILURE);
        }

        if (command.size() > 1) {
            std::cout << "Too many parameters to 'run' command" << endl;
            printMongodHelp(moe::startupOptions);
            quickExit(EXIT_FAILURE);
        }
    }

#ifdef _WIN32
    ntservice::configureService(initService,
                                moe::startupOptionsParsed,
                                defaultServiceStrings,
                                std::vector<std::string>(),
                                args);
#endif  // _WIN32

#ifdef __linux__
    if (moe::startupOptionsParsed.count("shutdown") &&
        moe::startupOptionsParsed["shutdown"].as<bool>() == true) {
        auto status = shutdownProcessByDBPathPidFile(storageGlobalParams.dbpath);
        if (!status.isOK()) {
            std::cerr << status.reason() << std::endl;
            quickExit(EXIT_FAILURE);
        }

        quickExit(EXIT_SUCCESS);
    }
#endif
}

void setUpCollectionShardingState(ServiceContext* serviceContext) {
    if (serverGlobalParams.clusterRole == ClusterRole::ShardServer) {
        CollectionShardingStateFactory::set(
            serviceContext, std::make_unique<CollectionShardingStateFactoryShard>(serviceContext));
    } else {
        CollectionShardingStateFactory::set(
            serviceContext,
            std::make_unique<CollectionShardingStateFactoryStandalone>(serviceContext));
    }
}

void setUpCatalog(ServiceContext* serviceContext) {
    DatabaseHolder::set(serviceContext, std::make_unique<DatabaseHolderImpl>());
    Collection::Factory::set(serviceContext, std::make_unique<CollectionImpl::FactoryImpl>());
}

auto makeReplicaSetNodeExecutor(ServiceContext* serviceContext) {
    ThreadPool::Options tpOptions;
    tpOptions.threadNamePrefix = "ReplNodeDbWorker-";
    tpOptions.poolName = "ReplNodeDbWorkerThreadPool";
    tpOptions.maxThreads = ThreadPool::Options::kUnlimited;
    tpOptions.onCreateThread = [](const std::string& threadName) {
        Client::initThread(threadName.c_str());
    };
    auto hookList = std::make_unique<rpc::EgressMetadataHookList>();
    hookList->addHook(std::make_unique<rpc::VectorClockMetadataHook>(serviceContext));
    hookList->addHook(std::make_unique<rpc::ClientMetadataPropagationEgressHook>());
    return std::make_unique<executor::ThreadPoolTaskExecutor>(
        std::make_unique<ThreadPool>(tpOptions),
        executor::makeNetworkInterface("ReplNodeDbWorkerNetwork", nullptr, std::move(hookList)));
}

auto makeReplicationExecutor(ServiceContext* serviceContext) {
    ThreadPool::Options tpOptions;
    tpOptions.threadNamePrefix = "ReplCoord-";
    tpOptions.poolName = "ReplCoordThreadPool";
    tpOptions.maxThreads = 50;
    tpOptions.onCreateThread = [](const std::string& threadName) {
        Client::initThread(threadName.c_str());
    };
    auto hookList = std::make_unique<rpc::EgressMetadataHookList>();
    hookList->addHook(std::make_unique<rpc::VectorClockMetadataHook>(serviceContext));
    return std::make_unique<executor::ThreadPoolTaskExecutor>(
        std::make_unique<ThreadPool>(tpOptions),
        executor::makeNetworkInterface("ReplNetwork", nullptr, std::move(hookList)));
}

void setUpReplication(ServiceContext* serviceContext) {
    repl::StorageInterface::set(serviceContext, std::make_unique<repl::StorageInterfaceImpl>());
    auto storageInterface = repl::StorageInterface::get(serviceContext);

    auto consistencyMarkers =
        std::make_unique<repl::ReplicationConsistencyMarkersImpl>(storageInterface);
    auto recovery =
        std::make_unique<repl::ReplicationRecoveryImpl>(storageInterface, consistencyMarkers.get());
    repl::ReplicationProcess::set(
        serviceContext,
        std::make_unique<repl::ReplicationProcess>(
            storageInterface, std::move(consistencyMarkers), std::move(recovery)));
    auto replicationProcess = repl::ReplicationProcess::get(serviceContext);

    repl::DropPendingCollectionReaper::set(
        serviceContext, std::make_unique<repl::DropPendingCollectionReaper>(storageInterface));
    auto dropPendingCollectionReaper = repl::DropPendingCollectionReaper::get(serviceContext);

    repl::TopologyCoordinator::Options topoCoordOptions;
    topoCoordOptions.maxSyncSourceLagSecs = Seconds(repl::maxSyncSourceLagSecs);
    topoCoordOptions.clusterRole = serverGlobalParams.clusterRole;

    auto replCoord = std::make_unique<repl::ReplicationCoordinatorImpl>(
        serviceContext,
        getGlobalReplSettings(),
        std::make_unique<repl::ReplicationCoordinatorExternalStateImpl>(
            serviceContext, dropPendingCollectionReaper, storageInterface, replicationProcess),
        makeReplicationExecutor(serviceContext),
        std::make_unique<repl::TopologyCoordinator>(topoCoordOptions),
        replicationProcess,
        storageInterface,
        SecureRandom().nextInt64());
    // Only create a ReplicaSetNodeExecutor if sharding is disabled and replication is enabled.
    // Note that sharding sets up its own executors for scheduling work to remote nodes.
    if (serverGlobalParams.clusterRole == ClusterRole::None && replCoord->isReplEnabled())
        ReplicaSetNodeProcessInterface::setReplicaSetNodeExecutor(
            serviceContext, makeReplicaSetNodeExecutor(serviceContext));

    repl::ReplicationCoordinator::set(serviceContext, std::move(replCoord));

    IndexBuildsCoordinator::set(serviceContext, std::make_unique<IndexBuildsCoordinatorMongod>());

    // Register primary-only services here so that the services are started up when the replication
    // coordinator starts up.
    registerPrimaryOnlyServices(serviceContext);
}

void setUpObservers(ServiceContext* serviceContext) {
    auto opObserverRegistry = std::make_unique<OpObserverRegistry>();
    if (serverGlobalParams.clusterRole == ClusterRole::ShardServer) {
        DurableHistoryRegistry::get(serviceContext)
            ->registerPin(std::make_unique<ReshardingHistoryHook>());
        opObserverRegistry->addObserver(std::make_unique<OpObserverShardingImpl>());
        opObserverRegistry->addObserver(std::make_unique<ShardServerOpObserver>());
        opObserverRegistry->addObserver(std::make_unique<ReshardingOpObserver>());
        opObserverRegistry->addObserver(std::make_unique<repl::TenantMigrationDonorOpObserver>());
        opObserverRegistry->addObserver(
            std::make_unique<repl::TenantMigrationRecipientOpObserver>());
        opObserverRegistry->addObserver(std::make_unique<ShardSplitDonorOpObserver>());
        opObserverRegistry->addObserver(std::make_unique<UserWriteBlockModeOpObserver>());
    } else if (serverGlobalParams.clusterRole == ClusterRole::ConfigServer) {
        opObserverRegistry->addObserver(std::make_unique<OpObserverImpl>());
        opObserverRegistry->addObserver(std::make_unique<ConfigServerOpObserver>());
        opObserverRegistry->addObserver(std::make_unique<ReshardingOpObserver>());
    } else {
        opObserverRegistry->addObserver(std::make_unique<OpObserverImpl>());
        opObserverRegistry->addObserver(std::make_unique<repl::TenantMigrationDonorOpObserver>());
        opObserverRegistry->addObserver(
            std::make_unique<repl::TenantMigrationRecipientOpObserver>());
        opObserverRegistry->addObserver(std::make_unique<ShardSplitDonorOpObserver>());
        opObserverRegistry->addObserver(std::make_unique<UserWriteBlockModeOpObserver>());
    }
    opObserverRegistry->addObserver(std::make_unique<AuthOpObserver>());
    opObserverRegistry->addObserver(
        std::make_unique<repl::PrimaryOnlyServiceOpObserver>(serviceContext));
    opObserverRegistry->addObserver(std::make_unique<FcvOpObserver>());

    if (gFeatureFlagClusterWideConfig.isEnabledAndIgnoreFCV()) {
        opObserverRegistry->addObserver(std::make_unique<ClusterServerParameterOpObserver>());
    }

    if (audit::opObserverRegistrar) {
        audit::opObserverRegistrar(opObserverRegistry.get());
    }

    serviceContext->setOpObserver(std::move(opObserverRegistry));
}

#ifdef MONGO_CONFIG_SSL
MONGO_INITIALIZER_GENERAL(setSSLManagerType, (), ("SSLManager"))
(InitializerContext* context) {
    isSSLServer = true;
}
#endif

#if !defined(__has_feature)
#define __has_feature(x) 0
#endif

void logShutdownTimeElapsedStatistics(ServiceContext* serviceContext,
                                      Date_t beginShutdownTask,
                                      BSONObjBuilder* shutdownTimeElapsedBuilder,
                                      BSONObjBuilder* shutdownInfoBuilder) {
    mongo::Milliseconds elapsedInitAndListen =
        serviceContext->getFastClockSource()->now() - beginShutdownTask;
    shutdownTimeElapsedBuilder->append("shutdownTask total elapsed time",
                                       elapsedInitAndListen.toString());
    shutdownInfoBuilder->append("Statistics", shutdownTimeElapsedBuilder->obj());
    LOGV2_INFO(8423404,
               "shutdownTask complete",
               "Summary of time elapsed"_attr = shutdownInfoBuilder->obj());
}

// NOTE: This function may be called at any time after registerShutdownTask is called below. It
// must not depend on the prior execution of mongo initializers or the existence of threads.
void shutdownTask(const ShutdownTaskArgs& shutdownArgs) {
    // This client initiation pattern is only to be used here, with plans to eliminate this pattern
    // down the line.
    if (!haveClient())
        Client::initThread(getThreadName());

    auto const client = Client::getCurrent();
    auto const serviceContext = client->getServiceContext();

    Milliseconds shutdownTimeout;
    if (shutdownArgs.quiesceTime) {
        shutdownTimeout = *shutdownArgs.quiesceTime;
    } else {
        invariant(!shutdownArgs.isUserInitiated);
        shutdownTimeout = Milliseconds(repl::shutdownTimeoutMillisForSignaledShutdown.load());
    }

    if (MONGO_unlikely(hangBeforeShutdown.shouldFail())) {
        LOGV2(4944800, "Hanging before shutdown due to hangBeforeShutdown failpoint");
        hangBeforeShutdown.pauseWhileSet();
    }

    BSONObjBuilder shutdownTimeElapsedBuilder;
    BSONObjBuilder shutdownInfoBuilder;

    Date_t beginShutdownTask = serviceContext->getFastClockSource()->now();
    ScopeGuard logShutdownStats(
        [serviceContext, beginShutdownTask, &shutdownTimeElapsedBuilder, &shutdownInfoBuilder] {
            logShutdownTimeElapsedStatistics(serviceContext,
                                             beginShutdownTask,
                                             &shutdownTimeElapsedBuilder,
                                             &shutdownInfoBuilder);
        });

    // If we don't have shutdownArgs, we're shutting down from a signal, or other clean shutdown
    // path.
    //
    // In that case, do a default step down, still shutting down if stepDown fails.
    if (auto replCoord = repl::ReplicationCoordinator::get(serviceContext);
        replCoord && !shutdownArgs.isUserInitiated) {
        {
            TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                      "Enter terminal shutdown",
                                                      &shutdownTimeElapsedBuilder);
            replCoord->enterTerminalShutdown();
        }
        ServiceContext::UniqueOperationContext uniqueOpCtx;
        OperationContext* opCtx = client->getOperationContext();
        if (!opCtx) {
            uniqueOpCtx = client->makeOperationContext();
            opCtx = uniqueOpCtx.get();
        }
        TimeElapsedBuilderScopedTimer scopedTimer(
            serviceContext->getFastClockSource(),
            "Step down the replication coordinator for shutdown",
            &shutdownTimeElapsedBuilder);
        const auto forceShutdown = true;
        auto stepDownStartTime = opCtx->getServiceContext()->getPreciseClockSource()->now();
        // stepDown should never return an error during force shutdown.
        LOGV2_OPTIONS(4784900,
                      {LogComponent::kReplication},
                      "Stepping down the ReplicationCoordinator for shutdown",
                      "waitTime"_attr = shutdownTimeout);
        invariantStatusOK(stepDownForShutdown(opCtx, shutdownTimeout, forceShutdown));
        shutdownTimeout = std::max(
            Milliseconds::zero(),
            shutdownTimeout -
                (opCtx->getServiceContext()->getPreciseClockSource()->now() - stepDownStartTime));
    }

    {
        TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                  "Time spent in quiesce mode",
                                                  &shutdownTimeElapsedBuilder);
        if (auto replCoord = repl::ReplicationCoordinator::get(serviceContext);
            replCoord && replCoord->enterQuiesceModeIfSecondary(shutdownTimeout)) {
            ServiceContext::UniqueOperationContext uniqueOpCtx;
            OperationContext* opCtx = client->getOperationContext();
            if (!opCtx) {
                uniqueOpCtx = client->makeOperationContext();
                opCtx = uniqueOpCtx.get();
            }
            if (MONGO_unlikely(hangDuringQuiesceMode.shouldFail())) {
                LOGV2_OPTIONS(4695101,
                              {LogComponent::kReplication},
                              "hangDuringQuiesceMode failpoint enabled");
                hangDuringQuiesceMode.pauseWhileSet(opCtx);
            }

            LOGV2_OPTIONS(4695102,
                          {LogComponent::kReplication},
                          "Entering quiesce mode for shutdown",
                          "quiesceTime"_attr = shutdownTimeout);
            opCtx->sleepFor(shutdownTimeout);
            LOGV2_OPTIONS(
                4695103, {LogComponent::kReplication}, "Exiting quiesce mode for shutdown");
        }
    }


    {
        TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                  "Shut down FLE Crud subsystem",
                                                  &shutdownTimeElapsedBuilder);
        LOGV2_OPTIONS(6371601, {LogComponent::kDefault}, "Shutting down the FLE Crud thread pool");
        stopFLECrud();
    }

    {
        TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                  "Shut down MirrorMaestro",
                                                  &shutdownTimeElapsedBuilder);
        LOGV2_OPTIONS(4784901, {LogComponent::kCommand}, "Shutting down the MirrorMaestro");
        MirrorMaestro::shutdown(serviceContext);
    }

    {
        TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                  "Shut down WaitForMajorityService",
                                                  &shutdownTimeElapsedBuilder);
        LOGV2_OPTIONS(
            4784902, {LogComponent::kSharding}, "Shutting down the WaitForMajorityService");
        WaitForMajorityService::get(serviceContext).shutDown();
    }

    // Join the logical session cache before the transport layer.
    if (auto lsc = LogicalSessionCache::get(serviceContext)) {
        TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                  "Shut down the logical session cache",
                                                  &shutdownTimeElapsedBuilder);
        LOGV2(4784903, "Shutting down the LogicalSessionCache");
        lsc->joinOnShutDown();
    }

    // Shutdown the TransportLayer so that new connections aren't accepted
    if (auto tl = serviceContext->getTransportLayer()) {
        TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                  "Shut down the transport layer",
                                                  &shutdownTimeElapsedBuilder);
        LOGV2_OPTIONS(
            20562, {LogComponent::kNetwork}, "Shutdown: going to close listening sockets");
        tl->shutdown();
    }

    // Shut down the global dbclient pool so callers stop waiting for connections.
    {
        TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                  "Shut down the global connection pool",
                                                  &shutdownTimeElapsedBuilder);
        LOGV2_OPTIONS(
            4784905, {LogComponent::kNetwork}, "Shutting down the global connection pool");
        globalConnPool.shutdown();
    }

    // Inform Flow Control to stop gating writes on ticket admission. This must be done before the
    // Periodic Runner is shut down (see SERVER-41751).
    if (auto flowControlTicketholder = FlowControlTicketholder::get(serviceContext)) {
        TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                  "Shut down the flow control ticket holder",
                                                  &shutdownTimeElapsedBuilder);
        LOGV2(4784906, "Shutting down the FlowControlTicketholder");
        flowControlTicketholder->setInShutdown();
    }

    if (auto exec = ReplicaSetNodeProcessInterface::getReplicaSetNodeExecutor(serviceContext)) {
        TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                  "Shut down the replica set node executor",
                                                  &shutdownTimeElapsedBuilder);
        LOGV2_OPTIONS(
            4784907, {LogComponent::kReplication}, "Shutting down the replica set node executor");
        exec->shutdown();
        exec->join();
    }

    if (auto storageEngine = serviceContext->getStorageEngine()) {
        if (storageEngine->supportsReadConcernSnapshot()) {
            LOGV2(4784908, "Shutting down the PeriodicThreadToAbortExpiredTransactions");
            PeriodicThreadToAbortExpiredTransactions::get(serviceContext)->stop();
        }

        ServiceContext::UniqueOperationContext uniqueOpCtx;
        OperationContext* opCtx = client->getOperationContext();
        if (!opCtx) {
            uniqueOpCtx = client->makeOperationContext();
            opCtx = uniqueOpCtx.get();
        }
        opCtx->setIsExecutingShutdown();

        // This can wait a long time while we drain the secondary's apply queue, especially if
        // it is building an index.
        LOGV2_OPTIONS(
            4784909, {LogComponent::kReplication}, "Shutting down the ReplicationCoordinator");
        repl::ReplicationCoordinator::get(serviceContext)
            ->shutdown(opCtx, &shutdownTimeElapsedBuilder);

        // Terminate the index consistency check.
        if (serverGlobalParams.clusterRole == ClusterRole::ConfigServer) {
            TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                      "Shut down the index consistency checker",
                                                      &shutdownTimeElapsedBuilder);
            LOGV2_OPTIONS(4784904,
                          {LogComponent::kSharding},
                          "Shutting down the PeriodicShardedIndexConsistencyChecker");
            PeriodicShardedIndexConsistencyChecker::get(serviceContext).onShutDown();
        }

        LOGV2_OPTIONS(
            4784910, {LogComponent::kSharding}, "Shutting down the ShardingInitializationMongoD");
        ShardingInitializationMongoD::get(serviceContext)->shutDown(opCtx);

        // Acquire the RSTL in mode X. First we enqueue the lock request, then kill all operations,
        // destroy all stashed transaction resources in order to release locks, and finally wait
        // until the lock request is granted.
        LOGV2_OPTIONS(4784911,
                      {LogComponent::kReplication},
                      "Enqueuing the ReplicationStateTransitionLock for shutdown");
        repl::ReplicationStateTransitionLockGuard rstl(
            opCtx, MODE_X, repl::ReplicationStateTransitionLockGuard::EnqueueOnly());

        // Kill all operations except FTDC to continue gathering metrics. This makes all newly
        // created opCtx to be immediately interrupted. After this point, the opCtx will have been
        // marked as killed and will not be usable other than to kill all transactions directly
        // below.
        LOGV2_OPTIONS(4784912, {LogComponent::kDefault}, "Killing all operations for shutdown");
        {
            const std::set<std::string> excludedClients = {std::string(kFTDCThreadName)};
            TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                      "Kill all operations for shutdown",
                                                      &shutdownTimeElapsedBuilder);
            serviceContext->setKillAllOperations(excludedClients);

            if (MONGO_unlikely(pauseWhileKillingOperationsAtShutdown.shouldFail())) {
                LOGV2_OPTIONS(4701700,
                              {LogComponent::kDefault},
                              "pauseWhileKillingOperationsAtShutdown failpoint enabled");
                sleepsecs(1);
            }
        }

        {
            // Clear tenant migration access blockers after killing all operation contexts to ensure
            // that no operation context cancellation token continuation holds the last reference to
            // the TenantMigrationAccessBlockerExecutor.
            TimeElapsedBuilderScopedTimer scopedTimer(
                serviceContext->getFastClockSource(),
                "Shut down all tenant migration access blockers on global shutdown",
                &shutdownTimeElapsedBuilder);
            LOGV2_OPTIONS(5093807,
                          {LogComponent::kTenantMigration},
                          "Shutting down all TenantMigrationAccessBlockers on global shutdown");
            TenantMigrationAccessBlockerRegistry::get(serviceContext).shutDown();
        }

        // Destroy all stashed transaction resources, in order to release locks.
        {
            TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                      "Shut down all open transactions",
                                                      &shutdownTimeElapsedBuilder);
            LOGV2_OPTIONS(4784913, {LogComponent::kCommand}, "Shutting down all open transactions");
            killSessionsLocalShutdownAllTransactions(opCtx);
        }

        {
            TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                      "Acquire the RSTL for shutdown",
                                                      &shutdownTimeElapsedBuilder);
            LOGV2_OPTIONS(4784914,
                          {LogComponent::kReplication},
                          "Acquiring the ReplicationStateTransitionLock for shutdown");
            rstl.waitForLockUntil(Date_t::max());
        }

        // Release the rstl before waiting for the index build threads to join as index build
        // reacquires rstl in uninterruptible lock guard to finish their cleanup process.
        rstl.release();

        // Shuts down the thread pool and waits for index builds to finish.
        // Depends on setKillAllOperations() above to interrupt the index build operations.
        {
            TimeElapsedBuilderScopedTimer scopedTimer(
                serviceContext->getFastClockSource(),
                "Shut down the IndexBuildsCoordinator and wait for index builds to finish",
                &shutdownTimeElapsedBuilder);
            LOGV2_OPTIONS(
                4784915, {LogComponent::kIndex}, "Shutting down the IndexBuildsCoordinator");
            IndexBuildsCoordinator::get(serviceContext)->shutdown(opCtx);
        }
    }

    {
        TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                  "Shut down the replica set monitor",
                                                  &shutdownTimeElapsedBuilder);
        LOGV2_OPTIONS(4784918, {LogComponent::kNetwork}, "Shutting down the ReplicaSetMonitor");
        ReplicaSetMonitor::shutdown();
    }

    if (auto sr = Grid::get(serviceContext)->shardRegistry()) {
        TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                  "Shut down the shard registry",
                                                  &shutdownTimeElapsedBuilder);
        LOGV2_OPTIONS(4784919, {LogComponent::kSharding}, "Shutting down the shard registry");
        sr->shutdown();
    }

    if (ShardingState::get(serviceContext)->enabled()) {
        TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                  "Shut down the transaction coordinator service",
                                                  &shutdownTimeElapsedBuilder);
        TransactionCoordinatorService::get(serviceContext)->shutdown();
    }

    // Validator shutdown must be called after setKillAllOperations is called. Otherwise, this can
    // deadlock.
    if (auto validator = LogicalTimeValidator::get(serviceContext)) {
        TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                  "Shut down the logical time validator",
                                                  &shutdownTimeElapsedBuilder);
        LOGV2_OPTIONS(
            4784920, {LogComponent::kReplication}, "Shutting down the LogicalTimeValidator");
        validator->shutDown();
    }

    if (TestingProctor::instance().isEnabled()) {
        if (auto pool = Grid::get(serviceContext)->getExecutorPool()) {
            TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                      "Shut down the executor pool",
                                                      &shutdownTimeElapsedBuilder);
            LOGV2_OPTIONS(6773200, {LogComponent::kSharding}, "Shutting down the ExecutorPool");
            pool->shutdownAndJoin();
        }
    }

    // The migrationutil executor must be shut down before shutting down the CatalogCacheLoader.
    // Otherwise, it may try to schedule work on the CatalogCacheLoader and fail.
    LOGV2_OPTIONS(4784921, {LogComponent::kSharding}, "Shutting down the MigrationUtilExecutor");
    auto migrationUtilExecutor = migrationutil::getMigrationUtilExecutor(serviceContext);
    {
        TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                  "Shut down the migration util executor",
                                                  &shutdownTimeElapsedBuilder);
        migrationUtilExecutor->shutdown();
        migrationUtilExecutor->join();
    }

    if (Grid::get(serviceContext)->isShardingInitialized()) {
        // The CatalogCache must be shuted down before shutting down the CatalogCacheLoader as the
        // CatalogCache may try to schedule work on CatalogCacheLoader and fail.
        TimeElapsedBuilderScopedTimer scopedTimer(
            serviceContext->getFastClockSource(),
            "Shut down the catalog cache and catalog cache loader",
            &shutdownTimeElapsedBuilder);
        LOGV2_OPTIONS(6773201, {LogComponent::kSharding}, "Shutting down the CatalogCache");
        Grid::get(serviceContext)->catalogCache()->shutDownAndJoin();

        LOGV2_OPTIONS(4784922, {LogComponent::kSharding}, "Shutting down the CatalogCacheLoader");
        CatalogCacheLoader::get(serviceContext).shutDown();
    }

    // Shutdown the Service Entry Point and its sessions and give it a grace period to complete.
    if (auto sep = serviceContext->getServiceEntryPoint()) {
        LOGV2_OPTIONS(4784923, {LogComponent::kCommand}, "Shutting down the ServiceEntryPoint");
        if (!sep->shutdown(Seconds(10))) {
            LOGV2_OPTIONS(20563,
                          {LogComponent::kNetwork},
                          "Service entry point did not shutdown within the time limit");
        }
    }

    if (auto* healthLog = HealthLogInterface::get(serviceContext)) {
        TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                  "Shut down the health log",
                                                  &shutdownTimeElapsedBuilder);
        LOGV2(4784927, "Shutting down the HealthLog");
        healthLog->shutdown();
    }

    {
        TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                  "Shut down the TTL monitor",
                                                  &shutdownTimeElapsedBuilder);
        LOGV2(4784928, "Shutting down the TTL monitor");
        shutdownTTLMonitor(serviceContext);
    }

    {
        TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                  "Shut down expired pre-images remover",
                                                  &shutdownTimeElapsedBuilder);
        LOGV2(6278511, "Shutting down the Change Stream Expired Pre-images Remover");
        shutdownChangeStreamExpiredPreImagesRemover(serviceContext);
    }

    // We should always be able to acquire the global lock at shutdown.
    // An OperationContext is not necessary to call lockGlobal() during shutdown, as it's only used
    // to check that lockGlobal() is not called after a transaction timestamp has been set.
    //
    // For a Windows service, dbexit does not call exit(), so we must leak the lock outside
    // of this function to prevent any operations from running that need a lock.
    //
    LOGV2(4784929, "Acquiring the global lock for shutdown");
    LockerImpl* globalLocker = new LockerImpl(serviceContext);
    globalLocker->lockGlobal(nullptr, MODE_X);

    // Global storage engine may not be started in all cases before we exit
    if (serviceContext->getStorageEngine()) {
        TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                  "Shut down the storage engine",
                                                  &shutdownTimeElapsedBuilder);
        LOGV2(4784930, "Shutting down the storage engine");
        shutdownGlobalStorageEngineCleanly(serviceContext);
    }

    // We drop the scope cache because leak sanitizer can't see across the
    // thread we use for proxying MozJS requests. Dropping the cache cleans up
    // the memory and makes leak sanitizer happy.
    LOGV2_OPTIONS(4784931, {LogComponent::kDefault}, "Dropping the scope cache for shutdown");
    ScriptEngine::dropScopeCache();

    // Shutdown Full-Time Data Capture
    {
        TimeElapsedBuilderScopedTimer scopedTimer(serviceContext->getFastClockSource(),
                                                  "Shut down full-time data capture",
                                                  &shutdownTimeElapsedBuilder);
        stopMongoDFTDC();
    }

    LOGV2(20565, "Now exiting");

    audit::logShutdown(client);

#ifndef MONGO_CONFIG_USE_RAW_LATCHES
    LatchAnalyzer::get(serviceContext).dump();
#endif

#if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer)
    // SessionKiller relies on the network stack being cleanly shutdown which only occurs under
    // sanitizers
    SessionKiller::shutdown(serviceContext);
#endif

    FlowControl::shutdown(serviceContext);
#ifdef MONGO_CONFIG_SSL
    OCSPManager::shutdown(serviceContext);
#endif
}

void disableMongodTHPUnderTestingEnvironment() {
#ifdef __linux__
    if (TestingProctor::instance().isEnabled()) {
        if (prctl(PR_SET_THP_DISABLE, 1, 0, 0, 0) == -1) {
            LOGV2_WARNING(8751800, "Could not disable THP on mongod");
        } else {
            LOGV2_INFO(8751801, "Successfully disabled THP on mongod");
        }
    }
#endif
}

}  // namespace

int mongod_main(int argc, char* argv[]) {
    ThreadSafetyContext::getThreadSafetyContext()->forbidMultiThreading();

    registerShutdownTask(shutdownTask);

    setupSignalHandlers();

    srand(static_cast<unsigned>(curTimeMicros64()));

    Status status = mongo::runGlobalInitializers(std::vector<std::string>(argv, argv + argc));
    if (!status.isOK()) {
        LOGV2_FATAL_OPTIONS(
            20574,
            logv2::LogOptions(logv2::LogComponent::kControl, logv2::FatalMode::kContinue),
            "Error during global initialization: {error}",
            "Error during global initialization",
            "error"_attr = status);
        quickExit(EXIT_FAILURE);
    }

    disableMongodTHPUnderTestingEnvironment();

    auto* service = [] {
        try {
            auto serviceContextHolder = ServiceContext::make();
            auto* serviceContext = serviceContextHolder.get();
            setGlobalServiceContext(std::move(serviceContextHolder));

            return serviceContext;
        } catch (...) {
            auto cause = exceptionToStatus();
            LOGV2_FATAL_OPTIONS(
                20575,
                logv2::LogOptions(logv2::LogComponent::kControl, logv2::FatalMode::kContinue),
                "Error creating service context: {error}",
                "Error creating service context",
                "error"_attr = redact(cause));
            quickExit(EXIT_FAILURE);
        }
    }();

    {
        // Create the durable history registry prior to calling the `setUp*` methods. They may
        // depend on it existing at this point.
        DurableHistoryRegistry::set(service, std::make_unique<DurableHistoryRegistry>());
        DurableHistoryRegistry* registry = DurableHistoryRegistry::get(service);
        if (getTestCommandsEnabled()) {
            registry->registerPin(std::make_unique<TestingDurableHistoryPin>());
        }
    }

    // Attempt to rotate the audit log pre-emptively on startup to avoid any potential conflicts
    // with existing log state. If this rotation fails, then exit nicely with failure
    try {
        audit::rotateAuditLog();
    } catch (...) {

        Status err = mongo::exceptionToStatus();
        LOGV2(6169900, "Error rotating audit log", "error"_attr = err);

        quickExit(ExitCode::EXIT_AUDIT_ROTATE_ERROR);
    }

    setUpCollectionShardingState(service);
    setUpCatalog(service);
    setUpReplication(service);
    setUpObservers(service);
    service->setServiceEntryPoint(std::make_unique<ServiceEntryPointMongod>(service));
    SessionCatalog::get(service)->setOnEagerlyReapedSessionsFn(
        InternalTransactionsReapService::onEagerlyReapedSessions);

    ErrorExtraInfo::invariantHaveAllParsers();

    startupConfigActions(std::vector<std::string>(argv, argv + argc));
    cmdline_utils::censorArgvArray(argc, argv);

    if (!initializeServerGlobalState(service))
        quickExit(EXIT_FAILURE);

    // There is no single-threaded guarantee beyond this point.
    ThreadSafetyContext::getThreadSafetyContext()->allowMultiThreading();
    LOGV2(5945603, "Multi threading initialized");

    // Per SERVER-7434, startSignalProcessingThread must run after any forks (i.e.
    // initializeServerGlobalState) and before the creation of any other threads
    startSignalProcessingThread();

    ReadWriteConcernDefaults::create(service, readWriteConcernDefaultsCacheLookupMongoD);
    ChangeStreamOptionsManager::create(service);

#if defined(_WIN32)
    if (ntservice::shouldStartService()) {
        ntservice::startService();
        // exits directly and so never reaches here either.
    }
#endif

    ExitCode exitCode = initAndListen(service, serverGlobalParams.port);
    exitCleanly(exitCode);
    return 0;
}

}  // namespace mongo
