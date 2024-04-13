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
#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kControl

#include "mongo/idl/cluster_parameter_synchronization_helpers.h"

#include "mongo/base/string_data.h"
#include "mongo/db/audit.h"
#include "mongo/db/catalog_raii.h"
#include "mongo/db/multitenancy_gen.h"
#include "mongo/logv2/log.h"

namespace mongo::cluster_parameters {

constexpr auto kIdField = "_id"_sd;
constexpr auto kCPTField = "clusterParameterTime"_sd;
constexpr auto kOplog = "oplog"_sd;

void updateParameter(BSONObj doc, StringData mode) {
    auto nameElem = doc[kIdField];
    if (nameElem.type() != String) {
        LOGV2_DEBUG(6226301,
                    1,
                    "Update with invalid cluster server parameter name",
                    "mode"_attr = mode,
                    "_id"_attr = nameElem);
        return;
    }

    auto name = nameElem.valueStringData();
    auto* sp = ServerParameterSet::getClusterParameterSet()->getIfExists(name);
    if (!sp) {
        LOGV2_DEBUG(6226300,
                    3,
                    "Update to unknown cluster server parameter",
                    "mode"_attr = mode,
                    "name"_attr = name);
        return;
    }

    auto cptElem = doc[kCPTField];
    if ((cptElem.type() != mongo::Date) && (cptElem.type() != bsonTimestamp)) {
        LOGV2_DEBUG(6226302,
                    1,
                    "Update to cluster server parameter has invalid clusterParameterTime",
                    "mode"_attr = mode,
                    "name"_attr = name,
                    "clusterParameterTime"_attr = cptElem);
        return;
    }

    uassertStatusOK(sp->set(doc));
}

void clearParameter(ServerParameter* sp) {
    if (sp->getClusterParameterTime() == LogicalTime::kUninitialized) {
        // Nothing to clear.
        return;
    }

    uassertStatusOK(sp->reset());
}

void clearParameter(StringData id) {
    auto* sp = ServerParameterSet::getClusterParameterSet()->getIfExists(id);
    if (!sp) {
        LOGV2_DEBUG(6226303,
                    5,
                    "oplog event deletion of unknown cluster server parameter",
                    "name"_attr = id);
        return;
    }

    clearParameter(sp);
}

void clearAllParameters() {
    const auto& params = ServerParameterSet::getClusterParameterSet()->getMap();
    for (const auto& it : params) {
        clearParameter(it.second);
    }
}

void initializeAllParametersFromDisk(OperationContext* opCtx) {
    doLoadAllParametersFromDisk(
        opCtx, "initializing"_sd, [](OperationContext* opCtx, BSONObj doc, StringData mode) {
            updateParameter(doc, mode);
        });
}

void resynchronizeAllParametersFromDisk(OperationContext* opCtx) {
    const auto& allParams = ServerParameterSet::getClusterParameterSet()->getMap();
    std::set<std::string> unsetSettings;
    for (const auto& it : allParams) {
        unsetSettings.insert(it.second->name());
    }

    doLoadAllParametersFromDisk(
        opCtx,
        "resynchronizing"_sd,
        [&unsetSettings](OperationContext* opCtx, BSONObj doc, StringData mode) {
            unsetSettings.erase(doc[kIdField].str());
            updateParameter(doc, mode);
        });

    // For all known settings which were not present in this resync,
    // explicitly clear any value which may be present in-memory.
    for (const auto& setting : unsetSettings) {
        clearParameter(setting);
    }
}

void maybeUpdateClusterParametersPostImportCollectionCommit(OperationContext* opCtx,
                                                            const NamespaceString& nss) {
    if (nss == NamespaceString::kClusterParametersNamespace) {
        // Something was imported, do a full collection scan to sync up.
        cluster_parameters::initializeAllParametersFromDisk(opCtx);
    }
}

}  // namespace mongo::cluster_parameters
