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

#pragma once

#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "mongo/db/query/optimizer/containers.h"
#include "mongo/db/query/optimizer/utils/printable_enum.h"


namespace mongo::optimizer {

using FieldNameType = std::string;
using FieldPathType = std::vector<FieldNameType>;

using CollectionNameType = std::string;

using ProjectionName = std::string;
using ProjectionNameSet = opt::unordered_set<ProjectionName>;
using ProjectionNameOrderedSet = std::set<ProjectionName>;
using ProjectionNameVector = std::vector<ProjectionName>;
using ProjectionRenames = opt::unordered_map<ProjectionName, ProjectionName>;

// Map from scanDefName to rid projection name.
using RIDProjectionsMap = opt::unordered_map<std::string, ProjectionName>;

class ProjectionNameOrderPreservingSet {
public:
    ProjectionNameOrderPreservingSet() = default;
    ProjectionNameOrderPreservingSet(ProjectionNameVector v);

    ProjectionNameOrderPreservingSet(const ProjectionNameOrderPreservingSet& other);
    ProjectionNameOrderPreservingSet(ProjectionNameOrderPreservingSet&& other) noexcept;

    bool operator==(const ProjectionNameOrderPreservingSet& other) const;

    std::pair<size_t, bool> emplace_back(ProjectionName projectionName);
    std::pair<size_t, bool> find(const ProjectionName& projectionName) const;
    bool erase(const ProjectionName& projectionName);

    bool isEqualIgnoreOrder(const ProjectionNameOrderPreservingSet& other) const;

    const ProjectionNameVector& getVector() const;

private:
    opt::unordered_map<ProjectionName, size_t> _map;
    ProjectionNameVector _vector;
};

#define INDEXREQTARGET_NAMES(F) \
    F(Index)                    \
    F(Seek)                     \
    F(Complete)

MAKE_PRINTABLE_ENUM(IndexReqTarget, INDEXREQTARGET_NAMES);
MAKE_PRINTABLE_ENUM_STRING_ARRAY(IndexReqTargetEnum, IndexReqTarget, INDEXREQTARGET_NAMES);
#undef INDEXREQTARGET_NAMES

#define DISTRIBUTIONTYPE_NAMES(F) \
    F(Centralized)                \
    F(Replicated)                 \
    F(RoundRobin)                 \
    F(HashPartitioning)           \
    F(RangePartitioning)          \
    F(UnknownPartitioning)

MAKE_PRINTABLE_ENUM(DistributionType, DISTRIBUTIONTYPE_NAMES);
MAKE_PRINTABLE_ENUM_STRING_ARRAY(DistributionTypeEnum, DistributionType, DISTRIBUTIONTYPE_NAMES);
#undef DISTRIBUTIONTYPE_NAMES

// In case of covering scan, index, or fetch, specify names of bound projections for each field.
// Also optionally specify if applicable the rid and record (root) projections.
struct FieldProjectionMap {
    ProjectionName _ridProjection;
    ProjectionName _rootProjection;
    opt::unordered_map<FieldNameType, ProjectionName> _fieldProjections;

    bool operator==(const FieldProjectionMap& other) const;
};

// Used to generate field names encoding index keys for covered indexes.
static constexpr const char* kIndexKeyPrefix = "<indexKey>";

/**
 * Memo-related types.
 */
using GroupIdType = int64_t;

// Logical node id.
struct MemoLogicalNodeId {
    GroupIdType _groupId;
    size_t _index;

    bool operator==(const MemoLogicalNodeId& other) const;
};

struct NodeIdHash {
    size_t operator()(const MemoLogicalNodeId& id) const;
};
using NodeIdSet = opt::unordered_set<MemoLogicalNodeId, NodeIdHash>;

// Physical node id.
struct MemoPhysicalNodeId {
    GroupIdType _groupId;
    size_t _index;

    bool operator==(const MemoPhysicalNodeId& other) const;
};

class DebugInfo {
public:
    static const int kIterationLimitForTests = 10000;
    static const int kDefaultDebugLevelForTests = 1;

    static DebugInfo kDefaultForTests;
    static DebugInfo kDefaultForProd;

    DebugInfo(bool debugMode, int debugLevel, int iterationLimit);

    bool isDebugMode() const;

    bool hasDebugLevel(int debugLevel) const;

    bool exceedsIterationLimit(int iterations) const;

private:
    // Are we in debug mode? Can we do additional logging, etc?
    const bool _debugMode;

    const int _debugLevel;

    // Maximum number of rewrite iterations.
    const int _iterationLimit;
};

using CEType = double;
using SelectivityType = double;

class CostType {
public:
    static CostType kInfinity;
    static CostType kZero;

    static CostType fromDouble(double cost);

    CostType(const CostType& other) = default;
    CostType(CostType&& other) = default;
    CostType& operator=(const CostType& other) = default;

    bool operator==(const CostType& other) const;
    bool operator!=(const CostType& other) const;
    bool operator<(const CostType& other) const;

    CostType operator+(const CostType& other) const;
    CostType operator-(const CostType& other) const;
    CostType& operator+=(const CostType& other);

    std::string toString() const;

    /**
     * Returns the cost as a double, or asserts if infinite.
     */
    double getCost() const;

    bool isInfinite() const;

private:
    CostType(bool isInfinite, double cost);

    bool _isInfinite;
    double _cost;
};

struct CostAndCE {
    CostType _cost;
    CEType _ce;
};

#define COLLATIONOP_OPNAMES(F) \
    F(Ascending)               \
    F(Descending)              \
    F(Clustered)

MAKE_PRINTABLE_ENUM(CollationOp, COLLATIONOP_OPNAMES);
MAKE_PRINTABLE_ENUM_STRING_ARRAY(CollationOpEnum, CollationOp, COLLATIONOP_OPNAMES);
#undef PATHSYNTAX_OPNAMES

using ProjectionCollationEntry = std::pair<ProjectionName, CollationOp>;
using ProjectionCollationSpec = std::vector<ProjectionCollationEntry>;

CollationOp reverseCollationOp(CollationOp op);

bool collationOpsCompatible(CollationOp availableOp, CollationOp requiredOp);
bool collationsCompatible(const ProjectionCollationSpec& available,
                          const ProjectionCollationSpec& required);

enum class DisableIndexOptions {
    Enabled,            // All types of indexes are enabled.
    DisableAll,         // Disable all indexes.
    DisablePartialOnly  // Only disable partial indexes.
};

struct QueryHints {
    // Disable full collection scans.
    bool _disableScan = false;

    // Disable index scans.
    DisableIndexOptions _disableIndexes = DisableIndexOptions::Enabled;

    // Disable placing a hash-join during RIDIntersect implementation.
    bool _disableHashJoinRIDIntersect = false;

    // Disable placing a merge-join during RIDIntersect implementation.
    bool _disableMergeJoinRIDIntersect = false;

    // Disable placing a group-by and union based RIDIntersect implementation.
    bool _disableGroupByAndUnionRIDIntersect = false;

    // If set keep track of rejected plans in the memo.
    bool _keepRejectedPlans = false;

    // Disable Cascades branch-and-bound strategy, and fully evaluate all plans. Used in conjunction
    // with keeping rejected plans.
    bool _disableBranchAndBound = false;
};

}  // namespace mongo::optimizer
