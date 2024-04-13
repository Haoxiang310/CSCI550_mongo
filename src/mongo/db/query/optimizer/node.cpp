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

#include <functional>
#include <stack>

#include "mongo/db/query/optimizer/node.h"
#include "mongo/db/query/optimizer/utils/utils.h"

namespace mongo::optimizer {

/**
 * A simple helper that creates a vector of Sources and binds names.
 */
static ABT buildSimpleBinder(const ProjectionNameVector& names) {
    ABTVector sources;
    for (size_t idx = 0; idx < names.size(); ++idx) {
        sources.emplace_back(make<Source>());
    }

    return make<ExpressionBinder>(names, std::move(sources));
}

static ABT buildReferences(const ProjectionNameSet& projections) {
    ABTVector variables;
    ProjectionNameOrderedSet ordered = convertToOrderedSet(projections);
    for (const ProjectionName& projection : ordered) {
        variables.emplace_back(make<Variable>(projection));
    }
    return make<References>(std::move(variables));
}

ScanNode::ScanNode(ProjectionName projectionName, std::string scanDefName)
    : Base(buildSimpleBinder({std::move(projectionName)})), _scanDefName(std::move(scanDefName)) {}

const ProjectionName& ScanNode::getProjectionName() const {
    return binder().names()[0];
}

const ProjectionType& ScanNode::getProjection() const {
    return binder().exprs()[0];
}

const std::string& ScanNode::getScanDefName() const {
    return _scanDefName;
}

bool ScanNode::operator==(const ScanNode& other) const {
    return getProjectionName() == other.getProjectionName() && _scanDefName == other._scanDefName;
}

static ProjectionNameVector extractProjectionNamesForScan(
    const FieldProjectionMap& fieldProjectionMap) {
    ProjectionNameVector result;

    if (!fieldProjectionMap._ridProjection.empty()) {
        result.push_back(fieldProjectionMap._ridProjection);
    }
    if (!fieldProjectionMap._rootProjection.empty()) {
        result.push_back(fieldProjectionMap._rootProjection);
    }
    for (const auto& entry : fieldProjectionMap._fieldProjections) {
        result.push_back(entry.second);
    }

    return result;
}

PhysicalScanNode::PhysicalScanNode(FieldProjectionMap fieldProjectionMap,
                                   std::string scanDefName,
                                   bool useParallelScan)
    : Base(buildSimpleBinder(extractProjectionNamesForScan(fieldProjectionMap))),
      _fieldProjectionMap(std::move(fieldProjectionMap)),
      _scanDefName(std::move(scanDefName)),
      _useParallelScan(useParallelScan) {}

bool PhysicalScanNode::operator==(const PhysicalScanNode& other) const {
    return _fieldProjectionMap == other._fieldProjectionMap && _scanDefName == other._scanDefName &&
        _useParallelScan == other._useParallelScan;
}

const FieldProjectionMap& PhysicalScanNode::getFieldProjectionMap() const {
    return _fieldProjectionMap;
}

const std::string& PhysicalScanNode::getScanDefName() const {
    return _scanDefName;
}

bool PhysicalScanNode::useParallelScan() const {
    return _useParallelScan;
}

ValueScanNode::ValueScanNode(ProjectionNameVector projections)
    : ValueScanNode(std::move(projections), Constant::emptyArray()) {}

ValueScanNode::ValueScanNode(ProjectionNameVector projections, ABT valueArray)
    : Base(buildSimpleBinder(std::move(projections))), _valueArray(std::move(valueArray)) {
    const auto constPtr = _valueArray.cast<Constant>();
    uassert(6624081, "Expected a constant", constPtr != nullptr);

    const auto [tag, val] = constPtr->get();
    uassert(6624082, "Expected an array constant.", tag == sbe::value::TypeTags::Array);

    const auto arr = sbe::value::getArrayView(val);
    _arraySize = arr->size();
    const size_t projectionCount = binder().names().size();
    for (size_t i = 0; i < _arraySize; i++) {
        const auto [tag1, val1] = arr->getAt(i);
        uassert(6624083, "Expected an array element.", tag1 == sbe::value::TypeTags::Array);
        const size_t innerSize = sbe::value::getArrayView(val1)->size();
        uassert(6624084, "Invalid array size.", innerSize == projectionCount);
    }
}

bool ValueScanNode::operator==(const ValueScanNode& other) const {
    return binder() == other.binder() && _arraySize == other._arraySize &&
        _valueArray == other._valueArray;
}

const ABT& ValueScanNode::getValueArray() const {
    return _valueArray;
}

size_t ValueScanNode::getArraySize() const {
    return _arraySize;
}

CoScanNode::CoScanNode() : Base() {}

bool CoScanNode::operator==(const CoScanNode& other) const {
    return true;
}

IndexScanNode::IndexScanNode(FieldProjectionMap fieldProjectionMap, IndexSpecification indexSpec)
    : Base(buildSimpleBinder(extractProjectionNamesForScan(fieldProjectionMap))),
      _fieldProjectionMap(std::move(fieldProjectionMap)),
      _indexSpec(std::move(indexSpec)) {}

bool IndexScanNode::operator==(const IndexScanNode& other) const {
    // Scan spec does not participate, the indexSpec by itself should determine equality.
    return _fieldProjectionMap == other._fieldProjectionMap && _indexSpec == other._indexSpec;
}

const FieldProjectionMap& IndexScanNode::getFieldProjectionMap() const {
    return _fieldProjectionMap;
}

const IndexSpecification& IndexScanNode::getIndexSpecification() const {
    return _indexSpec;
}

SeekNode::SeekNode(ProjectionName ridProjectionName,
                   FieldProjectionMap fieldProjectionMap,
                   std::string scanDefName)
    : Base(buildSimpleBinder(extractProjectionNamesForScan(fieldProjectionMap)),
           make<References>(ProjectionNameVector{ridProjectionName})),
      _ridProjectionName(std::move(ridProjectionName)),
      _fieldProjectionMap(std::move(fieldProjectionMap)),
      _scanDefName(std::move(scanDefName)) {}

bool SeekNode::operator==(const SeekNode& other) const {
    return _ridProjectionName == other._ridProjectionName &&
        _fieldProjectionMap == other._fieldProjectionMap && _scanDefName == other._scanDefName;
}

const FieldProjectionMap& SeekNode::getFieldProjectionMap() const {
    return _fieldProjectionMap;
}

const std::string& SeekNode::getScanDefName() const {
    return _scanDefName;
}

const ProjectionName& SeekNode::getRIDProjectionName() const {
    return _ridProjectionName;
}

MemoLogicalDelegatorNode::MemoLogicalDelegatorNode(const GroupIdType groupId)
    : Base(), _groupId(groupId) {}

GroupIdType MemoLogicalDelegatorNode::getGroupId() const {
    return _groupId;
}

bool MemoLogicalDelegatorNode::operator==(const MemoLogicalDelegatorNode& other) const {
    return _groupId == other._groupId;
}

MemoPhysicalDelegatorNode::MemoPhysicalDelegatorNode(const MemoPhysicalNodeId nodeId)
    : Base(), _nodeId(nodeId) {}

bool MemoPhysicalDelegatorNode::operator==(const MemoPhysicalDelegatorNode& other) const {
    return _nodeId == other._nodeId;
}

MemoPhysicalNodeId MemoPhysicalDelegatorNode::getNodeId() const {
    return _nodeId;
}

FilterNode::FilterNode(FilterType filter, ABT child) : Base(std::move(child), std::move(filter)) {
    assertExprSort(getFilter());
    assertNodeSort(getChild());
}

bool FilterNode::operator==(const FilterNode& other) const {
    return getFilter() == other.getFilter() && getChild() == other.getChild();
}

const FilterType& FilterNode::getFilter() const {
    return get<1>();
}

FilterType& FilterNode::getFilter() {
    return get<1>();
}

const ABT& FilterNode::getChild() const {
    return get<0>();
}

ABT& FilterNode::getChild() {
    return get<0>();
}

EvaluationNode::EvaluationNode(ProjectionName projectionName, ProjectionType projection, ABT child)
    : Base(std::move(child),
           make<ExpressionBinder>(std::move(projectionName), std::move(projection))) {
    assertNodeSort(getChild());
}

bool EvaluationNode::operator==(const EvaluationNode& other) const {
    return binder() == other.binder() && getProjection() == other.getProjection() &&
        getChild() == other.getChild();
}

RIDIntersectNode::RIDIntersectNode(ProjectionName scanProjectionName,
                                   const bool hasLeftIntervals,
                                   const bool hasRightIntervals,
                                   ABT leftChild,
                                   ABT rightChild)
    : Base(std::move(leftChild), std::move(rightChild)),
      _scanProjectionName(std::move(scanProjectionName)),
      _hasLeftIntervals(hasLeftIntervals),
      _hasRightIntervals(hasRightIntervals) {
    assertNodeSort(getLeftChild());
    assertNodeSort(getRightChild());
}

const ABT& RIDIntersectNode::getLeftChild() const {
    return get<0>();
}

ABT& RIDIntersectNode::getLeftChild() {
    return get<0>();
}

const ABT& RIDIntersectNode::getRightChild() const {
    return get<1>();
}

ABT& RIDIntersectNode::getRightChild() {
    return get<1>();
}

bool RIDIntersectNode::operator==(const RIDIntersectNode& other) const {
    return _scanProjectionName == other._scanProjectionName &&
        _hasLeftIntervals == other._hasLeftIntervals &&
        _hasRightIntervals == other._hasRightIntervals && getLeftChild() == other.getLeftChild() &&
        getRightChild() == other.getRightChild();
}

const ProjectionName& RIDIntersectNode::getScanProjectionName() const {
    return _scanProjectionName;
}

bool RIDIntersectNode::hasLeftIntervals() const {
    return _hasLeftIntervals;
}

bool RIDIntersectNode::hasRightIntervals() const {
    return _hasRightIntervals;
}

static ProjectionNameVector createSargableBindings(const PartialSchemaRequirements& reqMap) {
    ProjectionNameVector result;
    for (const auto& entry : reqMap) {
        if (entry.second.hasBoundProjectionName()) {
            result.push_back(entry.second.getBoundProjectionName());
        }
    }
    return result;
}

static ProjectionNameVector createSargableReferences(const PartialSchemaRequirements& reqMap) {
    ProjectionNameOrderPreservingSet result;
    for (const auto& entry : reqMap) {
        result.emplace_back(entry.first._projectionName);
    }
    return result.getVector();
}

SargableNode::SargableNode(PartialSchemaRequirements reqMap,
                           CandidateIndexMap candidateIndexMap,
                           const IndexReqTarget target,
                           ABT child)
    : Base(std::move(child),
           buildSimpleBinder(createSargableBindings(reqMap)),
           make<References>(createSargableReferences(reqMap))),
      _reqMap(std::move(reqMap)),
      _candidateIndexMap(std::move(candidateIndexMap)),
      _target(target) {
    assertNodeSort(getChild());
    uassert(6624085, "Empty requirements map", !_reqMap.empty());
    // We currently use a 64-bit mask when splitting into left and right requirements.
    uassert(6624086, "Requirements map too large", _reqMap.size() < 64);

    // Assert merged map does not contain duplicate bound projections.
    ProjectionNameSet boundsProjectionNameSet;
    for (const auto& entry : _reqMap) {
        if (entry.second.hasBoundProjectionName() &&
            !boundsProjectionNameSet.insert(entry.second.getBoundProjectionName()).second) {
            uasserted(6624087, "Duplicate bound projection");
        }
    }

    // Assert there are no references to internally bound projections.
    for (const auto& entry : _reqMap) {
        if (boundsProjectionNameSet.find(entry.first._projectionName) !=
            boundsProjectionNameSet.cend()) {
            uasserted(6624088, "We are binding to an internal projection");
        }
    }
}

bool SargableNode::operator==(const SargableNode& other) const {
    return _reqMap == other._reqMap && _candidateIndexMap == other._candidateIndexMap &&
        _target == other._target && getChild() == other.getChild();
}

const PartialSchemaRequirements& SargableNode::getReqMap() const {
    return _reqMap;
}

const CandidateIndexMap& SargableNode::getCandidateIndexMap() const {
    return _candidateIndexMap;
}

IndexReqTarget SargableNode::getTarget() const {
    return _target;
}

BinaryJoinNode::BinaryJoinNode(JoinType joinType,
                               ProjectionNameSet correlatedProjectionNames,
                               FilterType filter,
                               ABT leftChild,
                               ABT rightChild)
    : Base(std::move(leftChild), std::move(rightChild), std::move(filter)),
      _joinType(joinType),
      _correlatedProjectionNames(std::move(correlatedProjectionNames)) {
    assertExprSort(getFilter());
    assertNodeSort(getLeftChild());
    assertNodeSort(getRightChild());
}

JoinType BinaryJoinNode::getJoinType() const {
    return _joinType;
}

const ProjectionNameSet& BinaryJoinNode::getCorrelatedProjectionNames() const {
    return _correlatedProjectionNames;
}

bool BinaryJoinNode::operator==(const BinaryJoinNode& other) const {
    return _joinType == other._joinType &&
        _correlatedProjectionNames == other._correlatedProjectionNames &&
        getLeftChild() == other.getLeftChild() && getRightChild() == other.getRightChild();
}

const ABT& BinaryJoinNode::getLeftChild() const {
    return get<0>();
}

ABT& BinaryJoinNode::getLeftChild() {
    return get<0>();
}

const ABT& BinaryJoinNode::getRightChild() const {
    return get<1>();
}

ABT& BinaryJoinNode::getRightChild() {
    return get<1>();
}

const ABT& BinaryJoinNode::getFilter() const {
    return get<2>();
}

static ABT buildHashJoinReferences(const ProjectionNameVector& leftKeys,
                                   const ProjectionNameVector& rightKeys) {
    ABTVector variables;
    for (const ProjectionName& projection : leftKeys) {
        variables.emplace_back(make<Variable>(projection));
    }
    for (const ProjectionName& projection : rightKeys) {
        variables.emplace_back(make<Variable>(projection));
    }

    return make<References>(std::move(variables));
}

HashJoinNode::HashJoinNode(JoinType joinType,
                           ProjectionNameVector leftKeys,
                           ProjectionNameVector rightKeys,
                           ABT leftChild,
                           ABT rightChild)
    : Base(std::move(leftChild),
           std::move(rightChild),
           buildHashJoinReferences(leftKeys, rightKeys)),
      _joinType(joinType),
      _leftKeys(std::move(leftKeys)),
      _rightKeys(std::move(rightKeys)) {
    uassert(
        6624089, "Invalid key sizes", !_leftKeys.empty() && _leftKeys.size() == _rightKeys.size());
    assertNodeSort(getLeftChild());
    assertNodeSort(getRightChild());
}

bool HashJoinNode::operator==(const HashJoinNode& other) const {
    return _joinType == other._joinType && _leftKeys == other._leftKeys &&
        _rightKeys == other._rightKeys && getLeftChild() == other.getLeftChild() &&
        getRightChild() == other.getRightChild();
}

JoinType HashJoinNode::getJoinType() const {
    return _joinType;
}

const ProjectionNameVector& HashJoinNode::getLeftKeys() const {
    return _leftKeys;
}

const ProjectionNameVector& HashJoinNode::getRightKeys() const {
    return _rightKeys;
}

const ABT& HashJoinNode::getLeftChild() const {
    return get<0>();
}

ABT& HashJoinNode::getLeftChild() {
    return get<0>();
}

const ABT& HashJoinNode::getRightChild() const {
    return get<1>();
}

ABT& HashJoinNode::getRightChild() {
    return get<1>();
}

MergeJoinNode::MergeJoinNode(ProjectionNameVector leftKeys,
                             ProjectionNameVector rightKeys,
                             std::vector<CollationOp> collation,
                             ABT leftChild,
                             ABT rightChild)
    : Base(std::move(leftChild),
           std::move(rightChild),
           buildHashJoinReferences(leftKeys, rightKeys)),
      _collation(std::move(collation)),
      _leftKeys(std::move(leftKeys)),
      _rightKeys(std::move(rightKeys)) {
    uassert(
        6624090, "Invalid key sizes", !_leftKeys.empty() && _leftKeys.size() == _rightKeys.size());
    uassert(6624091, "Invalid collation size", _collation.size() == _leftKeys.size());
    assertNodeSort(getLeftChild());
    assertNodeSort(getRightChild());
}

bool MergeJoinNode::operator==(const MergeJoinNode& other) const {
    return _leftKeys == other._leftKeys && _rightKeys == other._rightKeys &&
        _collation == other._collation && getLeftChild() == other.getLeftChild() &&
        getRightChild() == other.getRightChild();
}

const ProjectionNameVector& MergeJoinNode::getLeftKeys() const {
    return _leftKeys;
}

const ProjectionNameVector& MergeJoinNode::getRightKeys() const {
    return _rightKeys;
}

const std::vector<CollationOp>& MergeJoinNode::getCollation() const {
    return _collation;
}

const ABT& MergeJoinNode::getLeftChild() const {
    return get<0>();
}

ABT& MergeJoinNode::getLeftChild() {
    return get<0>();
}

const ABT& MergeJoinNode::getRightChild() const {
    return get<1>();
}

ABT& MergeJoinNode::getRightChild() {
    return get<1>();
}

/**
 * A helper that builds References object of UnionNode for reference tracking purposes.
 *
 * Example: union outputs 3 projections: A,B,C and it has 4 children. Then the References object is
 * a vector of variables A,B,C,A,B,C,A,B,C,A,B,C. One group of variables per child.
 */
static ABT buildUnionReferences(const ProjectionNameVector& names, const size_t numOfChildren) {
    ABTVector variables;
    for (size_t outerIdx = 0; outerIdx < numOfChildren; ++outerIdx) {
        for (size_t idx = 0; idx < names.size(); ++idx) {
            variables.emplace_back(make<Variable>(names[idx]));
        }
    }

    return make<References>(std::move(variables));
}

UnionNode::UnionNode(ProjectionNameVector unionProjectionNames, ABTVector children)
    : Base(std::move(children),
           buildSimpleBinder(unionProjectionNames),
           buildUnionReferences(unionProjectionNames, children.size())) {
    uassert(6624007, "Empty union", !unionProjectionNames.empty());

    for (auto& n : nodes()) {
        assertNodeSort(n);
    }
}

bool UnionNode::operator==(const UnionNode& other) const {
    return binder() == other.binder() && nodes() == other.nodes();
}

GroupByNode::GroupByNode(ProjectionNameVector groupByProjectionNames,
                         ProjectionNameVector aggregationProjectionNames,
                         ABTVector aggregationExpressions,
                         ABT child)
    : GroupByNode(std::move(groupByProjectionNames),
                  std::move(aggregationProjectionNames),
                  std::move(aggregationExpressions),
                  GroupNodeType::Complete,
                  std::move(child)) {}

GroupByNode::GroupByNode(ProjectionNameVector groupByProjectionNames,
                         ProjectionNameVector aggregationProjectionNames,
                         ABTVector aggregationExpressions,
                         GroupNodeType type,
                         ABT child)
    : Base(std::move(child),
           buildSimpleBinder(aggregationProjectionNames),
           make<References>(std::move(aggregationExpressions)),
           buildSimpleBinder(groupByProjectionNames),
           make<References>(groupByProjectionNames)),
      _type(type) {
    assertNodeSort(getChild());
    uassert(6624300,
            "Mismatched number of agg expressions and names",
            getAggregationExpressions().size() == getAggregationProjectionNames().size());
}

bool GroupByNode::operator==(const GroupByNode& other) const {
    return getAggregationProjectionNames() == other.getAggregationProjectionNames() &&
        getAggregationProjections() == other.getAggregationProjections() &&
        getGroupByProjectionNames() == other.getGroupByProjectionNames() && _type == other._type &&
        getChild() == other.getChild();
}

const ABTVector& GroupByNode::getAggregationExpressions() const {
    return get<2>().cast<References>()->nodes();
}

const ABT& GroupByNode::getChild() const {
    return get<0>();
}

ABT& GroupByNode::getChild() {
    return get<0>();
}

GroupNodeType GroupByNode::getType() const {
    return _type;
}

UnwindNode::UnwindNode(ProjectionName projectionName,
                       ProjectionName pidProjectionName,
                       const bool retainNonArrays,
                       ABT child)
    : Base(std::move(child),
           buildSimpleBinder(ProjectionNameVector{projectionName, std::move(pidProjectionName)}),
           make<References>(ProjectionNameVector{projectionName})),
      _retainNonArrays(retainNonArrays) {
    assertNodeSort(getChild());
}

bool UnwindNode::getRetainNonArrays() const {
    return _retainNonArrays;
}

const ABT& UnwindNode::getChild() const {
    return get<0>();
}

ABT& UnwindNode::getChild() {
    return get<0>();
}

bool UnwindNode::operator==(const UnwindNode& other) const {
    return binder() == other.binder() && _retainNonArrays == other._retainNonArrays &&
        getChild() == other.getChild();
}

UniqueNode::UniqueNode(ProjectionNameVector projections, ABT child)
    : Base(std::move(child), make<References>(ProjectionNameVector{projections})),
      _projections(std::move(projections)) {
    assertNodeSort(getChild());
    uassert(6624092, "Empty projections", !_projections.empty());
}

bool UniqueNode::operator==(const UniqueNode& other) const {
    return _projections == other._projections;
}

const ProjectionNameVector& UniqueNode::getProjections() const {
    return _projections;
}

const ABT& UniqueNode::getChild() const {
    return get<0>();
}

CollationNode::CollationNode(properties::CollationRequirement property, ABT child)
    : Base(std::move(child),
           buildReferences(extractReferencedColumns(properties::makePhysProps(property)))),
      _property(std::move(property)) {
    assertNodeSort(getChild());
}

const properties::CollationRequirement& CollationNode::getProperty() const {
    return _property;
}

properties::CollationRequirement& CollationNode::getProperty() {
    return _property;
}

bool CollationNode::operator==(const CollationNode& other) const {
    return _property == other._property && getChild() == other.getChild();
}

const ABT& CollationNode::getChild() const {
    return get<0>();
}

ABT& CollationNode::getChild() {
    return get<0>();
}

LimitSkipNode::LimitSkipNode(properties::LimitSkipRequirement property, ABT child)
    : Base(std::move(child)), _property(std::move(property)) {
    assertNodeSort(getChild());
}

const properties::LimitSkipRequirement& LimitSkipNode::getProperty() const {
    return _property;
}

properties::LimitSkipRequirement& LimitSkipNode::getProperty() {
    return _property;
}

bool LimitSkipNode::operator==(const LimitSkipNode& other) const {
    return _property == other._property && getChild() == other.getChild();
}

const ABT& LimitSkipNode::getChild() const {
    return get<0>();
}

ABT& LimitSkipNode::getChild() {
    return get<0>();
}

ExchangeNode::ExchangeNode(const properties::DistributionRequirement distribution, ABT child)
    : Base(std::move(child), buildReferences(distribution.getAffectedProjectionNames())),
      _distribution(std::move(distribution)) {
    assertNodeSort(getChild());
    uassert(6624008,
            "Cannot exchange towards an unknown distribution",
            distribution.getDistributionAndProjections()._type !=
                DistributionType::UnknownPartitioning);
}

bool ExchangeNode::operator==(const ExchangeNode& other) const {
    return _distribution == other._distribution && getChild() == other.getChild();
}

const ABT& ExchangeNode::getChild() const {
    return get<0>();
}

ABT& ExchangeNode::getChild() {
    return get<0>();
}

const properties::DistributionRequirement& ExchangeNode::getProperty() const {
    return _distribution;
}

properties::DistributionRequirement& ExchangeNode::getProperty() {
    return _distribution;
}

RootNode::RootNode(properties::ProjectionRequirement property, ABT child)
    : Base(std::move(child), buildReferences(property.getAffectedProjectionNames())),
      _property(std::move(property)) {
    assertNodeSort(getChild());
}

bool RootNode::operator==(const RootNode& other) const {
    return getChild() == other.getChild() && _property == other._property;
}

const properties::ProjectionRequirement& RootNode::getProperty() const {
    return _property;
}

const ABT& RootNode::getChild() const {
    return get<0>();
}

ABT& RootNode::getChild() {
    return get<0>();
}

}  // namespace mongo::optimizer
