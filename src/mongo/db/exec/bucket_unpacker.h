/**
 *    Copyright (C) 2021-present MongoDB, Inc.
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


#include <algorithm>
#include <set>

#include "mongo/bson/bsonobj.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/matcher/expression.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/timeseries/timeseries_constants.h"

namespace mongo {
/**
 * Carries parameters for unpacking a bucket. The order of operations applied to determine which
 * fields are in the final document are:
 * If we are in include mode:
 *   1. Unpack all fields from the bucket.
 *   2. Remove any fields not in _fieldSet, since we are in include mode.
 *   3. Add fields from _computedMetaProjFields.
 * If we are in exclude mode:
 *   1. Unpack all fields from the bucket.
 *   2. Add fields from _computedMetaProjFields.
 *   3. Remove any fields in _fieldSet, since we are in exclude mode.
 */
class BucketSpec {
public:
    // When unpackin buckets with kInclude we must produce measurements that contain the
    // set of fields. Otherwise, if the kExclude option is used, the measurements will include the
    // set difference between all fields in the bucket and the provided fields.
    enum class Behavior { kInclude, kExclude };

    BucketSpec() = default;
    BucketSpec(const std::string& timeField,
               const boost::optional<std::string>& metaField,
               const std::set<std::string>& fields = {},
               Behavior behavior = Behavior::kExclude,
               const std::set<std::string>& computedProjections = {},
               bool usesExtendedRange = false);
    BucketSpec(const BucketSpec&);
    BucketSpec(BucketSpec&&);

    BucketSpec& operator=(const BucketSpec&);

    // The user-supplied timestamp field name specified during time-series collection creation.
    void setTimeField(std::string&& field);
    const std::string& timeField() const;
    HashedFieldName timeFieldHashed() const;

    // An optional user-supplied metadata field name specified during time-series collection
    // creation. This field name is used during materialization of metadata fields of a measurement
    // after unpacking.
    void setMetaField(boost::optional<std::string>&& field);
    const boost::optional<std::string>& metaField() const;
    boost::optional<HashedFieldName> metaFieldHashed() const;

    void setFieldSet(std::set<std::string>& fieldSet) {
        _fieldSet = std::move(fieldSet);
    }

    void addIncludeExcludeField(const StringData& field) {
        _fieldSet.emplace(field);
    }

    void removeIncludeExcludeField(const std::string& field) {
        _fieldSet.erase(field);
    }

    const std::set<std::string>& fieldSet() const {
        return _fieldSet;
    }

    void setBehavior(Behavior behavior) {
        _behavior = behavior;
    }

    Behavior behavior() const {
        return _behavior;
    }

    void addComputedMetaProjFields(const StringData& field) {
        _computedMetaProjFields.emplace(field);
    }

    const std::set<std::string>& computedMetaProjFields() const {
        return _computedMetaProjFields;
    }

    void eraseFromComputedMetaProjFields(const std::string& field) {
        _computedMetaProjFields.erase(field);
    }

    void setUsesExtendedRange(bool usesExtendedRange) {
        _usesExtendedRange = usesExtendedRange;
    }

    bool usesExtendedRange() const {
        return _usesExtendedRange;
    }

    // Returns whether 'field' depends on a pushed down $addFields or computed $project.
    bool fieldIsComputed(StringData field) const;

    // Says what to do when an event-level predicate cannot be mapped to a bucket-level predicate.
    enum class IneligiblePredicatePolicy {
        // When optimizing a query, it's fine if some predicates can't be pushed down. We'll still
        // run the predicate after unpacking, so the results will be correct.
        kIgnore,
        // When creating a partial index, it's misleading if we can't handle a predicate: the user
        // expects every predicate in the partialFilterExpression to contribute, somehow, to making
        // the index smaller.
        kError,
    };

    struct BucketPredicate {
        // A loose predicate is a predicate which returns true when any measures of a bucket
        // matches.
        std::unique_ptr<MatchExpression> loosePredicate;

        // A tight predicate is a predicate which returns true when all measures of a bucket
        // matches.
        std::unique_ptr<MatchExpression> tightPredicate;
    };

    /**
     * Takes a predicate after $_internalUnpackBucket as an argument and attempts to rewrite it as
     * new predicates on the 'control' field. There will be a 'loose' predicate that will match if
     * some of the event field matches, also a 'tight' predicate that will match if all of the event
     * field matches.
     *
     * For example, the event level predicate {a: {$gt: 5}} will generate the loose predicate
     * {control.max.a: {$_internalExprGt: 5}}. The loose predicate will be added before the
     * $_internalUnpackBucket stage to filter out buckets with no match.
     *
     * Ideally, we'd like to add a tight predicate such as {control.min.a: {$_internalExprGt: 5}} to
     * evaluate the filter on bucket level to avoid unnecessary event level evaluation. However, a
     * bucket might contain events with missing fields that are skipped when computing the controls,
     * so in reality we only add a tight predicate on timeField which is required to exist.
     *
     * If the original predicate is on the bucket's timeField we may also create a new loose
     * predicate on the '_id' field (as it incorporates min time for the bucket) to assist in index
     * utilization. For example, the predicate {time: {$lt: new Date(...)}} will generate the
     * following predicate:
     * {$and: [
     *      {_id: {$lt: ObjectId(...)}},
     *      {control.min.time: {$_internalExprLt: new Date(...)}}
     * ]}
     *
     * If the provided predicate is ineligible for this mapping and using
     * IneligiblePredicatePolicy::kIgnore, both loose and tight predicates will be set to nullptr.
     * When using IneligiblePredicatePolicy::kError it raises a user error.
     */
    static BucketPredicate createPredicatesOnBucketLevelField(
        const MatchExpression* matchExpr,
        const BucketSpec& bucketSpec,
        int bucketMaxSpanSeconds,
        ExpressionContext::CollationMatchesDefault collationMatchesDefault,
        const boost::intrusive_ptr<ExpressionContext>& pExpCtx,
        bool haveComputedMetaField,
        bool includeMetaField,
        bool assumeNoMixedSchemaData,
        IneligiblePredicatePolicy policy);

    /**
     * Converts an event-level predicate to a bucket-level predicate, such that
     *
     *     {$unpackBucket ...} {$match: <event-level predicate>}
     *
     * gives the same result as
     *
     *     {$match: <bucket-level predict>} {$unpackBucket ...} {$match: <event-level predicate>}
     *
     * This means the bucket-level predicate must include every bucket that might contain an event
     * matching the event-level predicate.
     *
     * This helper is used when creating a partial index on a time-series collection: logically,
     * we index only events that match the event-level partialFilterExpression, but physically we
     * index any bucket that matches the bucket-level partialFilterExpression.
     *
     * When using IneligiblePredicatePolicy::kIgnore, if the predicate can't be pushed down, it
     * returns null. When using IneligiblePredicatePolicy::kError it raises a user error.
     */
    static BSONObj pushdownPredicate(
        const boost::intrusive_ptr<ExpressionContext>& expCtx,
        const TimeseriesOptions& tsOptions,
        ExpressionContext::CollationMatchesDefault collationMatchesDefault,
        const BSONObj& predicate,
        bool haveComputedMetaField,
        bool includeMetaField,
        bool assumeNoMixedSchemaData,
        IneligiblePredicatePolicy policy);

    bool includeMinTimeAsMetadata = false;
    bool includeMaxTimeAsMetadata = false;

private:
    // The set of field names in the data region that should be included or excluded.
    std::set<std::string> _fieldSet;
    Behavior _behavior = Behavior::kExclude;

    // Set of computed meta field projection names. Added at the end of materialized
    // measurements.
    std::set<std::string> _computedMetaProjFields;

    std::string _timeField;
    boost::optional<HashedFieldName> _timeFieldHashed;

    boost::optional<std::string> _metaField = boost::none;
    boost::optional<HashedFieldName> _metaFieldHashed = boost::none;
    bool _usesExtendedRange = false;
};

/**
 * BucketUnpacker will unpack bucket fields for metadata and the provided fields.
 */
class BucketUnpacker {
public:
    /**
     * Returns the number of measurements in the bucket in O(1) time.
     */
    static int computeMeasurementCount(const BSONObj& bucket, StringData timeField);

    // Set of field names reserved for time-series buckets.
    static const std::set<StringData> reservedBucketFieldNames;

    BucketUnpacker();
    BucketUnpacker(BucketSpec spec);
    BucketUnpacker(const BucketUnpacker& other) = delete;
    BucketUnpacker(BucketUnpacker&& other);
    ~BucketUnpacker();
    BucketUnpacker& operator=(const BucketUnpacker& rhs) = delete;
    BucketUnpacker& operator=(BucketUnpacker&& rhs);

    /**
     * This method will continue to materialize Documents until the bucket is exhausted. A
     * precondition of this method is that 'hasNext()' must be true.
     */
    Document getNext();

    /**
     * Similar to the previous method, but return a BSON object instead.
     */
    BSONObj getNextBson();

    /**
     * This method will extract the j-th measurement from the bucket. A precondition of this method
     * is that j >= 0 && j <= the number of measurements within the underlying bucket.
     */
    Document extractSingleMeasurement(int j);

    /**
     * Returns true if there is more data to fetch, is the precondition for 'getNext'.
     */
    bool hasNext() const {
        return _hasNext;
    }

    /**
     * Makes a copy of this BucketUnpacker that is detached from current bucket. The new copy needs
     * to be reset to a new bucket object to perform unpacking.
     */
    BucketUnpacker copy() const {
        BucketUnpacker unpackerCopy;
        unpackerCopy._spec = _spec;
        unpackerCopy._includeMetaField = _includeMetaField;
        unpackerCopy._includeTimeField = _includeTimeField;
        return unpackerCopy;
    }

    /**
     * This resets the unpacker to prepare to unpack a new bucket described by the given document.
     */
    void reset(BSONObj&& bucket, bool bucketMatchedQuery = false);

    BucketSpec::Behavior behavior() const {
        return _spec.behavior();
    }

    const BucketSpec& bucketSpec() const {
        return _spec;
    }

    const BSONObj& bucket() const {
        return _bucket;
    }

    bool bucketMatchedQuery() const {
        return _bucketMatchedQuery;
    }

    bool includeMetaField() const {
        return _includeMetaField;
    }

    bool includeTimeField() const {
        return _includeTimeField;
    }

    int32_t numberOfMeasurements() const {
        return _numberOfMeasurements;
    }

    bool includeMinTimeAsMetadata() const {
        return _includeMinTimeAsMetadata;
    }

    bool includeMaxTimeAsMetadata() const {
        return _includeMaxTimeAsMetadata;
    }

    const std::string& getTimeField() const {
        return _spec.timeField();
    }

    const boost::optional<std::string>& getMetaField() const {
        return _spec.metaField();
    }

    std::string getMinField(StringData field) const {
        return std::string{timeseries::kControlMinFieldNamePrefix} + field;
    }

    std::string getMaxField(StringData field) const {
        return std::string{timeseries::kControlMaxFieldNamePrefix} + field;
    }

    void setBucketSpec(BucketSpec&& bucketSpec);
    void setIncludeMinTimeAsMetadata();
    void setIncludeMaxTimeAsMetadata();

    // Add computed meta projection names to the bucket specification.
    void addComputedMetaProjFields(const std::vector<StringData>& computedFieldNames);

    // Fill _spec.unpackFieldsToIncludeExclude with final list of fields to include/exclude during
    // unpacking. Only calculates the list the first time it is called.
    const std::set<std::string>& fieldsToIncludeExcludeDuringUnpack();

    class UnpackingImpl;

private:
    // Determines if timestamp values should be included in the materialized measurements.
    void determineIncludeTimeField();

    // Removes metaField from the field set and determines whether metaField should be
    // included in the materialized measurements.
    void eraseMetaFromFieldSetAndDetermineIncludeMeta();

    // Erase computed meta projection fields if they are present in the exclusion field set.
    void eraseExcludedComputedMetaProjFields();

    BucketSpec _spec;

    std::unique_ptr<UnpackingImpl> _unpackingImpl;

    bool _hasNext = false;

    // A flag used to mark that the entire bucket matches the following $match predicate.
    bool _bucketMatchedQuery = false;

    // A flag used to mark that the timestamp value should be materialized in measurements.
    bool _includeTimeField{false};

    // A flag used to mark that a bucket's metadata value should be materialized in measurements.
    bool _includeMetaField{false};

    // A flag used to mark that a bucket's min time should be materialized as metadata.
    bool _includeMinTimeAsMetadata{false};

    // A flag used to mark that a bucket's max time should be materialized as metadata.
    bool _includeMaxTimeAsMetadata{false};

    // The bucket being unpacked.
    BSONObj _bucket;

    // Since the metadata value is the same across all materialized measurements we can cache the
    // metadata Value in the reset phase and use it to materialize the metadata in each
    // measurement.
    Value _metaValue;

    BSONElement _metaBSONElem;

    // Since the bucket min time is the same across all materialized measurements, we can cache the
    // value in the reset phase and use it to materialize as a metadata field in each measurement
    // if required by the pipeline.
    boost::optional<Date_t> _minTime;

    // Since the bucket max time is the same across all materialized measurements, we can cache the
    // value in the reset phase and use it to materialize as a metadata field in each measurement
    // if required by the pipeline.
    boost::optional<Date_t> _maxTime;

    // Map <name, BSONElement> for the computed meta field projections. Updated for
    // every bucket upon reset().
    stdx::unordered_map<std::string, BSONElement> _computedMetaProjections;

    // The number of measurements in the bucket.
    int32_t _numberOfMeasurements = 0;

    // Final list of fields to include/exclude during unpacking. This is computed once during the
    // first doGetNext call so we don't have to recalculate every time we reach a new bucket.
    boost::optional<std::set<std::string>> _unpackFieldsToIncludeExclude = boost::none;
};

/**
 * Determines if an arbitrary field should be included in the materialized measurements.
 */
inline bool determineIncludeField(StringData fieldName,
                                  BucketSpec::Behavior unpackerBehavior,
                                  const std::set<std::string>& unpackFieldsToIncludeExclude) {
    const bool isInclude = unpackerBehavior == BucketSpec::Behavior::kInclude;
    const bool unpackFieldsContains = unpackFieldsToIncludeExclude.find(fieldName.toString()) !=
        unpackFieldsToIncludeExclude.cend();
    return isInclude == unpackFieldsContains;
}
}  // namespace mongo
