/**
 *    Copyright (C) 2020-present MongoDB, Inc.
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

#include "mongo/db/exec/bucket_unpacker.h"

#include <algorithm>

#include "mongo/bson/util/bsoncolumn.h"
#include "mongo/db/matcher/expression.h"
#include "mongo/db/matcher/expression_algo.h"
#include "mongo/db/matcher/expression_always_boolean.h"
#include "mongo/db/matcher/expression_expr.h"
#include "mongo/db/matcher/expression_geo.h"
#include "mongo/db/matcher/expression_internal_bucket_geo_within.h"
#include "mongo/db/matcher/expression_internal_expr_comparison.h"
#include "mongo/db/matcher/expression_parser.h"
#include "mongo/db/matcher/expression_tree.h"
#include "mongo/db/matcher/extensions_callback_noop.h"
#include "mongo/db/matcher/rewrite_expr.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/db/timeseries/timeseries_options.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery

#include "mongo/logv2/log.h"

namespace mongo {

using IneligiblePredicatePolicy = BucketSpec::IneligiblePredicatePolicy;

bool BucketSpec::fieldIsComputed(StringData field) const {
    return std::any_of(
        _computedMetaProjFields.begin(), _computedMetaProjFields.end(), [&](auto& s) {
            return s == field || expression::isPathPrefixOf(field, s) ||
                expression::isPathPrefixOf(s, field);
        });
}

namespace {

constexpr long long max32BitEpochMillis =
    static_cast<long long>(std::numeric_limits<uint32_t>::max()) * 1000;

/**
 * Creates an ObjectId initialized with an appropriate timestamp corresponding to 'rhs' and
 * returns it as a Value.
 */
template <typename MatchType>
auto constructObjectIdValue(const BSONElement& rhs, int bucketMaxSpanSeconds) {
    // Indicates whether to initialize an ObjectId with a max or min value for the non-date bytes.
    enum class OIDInit : bool { max, min };
    // Make an ObjectId cooresponding to a date value. As a conversion from date to ObjectId will
    // truncate milliseconds, we round up when needed to prevent missing results.
    auto makeDateOID = [](auto&& date, auto&& maxOrMin, bool roundMillisUpToSecond = false) {
        if (roundMillisUpToSecond && (date.toMillisSinceEpoch() % 1000 != 0)) {
            date += Seconds{1};
        }

        auto oid = OID{};
        oid.init(date, maxOrMin == OIDInit::max);
        return oid;
    };
    // Make an ObjectId corresponding to a date value adjusted by the max bucket value for the
    // time series view that this query operates on. This predicate can be used in a comparison
    // to gauge a max value for a given bucket, rather than a min value.
    auto makeMaxAdjustedDateOID = [&](auto&& date, auto&& maxOrMin) {
        // Ensure we don't underflow.
        if (date.toDurationSinceEpoch() >= Seconds{bucketMaxSpanSeconds})
            // Subtract max bucket range.
            return makeDateOID(date - Seconds{bucketMaxSpanSeconds}, maxOrMin);
        else
            // Since we're out of range, just make a predicate that is true for all dates.
            // We'll never use an OID for a date < 0 due to OID range limitations, so we set the
            // minimum date to 0.
            return makeDateOID(Date_t::fromMillisSinceEpoch(0LL), OIDInit::min);
    };

    // Because the OID timestamp is only 4 bytes, we can't convert larger dates
    invariant(rhs.date().toMillisSinceEpoch() >= 0LL);
    invariant(rhs.date().toMillisSinceEpoch() <= max32BitEpochMillis);

    // An ObjectId consists of a 4-byte timestamp, as well as a unique value and a counter, thus
    // two ObjectIds initialized with the same date will have different values. To ensure that we
    // do not incorrectly include or exclude any buckets, depending on the operator we will
    // construct either the largest or the smallest ObjectId possible with the corresponding date.
    // If the query operand is not of type Date, the original query will not match on any documents
    // because documents in a time-series collection must have a timeField of type Date. We will
    // make this case faster by keeping the ObjectId as the lowest or highest possible value so as
    // to eliminate all buckets.
    if constexpr (std::is_same_v<MatchType, LTMatchExpression>) {
        return Value{makeDateOID(rhs.date(), OIDInit::min, true /*roundMillisUpToSecond*/)};
    } else if constexpr (std::is_same_v<MatchType, LTEMatchExpression>) {
        return Value{makeDateOID(rhs.date(), OIDInit::max, true /*roundMillisUpToSecond*/)};
    } else if constexpr (std::is_same_v<MatchType, GTMatchExpression>) {
        return Value{makeMaxAdjustedDateOID(rhs.date(), OIDInit::max)};
    } else if constexpr (std::is_same_v<MatchType, GTEMatchExpression>) {
        return Value{makeMaxAdjustedDateOID(rhs.date(), OIDInit::min)};
    }
    MONGO_UNREACHABLE_TASSERT(5756800);
}

/**
 * Makes a disjunction of the given predicates.
 *
 * - The result is non-null; it may be an OrMatchExpression with zero children.
 * - Any trivially-false arguments are omitted.
 * - If only one argument is nontrivial, returns that argument rather than adding an extra
 *   OrMatchExpression around it.
 */
std::unique_ptr<MatchExpression> makeOr(std::vector<std::unique_ptr<MatchExpression>> predicates) {
    std::vector<std::unique_ptr<MatchExpression>> nontrivial;
    for (auto&& p : predicates) {
        if (!p->isTriviallyFalse())
            nontrivial.push_back(std::move(p));
    }

    if (nontrivial.size() == 1)
        return std::move(nontrivial[0]);

    return std::make_unique<OrMatchExpression>(std::move(nontrivial));
}

BucketSpec::BucketPredicate handleIneligible(IneligiblePredicatePolicy policy,
                                             const MatchExpression* matchExpr,
                                             StringData message) {
    switch (policy) {
        case IneligiblePredicatePolicy::kError:
            uasserted(
                5916301,
                "Error translating non-metadata time-series predicate to operate on buckets: " +
                    message + ": " + matchExpr->serialize().toString());
        case IneligiblePredicatePolicy::kIgnore:
            return {};
    }
    MONGO_UNREACHABLE_TASSERT(5916307);
}

/*
 * Creates a predicate that ensures that if there exists a subpath of matchExprPath such that the
 * type of `control.min.subpath` is not the same as `control.max.subpath` then we will match that
 * document.
 *
 * However, if the buckets collection has no mixed-schema data then this type-equality predicate is
 * unnecessary. In that case this function returns an empty, always-true predicate.
 */
std::unique_ptr<MatchExpression> createTypeEqualityPredicate(
    boost::intrusive_ptr<ExpressionContext> pExpCtx,
    const StringData& matchExprPath,
    bool assumeNoMixedSchemaData) {

    std::vector<std::unique_ptr<MatchExpression>> typeEqualityPredicates;

    if (assumeNoMixedSchemaData)
        return makeOr(std::move(typeEqualityPredicates));

    FieldPath matchExprField(matchExprPath);
    using namespace timeseries;

    // Assume that we're generating a predicate on "a.b"
    for (size_t i = 0; i < matchExprField.getPathLength(); i++) {
        auto minPath = std::string{kControlMinFieldNamePrefix} + matchExprField.getSubpath(i);
        auto maxPath = std::string{kControlMaxFieldNamePrefix} + matchExprField.getSubpath(i);

        // This whole block adds
        // {$expr: {$ne: [{$type: "$control.min.a"}, {$type: "$control.max.a"}]}}
        // in order to ensure that the type of `control.min.a` and `control.max.a` are the same.

        // This produces {$expr: ... }
        typeEqualityPredicates.push_back(std::make_unique<ExprMatchExpression>(
            // This produces {$ne: ... }
            make_intrusive<ExpressionCompare>(
                pExpCtx.get(),
                ExpressionCompare::CmpOp::NE,
                // This produces [...]
                makeVector<boost::intrusive_ptr<Expression>>(
                    // This produces {$type: ... }
                    make_intrusive<ExpressionType>(
                        pExpCtx.get(),
                        // This produces [...]
                        makeVector<boost::intrusive_ptr<Expression>>(
                            // This produces "$control.min.a"
                            ExpressionFieldPath::createPathFromString(
                                pExpCtx.get(), minPath, pExpCtx->variablesParseState))),
                    // This produces {$type: ... }
                    make_intrusive<ExpressionType>(
                        pExpCtx.get(),
                        // This produces [...]
                        makeVector<boost::intrusive_ptr<Expression>>(
                            // This produces "$control.max.a"
                            ExpressionFieldPath::createPathFromString(
                                pExpCtx.get(), maxPath, pExpCtx->variablesParseState))))),
            pExpCtx));
    }
    return makeOr(std::move(typeEqualityPredicates));
}

// Checks for the situations when it's not possible to create a bucket-level predicate (against the
// computed control values) for the given event-level predicate ('matchExpr').
boost::optional<StringData> checkComparisonPredicateEligibility(
    const ComparisonMatchExpressionBase* matchExpr,
    const StringData matchExprPath,
    const BSONElement& matchExprData,
    const BucketSpec& bucketSpec,
    ExpressionContext::CollationMatchesDefault collationMatchesDefault) {
    using namespace timeseries;
    // The control field's min and max are chosen using a field-order insensitive comparator, while
    // MatchExpressions use a comparator that treats field-order as significant. Because of this we
    // will not perform this optimization on queries with operands of compound types.
    if (matchExprData.type() == BSONType::Object || matchExprData.type() == BSONType::Array)
        return "operand can't be an object or array"_sd;

    const auto isTimeField = (matchExprPath == bucketSpec.timeField());

    // A bucket might contain events with the missing fields. These events aren't taken in account
    // when computing the control values for those fields. This design has two repercussions:
    // 1. MatchExpressions have special comparison semantics regarding null, in that {$eq: null}
    //    will match all documents where the field is either null or missing. This semantics cannot
    //    be represented in terms of comparisons against the min/max control values.
    // 2. Non-type-bracketing predicates, such as {$expr: {$lt(e): ['$x', 42]}} should evaluate to
    //    "true" if "x" is missing, which also cannot be represented as a bucket-level predicate.
    //    1) time field cannot be empty.
    //    2) the only type less than null is MinKey, which is internal, so we don't need to guard
    //       GT and GTE.
    //    3) for the buckets that might have mixed schema data, we'll compare the types of min and
    //       max when _creating_ the bucket-level predicate (that check won't help with missing).
    if (matchExprData.type() == BSONType::jstNULL)
        return "can't handle comparison to null"_sd;
    if (!isTimeField &&
        (matchExpr->matchType() == MatchExpression::INTERNAL_EXPR_LTE ||
         matchExpr->matchType() == MatchExpression::INTERNAL_EXPR_LT)) {
        return "can't handle a non-type-bracketing LT or LTE comparisons"_sd;
    }

    // The control field's min and max are chosen based on the collation of the collection. If the
    // query's collation does not match the collection's collation and the query operand is a
    // string or compound type (skipped above) we will not perform this optimization.
    if (collationMatchesDefault == ExpressionContext::CollationMatchesDefault::kNo &&
        matchExprData.type() == BSONType::String) {
        return "can't handle string comparison with a non-default collation"_sd;
    }

    // This function only handles time and measurement predicates--not metadata.
    if (bucketSpec.metaField() &&
        (matchExprPath == bucketSpec.metaField().get() ||
         expression::isPathPrefixOf(bucketSpec.metaField().get(), matchExprPath))) {
        tasserted(
            6707200,
            str::stream() << "createComparisonPredicate() does not handle metadata predicates: "
                          << matchExpr);
    }

    // We must avoid mapping predicates on fields computed via $addFields or a computed $project.
    if (bucketSpec.fieldIsComputed(matchExprPath.toString())) {
        return "can't handle a computed field"_sd;
    }

    // We must avoid mapping predicates on fields removed by $project.
    if (!determineIncludeField(matchExprPath, bucketSpec.behavior(), bucketSpec.fieldSet())) {
        return "can't handle a field removed by projection"_sd;
    }

    if (isTimeField && matchExprData.type() != BSONType::Date) {
        // TODO SERVER-84207: right now we will end up unpacking everything and applying the event
        // filter, which indeed would be either trivially true or trivially false but it won't be
        // optimized away.
        return "can't handle comparison of time field to a non-Date type"_sd;
    }

    return boost::none;
}

std::unique_ptr<MatchExpression> createComparisonPredicate(
    const ComparisonMatchExpressionBase* matchExpr,
    const BucketSpec& bucketSpec,
    int bucketMaxSpanSeconds,
    ExpressionContext::CollationMatchesDefault collationMatchesDefault,
    boost::intrusive_ptr<ExpressionContext> pExpCtx,
    bool haveComputedMetaField,
    bool includeMetaField,
    bool assumeNoMixedSchemaData,
    IneligiblePredicatePolicy policy) {
    using namespace timeseries;
    const auto matchExprPath = matchExpr->path();
    const auto matchExprData = matchExpr->getData();

    const auto error = checkComparisonPredicateEligibility(
        matchExpr, matchExprPath, matchExprData, bucketSpec, collationMatchesDefault);
    if (error) {
        return handleIneligible(policy, matchExpr, *error).loosePredicate;
    }

    const auto isTimeField = (matchExprPath == bucketSpec.timeField());
    auto minPath = std::string{kControlMinFieldNamePrefix} + matchExprPath;
    const StringData minPathStringData(minPath);
    auto maxPath = std::string{kControlMaxFieldNamePrefix} + matchExprPath;
    const StringData maxPathStringData(maxPath);

    BSONObj minTime;
    BSONObj maxTime;
    bool dateIsExtended = false;
    if (isTimeField) {
        auto timeField = matchExprData.Date();
        minTime = BSON("" << timeField - Seconds(bucketMaxSpanSeconds));
        maxTime = BSON("" << timeField + Seconds(bucketMaxSpanSeconds));

        // The date is in the "extended" range if it doesn't fit into the bottom
        // 32 bits.
        long long timestamp = timeField.toMillisSinceEpoch();
        dateIsExtended = timestamp < 0LL || timestamp > max32BitEpochMillis;
    }

    switch (matchExpr->matchType()) {
        case MatchExpression::EQ:
        case MatchExpression::INTERNAL_EXPR_EQ:
            // For $eq, make both a $lte against 'control.min' and a $gte predicate against
            // 'control.max'.
            //
            // If the comparison is against the 'time' field and we haven't stored a time outside of
            // the 32 bit range, include a predicate against the _id field which is converted to
            // the maximum for the corresponding range of ObjectIds and
            // is adjusted by the max range for a bucket to approximate the max bucket value given
            // the min. Also include a predicate against the _id field which is converted to the
            // minimum for the range of ObjectIds corresponding to the given date. In
            // addition, we include a {'control.min' : {$gte: 'time - bucketMaxSpanSeconds'}} and
            // a {'control.max' : {$lte: 'time + bucketMaxSpanSeconds'}} predicate which will be
            // helpful in reducing bounds for index scans on 'time' field and routing on mongos.
            //
            // The same procedure applies to aggregation expressions of the form
            // {$expr: {$eq: [...]}} that can be rewritten to use $_internalExprEq.
            if (!isTimeField) {
                return makeOr(makeVector<std::unique_ptr<MatchExpression>>(
                    makePredicate(
                        MatchExprPredicate<InternalExprLTEMatchExpression>(minPath, matchExprData),
                        MatchExprPredicate<InternalExprGTEMatchExpression>(maxPath, matchExprData)),
                    createTypeEqualityPredicate(pExpCtx, matchExprPath, assumeNoMixedSchemaData)));
            } else if (bucketSpec.usesExtendedRange()) {
                return makePredicate(
                    MatchExprPredicate<InternalExprLTEMatchExpression>(minPath, matchExprData),
                    MatchExprPredicate<InternalExprGTEMatchExpression>(minPath,
                                                                       minTime.firstElement()),
                    MatchExprPredicate<InternalExprGTEMatchExpression>(maxPath, matchExprData),
                    MatchExprPredicate<InternalExprLTEMatchExpression>(maxPath,
                                                                       maxTime.firstElement()));
            } else if (dateIsExtended) {
                // Since by this point we know that no time value has been inserted which is
                // outside the epoch range, we know that no document can meet this criteria
                return std::make_unique<AlwaysFalseMatchExpression>();
            } else {
                return makePredicate(
                    MatchExprPredicate<InternalExprLTEMatchExpression>(minPath, matchExprData),
                    MatchExprPredicate<InternalExprGTEMatchExpression>(minPath,
                                                                       minTime.firstElement()),
                    MatchExprPredicate<InternalExprGTEMatchExpression>(maxPath, matchExprData),
                    MatchExprPredicate<InternalExprLTEMatchExpression>(maxPath,
                                                                       maxTime.firstElement()),
                    MatchExprPredicate<LTEMatchExpression, Value>(
                        kBucketIdFieldName,
                        constructObjectIdValue<LTEMatchExpression>(matchExprData,
                                                                   bucketMaxSpanSeconds)),
                    MatchExprPredicate<GTEMatchExpression, Value>(
                        kBucketIdFieldName,
                        constructObjectIdValue<GTEMatchExpression>(matchExprData,
                                                                   bucketMaxSpanSeconds)));
            }
            MONGO_UNREACHABLE_TASSERT(6646903);

        case MatchExpression::GT:
        case MatchExpression::INTERNAL_EXPR_GT:
            // For $gt, make a $gt predicate against 'control.max'. In addition, if the comparison
            // is against the 'time' field, and the collection doesn't contain times outside the
            // 32 bit range, include a predicate against the _id field which is converted to the
            // maximum for the corresponding range of ObjectIds and is adjusted by the max range
            // for a bucket to approximate the max bucket value given the min.
            //
            // In addition, we include a {'control.min' : {$gt: 'time - bucketMaxSpanSeconds'}}
            // predicate which will be helpful in reducing bounds for index scans on 'time' field
            // and routing on mongos.
            //
            // The same procedure applies to aggregation expressions of the form
            // {$expr: {$gt: [...]}} that can be rewritten to use $_internalExprGt.
            if (!isTimeField) {
                return makeOr(makeVector<std::unique_ptr<MatchExpression>>(
                    std::make_unique<InternalExprGTMatchExpression>(maxPath, matchExprData),
                    createTypeEqualityPredicate(pExpCtx, matchExprPath, assumeNoMixedSchemaData)));
            } else if (bucketSpec.usesExtendedRange()) {
                return makePredicate(
                    MatchExprPredicate<InternalExprGTMatchExpression>(maxPath, matchExprData),
                    MatchExprPredicate<InternalExprGTMatchExpression>(minPath,
                                                                      minTime.firstElement()));
            } else if (matchExprData.Date().toMillisSinceEpoch() < 0LL) {
                // Since by this point we know that no time value has been inserted < 0,
                // every document must meet this criteria
                return std::make_unique<AlwaysTrueMatchExpression>();
            } else if (matchExprData.Date().toMillisSinceEpoch() > max32BitEpochMillis) {
                // Since by this point we know that no time value has been inserted >
                // max32BitEpochMillis, we know that no document can meet this criteria
                return std::make_unique<AlwaysFalseMatchExpression>();
            } else {
                return makePredicate(
                    MatchExprPredicate<InternalExprGTMatchExpression>(maxPath, matchExprData),
                    MatchExprPredicate<InternalExprGTMatchExpression>(minPath,
                                                                      minTime.firstElement()),
                    MatchExprPredicate<GTMatchExpression, Value>(
                        kBucketIdFieldName,
                        constructObjectIdValue<GTMatchExpression>(matchExprData,
                                                                  bucketMaxSpanSeconds)));
            }
            MONGO_UNREACHABLE_TASSERT(6646904);

        case MatchExpression::GTE:
        case MatchExpression::INTERNAL_EXPR_GTE:
            // For $gte, make a $gte predicate against 'control.max'. In addition, if the comparison
            // is against the 'time' field, and the collection doesn't contain times outside the
            // 32 bit range, include a predicate against the _id field which is
            // converted to the minimum for the corresponding range of ObjectIds and is adjusted
            // by the max range for a bucket to approximate the max bucket value given the min. In
            // addition, we include a {'control.min' : {$gte: 'time - bucketMaxSpanSeconds'}}
            // predicate which will be helpful in reducing bounds for index scans on 'time' field
            // and routing on mongos.
            //
            // The same procedure applies to aggregation expressions of the form
            // {$expr: {$gte: [...]}} that can be rewritten to use $_internalExprGte.
            if (!isTimeField) {
                return makeOr(makeVector<std::unique_ptr<MatchExpression>>(
                    std::make_unique<InternalExprGTEMatchExpression>(maxPath, matchExprData),
                    createTypeEqualityPredicate(pExpCtx, matchExprPath, assumeNoMixedSchemaData)));
            } else if (bucketSpec.usesExtendedRange()) {
                return makePredicate(
                    MatchExprPredicate<InternalExprGTEMatchExpression>(maxPath, matchExprData),
                    MatchExprPredicate<InternalExprGTEMatchExpression>(minPath,
                                                                       minTime.firstElement()));
            } else if (matchExprData.Date().toMillisSinceEpoch() < 0LL) {
                // Since by this point we know that no time value has been inserted < 0,
                // every document must meet this criteria
                return std::make_unique<AlwaysTrueMatchExpression>();
            } else if (matchExprData.Date().toMillisSinceEpoch() > max32BitEpochMillis) {
                // Since by this point we know that no time value has been inserted > 0xffffffff,
                // we know that no value can meet this criteria
                return std::make_unique<AlwaysFalseMatchExpression>();
            } else {
                return makePredicate(
                    MatchExprPredicate<InternalExprGTEMatchExpression>(maxPath, matchExprData),
                    MatchExprPredicate<InternalExprGTEMatchExpression>(minPath,
                                                                       minTime.firstElement()),
                    MatchExprPredicate<GTEMatchExpression, Value>(
                        kBucketIdFieldName,
                        constructObjectIdValue<GTEMatchExpression>(matchExprData,
                                                                   bucketMaxSpanSeconds)));
            }
            MONGO_UNREACHABLE_TASSERT(6646905);

        case MatchExpression::LT:
        case MatchExpression::INTERNAL_EXPR_LT:
            // For $lt, make a $lt predicate against 'control.min'. In addition, if the comparison
            // is against the 'time' field, include a predicate against the _id field which is
            // converted to the minimum for the corresponding range of ObjectIds, unless the
            // collection contain extended range dates which won't fit int the 32 bits allocated
            // for _id.
            //
            // In addition, we include a {'control.max' : {$lt: 'time + bucketMaxSpanSeconds'}}
            // predicate which will be helpful in reducing bounds for index scans on 'time' field
            // and routing on mongos.
            //
            // The same procedure applies to aggregation expressions of the form
            // {$expr: {$lt: [...]}} that can be rewritten to use $_internalExprLt.
            if (!isTimeField) {
                return makeOr(makeVector<std::unique_ptr<MatchExpression>>(
                    std::make_unique<InternalExprLTMatchExpression>(minPath, matchExprData),
                    createTypeEqualityPredicate(pExpCtx, matchExprPath, assumeNoMixedSchemaData)));
            } else if (bucketSpec.usesExtendedRange()) {
                return makePredicate(
                    MatchExprPredicate<InternalExprLTMatchExpression>(minPath, matchExprData),
                    MatchExprPredicate<InternalExprLTMatchExpression>(maxPath,
                                                                      maxTime.firstElement()));
            } else if (matchExprData.Date().toMillisSinceEpoch() < 0LL) {
                // Since by this point we know that no time value has been inserted < 0,
                // we know that no document can meet this criteria
                return std::make_unique<AlwaysFalseMatchExpression>();
            } else if (matchExprData.Date().toMillisSinceEpoch() > max32BitEpochMillis) {
                // Since by this point we know that no time value has been inserted > 0xffffffff
                // every time value must be less than this value
                return std::make_unique<AlwaysTrueMatchExpression>();
            } else {
                return makePredicate(
                    MatchExprPredicate<InternalExprLTMatchExpression>(minPath, matchExprData),
                    MatchExprPredicate<InternalExprLTMatchExpression>(maxPath,
                                                                      maxTime.firstElement()),
                    MatchExprPredicate<LTMatchExpression, Value>(
                        kBucketIdFieldName,
                        constructObjectIdValue<LTMatchExpression>(matchExprData,
                                                                  bucketMaxSpanSeconds)));
            }
            MONGO_UNREACHABLE_TASSERT(6646906);

        case MatchExpression::LTE:
        case MatchExpression::INTERNAL_EXPR_LTE:
            // For $lte, make a $lte predicate against 'control.min'. In addition, if the comparison
            // is against the 'time' field, and the collection doesn't contain times outside the
            // 32 bit range, include a predicate against the _id field which is
            // converted to the maximum for the corresponding range of ObjectIds. In
            // addition, we include a {'control.max' : {$lte: 'time + bucketMaxSpanSeconds'}}
            // predicate which will be helpful in reducing bounds for index scans on 'time' field
            // and routing on mongos.
            //
            // The same procedure applies to aggregation expressions of the form
            // {$expr: {$lte: [...]}} that can be rewritten to use $_internalExprLte.
            if (!isTimeField) {
                return makeOr(makeVector<std::unique_ptr<MatchExpression>>(
                    std::make_unique<InternalExprLTEMatchExpression>(minPath, matchExprData),
                    createTypeEqualityPredicate(pExpCtx, matchExprPath, assumeNoMixedSchemaData)));
            } else if (bucketSpec.usesExtendedRange()) {
                return makePredicate(
                    MatchExprPredicate<InternalExprLTEMatchExpression>(minPath, matchExprData),
                    MatchExprPredicate<InternalExprLTEMatchExpression>(maxPath,
                                                                       maxTime.firstElement()));
            } else if (matchExprData.Date().toMillisSinceEpoch() < 0LL) {
                // Since by this point we know that no time value has been inserted < 0,
                // we know that no document can meet this criteria
                return std::make_unique<AlwaysFalseMatchExpression>();
            } else if (matchExprData.Date().toMillisSinceEpoch() > max32BitEpochMillis) {
                // Since by this point we know that no time value has been inserted > 0xffffffff
                // every document must be less than this value
                return std::make_unique<AlwaysTrueMatchExpression>();
            } else {
                return makePredicate(
                    MatchExprPredicate<InternalExprLTEMatchExpression>(minPath, matchExprData),
                    MatchExprPredicate<InternalExprLTEMatchExpression>(maxPath,
                                                                       maxTime.firstElement()),
                    MatchExprPredicate<LTEMatchExpression, Value>(
                        kBucketIdFieldName,
                        constructObjectIdValue<LTEMatchExpression>(matchExprData,
                                                                   bucketMaxSpanSeconds)));
            }
            MONGO_UNREACHABLE_TASSERT(6646907);

        default:
            MONGO_UNREACHABLE_TASSERT(5348302);
    }

    MONGO_UNREACHABLE_TASSERT(5348303);
}

std::unique_ptr<MatchExpression> createTightComparisonPredicate(
    const ComparisonMatchExpressionBase* matchExpr,
    const BucketSpec& bucketSpec,
    ExpressionContext::CollationMatchesDefault collationMatchesDefault) {
    using namespace timeseries;
    const auto matchExprPath = matchExpr->path();
    const auto matchExprData = matchExpr->getData();

    const auto error = checkComparisonPredicateEligibility(
        matchExpr, matchExprPath, matchExprData, bucketSpec, collationMatchesDefault);
    if (error) {
        return handleIneligible(BucketSpec::IneligiblePredicatePolicy::kIgnore, matchExpr, *error)
            .loosePredicate;
    }

    // We have to disable the tight predicate for the measurement field. There might be missing
    // values in the measurements and the control fields ignore them on insertion. So we cannot use
    // bucket min and max to determine the property of all events in the bucket. For measurement
    // fields, there's a further problem that if the control field is an array, we cannot generate
    // the tight predicate because the predicate will be implicitly mapped over the array elements.
    if (matchExprPath != bucketSpec.timeField()) {
        return handleIneligible(BucketSpec::IneligiblePredicatePolicy::kIgnore,
                                matchExpr,
                                "can't create tight predicate on non-time field")
            .tightPredicate;
    }

    auto minPath = std::string{kControlMinFieldNamePrefix} + matchExprPath;
    const StringData minPathStringData(minPath);
    auto maxPath = std::string{kControlMaxFieldNamePrefix} + matchExprPath;
    const StringData maxPathStringData(maxPath);

    switch (matchExpr->matchType()) {
        // All events satisfy $eq if bucket min and max both satisfy $eq.
        case MatchExpression::EQ:
            return makePredicate(
                MatchExprPredicate<EqualityMatchExpression>(minPathStringData, matchExprData),
                MatchExprPredicate<EqualityMatchExpression>(maxPathStringData, matchExprData));
        case MatchExpression::INTERNAL_EXPR_EQ:
            return makePredicate(
                MatchExprPredicate<InternalExprEqMatchExpression>(minPathStringData, matchExprData),
                MatchExprPredicate<InternalExprEqMatchExpression>(maxPathStringData,
                                                                  matchExprData));

        // All events satisfy $gt if bucket min satisfy $gt.
        case MatchExpression::GT:
            return std::make_unique<GTMatchExpression>(minPathStringData, matchExprData);
        case MatchExpression::INTERNAL_EXPR_GT:
            return std::make_unique<InternalExprGTMatchExpression>(minPathStringData,
                                                                   matchExprData);

        // All events satisfy $gte if bucket min satisfy $gte.
        case MatchExpression::GTE:
            return std::make_unique<GTEMatchExpression>(minPathStringData, matchExprData);
        case MatchExpression::INTERNAL_EXPR_GTE:
            return std::make_unique<InternalExprGTEMatchExpression>(minPathStringData,
                                                                    matchExprData);

        // All events satisfy $lt if bucket max satisfy $lt.
        case MatchExpression::LT:
            return std::make_unique<LTMatchExpression>(maxPathStringData, matchExprData);
        case MatchExpression::INTERNAL_EXPR_LT:
            return std::make_unique<InternalExprLTMatchExpression>(maxPathStringData,
                                                                   matchExprData);

        // All events satisfy $lte if bucket max satisfy $lte.
        case MatchExpression::LTE:
            return std::make_unique<LTEMatchExpression>(maxPathStringData, matchExprData);
        case MatchExpression::INTERNAL_EXPR_LTE:
            return std::make_unique<InternalExprLTEMatchExpression>(maxPathStringData,
                                                                    matchExprData);

        default:
            MONGO_UNREACHABLE_TASSERT(7026901);
    }
}

std::unique_ptr<MatchExpression> createTightExprTimeFieldPredicate(
    const ExprMatchExpression* matchExpr,
    const BucketSpec& bucketSpec,
    ExpressionContext::CollationMatchesDefault collationMatchesDefault,
    boost::intrusive_ptr<ExpressionContext> pExpCtx) {
    using namespace timeseries;
    RewriteExpr::RewriteResult rewriteRes =
        RewriteExpr::rewrite(matchExpr->getExpression(), pExpCtx->getCollator());
    auto unownedExpr = rewriteRes.matchExpression();

    // There might be children in the $and expression that cannot be rewritten to a match
    // expression. If this is the case we cannot assume that the tight predicate or
    // wholeBucketFilter produced by the rewritten $and expression is correct. Measurements in the
    // bucket might fit the rewritten $and expression, but fail to fit the other children of the
    // $and expression and will be returned incorrectly.

    // It is an error to call 'createPredicate' on predicates on the meta field, and it only
    // returns a value for predicates on the 'timeField'.
    if (unownedExpr && rewriteRes.allSubExpressionsRewritten() &&
        unownedExpr->path() == bucketSpec.timeField() &&
        ComparisonMatchExpressionBase::isInternalExprComparison(unownedExpr->matchType())) {
        const auto compareMatchExpr =
            checked_cast<const ComparisonMatchExpressionBase*>(unownedExpr);
        return createTightComparisonPredicate(
            compareMatchExpr, bucketSpec, collationMatchesDefault);
    }

    return handleIneligible(BucketSpec::IneligiblePredicatePolicy::kIgnore,
                            matchExpr,
                            "can only handle comparison $expr match expressions on the timeField")
        .tightPredicate;
}

}  // namespace

BucketSpec::BucketPredicate BucketSpec::createPredicatesOnBucketLevelField(
    const MatchExpression* matchExpr,
    const BucketSpec& bucketSpec,
    int bucketMaxSpanSeconds,
    ExpressionContext::CollationMatchesDefault collationMatchesDefault,
    const boost::intrusive_ptr<ExpressionContext>& pExpCtx,
    bool haveComputedMetaField,
    bool includeMetaField,
    bool assumeNoMixedSchemaData,
    IneligiblePredicatePolicy policy) {

    tassert(5916304, "BucketSpec::createPredicatesOnBucketLevelField nullptr", matchExpr);

    // If we have a leaf predicate on a meta field, we can map it to the bucket's meta field.
    // This includes comparisons such as $eq and $lte, as well as other non-comparison predicates
    // such as $exists, or $mod. Unrenamable expressions can't be split into a whole bucket level
    // filter, when we should return nullptr.
    //
    // Metadata predicates are partially handled earlier, by splitting the match expression into a
    // metadata-only part, and measurement/time-only part. However, splitting a $match into two
    // sequential $matches only works when splitting a conjunction. A predicate like
    // {$or: [ {a: 5}, {meta.b: 5} ]} can't be split, and can't be metadata-only, so we have to
    // handle it here.
    const auto matchExprPath = matchExpr->path();
    if (!matchExprPath.empty() && bucketSpec.metaField() &&
        (matchExprPath == bucketSpec.metaField().get() ||
         expression::isPathPrefixOf(bucketSpec.metaField().get(), matchExprPath))) {

        if (haveComputedMetaField)
            return handleIneligible(policy, matchExpr, "can't handle a computed meta field");

        if (!includeMetaField)
            return handleIneligible(policy, matchExpr, "cannot handle an excluded meta field");

        if (expression::hasOnlyRenameableMatchExpressionChildren(*matchExpr)) {
            auto looseResult = matchExpr->shallowClone();
            expression::applyRenamesToExpression(
                looseResult.get(),
                {{bucketSpec.metaField().value(), timeseries::kBucketMetaFieldName.toString()}});
            auto tightResult = looseResult->shallowClone();
            return {std::move(looseResult), std::move(tightResult)};
        } else {
            return {nullptr, nullptr};
        }
    }

    if (matchExpr->matchType() == MatchExpression::AND) {
        auto nextAnd = static_cast<const AndMatchExpression*>(matchExpr);
        auto looseAndExpression = std::make_unique<AndMatchExpression>();
        auto tightAndExpression = std::make_unique<AndMatchExpression>();
        for (size_t i = 0; i < nextAnd->numChildren(); i++) {
            auto child = createPredicatesOnBucketLevelField(nextAnd->getChild(i),
                                                            bucketSpec,
                                                            bucketMaxSpanSeconds,
                                                            collationMatchesDefault,
                                                            pExpCtx,
                                                            haveComputedMetaField,
                                                            includeMetaField,
                                                            assumeNoMixedSchemaData,
                                                            policy);
            if (child.loosePredicate) {
                looseAndExpression->add(std::move(child.loosePredicate));
            }

            if (tightAndExpression && child.tightPredicate) {
                tightAndExpression->add(std::move(child.tightPredicate));
            } else {
                // For tight expression, null means always false, we can short circuit here.
                tightAndExpression = nullptr;
            }
        }

        // For a loose predicate, if we are unable to generate an expression we can just treat it as
        // always true or an empty AND. This is because we are trying to generate a predicate that
        // will match the superset of our actual results.
        std::unique_ptr<MatchExpression> looseExpression = nullptr;
        if (looseAndExpression->numChildren() == 1) {
            looseExpression = looseAndExpression->releaseChild(0);
        } else if (looseAndExpression->numChildren() > 1) {
            looseExpression = std::move(looseAndExpression);
        }

        // For a tight predicate, if we are unable to generate an expression we can just treat it as
        // always false. This is because we are trying to generate a predicate that will match the
        // subset of our actual results.
        std::unique_ptr<MatchExpression> tightExpression = nullptr;
        if (tightAndExpression && tightAndExpression->numChildren() == 1) {
            tightExpression = tightAndExpression->releaseChild(0);
        } else {
            tightExpression = std::move(tightAndExpression);
        }

        return {std::move(looseExpression), std::move(tightExpression)};
    } else if (matchExpr->matchType() == MatchExpression::OR) {
        // Given {$or: [A, B]}, suppose A, B can be pushed down as A', B'.
        // If an event matches {$or: [A, B]} then either:
        //     - it matches A, which means any bucket containing it matches A'
        //     - it matches B, which means any bucket containing it matches B'
        // So {$or: [A', B']} will capture all the buckets we need to satisfy {$or: [A, B]}.
        auto nextOr = static_cast<const OrMatchExpression*>(matchExpr);
        auto looseOrExpression = std::make_unique<OrMatchExpression>();
        auto tightOrExpression = std::make_unique<OrMatchExpression>();

        for (size_t i = 0; i < nextOr->numChildren(); i++) {
            auto child = createPredicatesOnBucketLevelField(nextOr->getChild(i),
                                                            bucketSpec,
                                                            bucketMaxSpanSeconds,
                                                            collationMatchesDefault,
                                                            pExpCtx,
                                                            haveComputedMetaField,
                                                            includeMetaField,
                                                            assumeNoMixedSchemaData,
                                                            policy);
            if (looseOrExpression && child.loosePredicate) {
                looseOrExpression->add(std::move(child.loosePredicate));
            } else {
                // For loose expression, null means always true, we can short circuit here.
                looseOrExpression = nullptr;
            }

            // For tight predicate, we give a tighter bound so that all events in the bucket
            // either all matches A or all matches B.
            if (child.tightPredicate) {
                tightOrExpression->add(std::move(child.tightPredicate));
            }
        }

        // For a loose predicate, if we are unable to generate an expression we can just treat it as
        // always true. This is because we are trying to generate a predicate that will match the
        // superset of our actual results.
        std::unique_ptr<MatchExpression> looseExpression = nullptr;
        if (looseOrExpression && looseOrExpression->numChildren() == 1) {
            looseExpression = looseOrExpression->releaseChild(0);
        } else {
            looseExpression = std::move(looseOrExpression);
        }

        // For a tight predicate, if we are unable to generate an expression we can just treat it as
        // always false or an empty OR. This is because we are trying to generate a predicate that
        // will match the subset of our actual results.
        std::unique_ptr<MatchExpression> tightExpression = nullptr;
        if (tightOrExpression->numChildren() == 1) {
            tightExpression = tightOrExpression->releaseChild(0);
        } else if (tightOrExpression->numChildren() > 1) {
            tightExpression = std::move(tightOrExpression);
        }

        return {std::move(looseExpression), std::move(tightExpression)};
    } else if (ComparisonMatchExpression::isComparisonMatchExpression(matchExpr) ||
               ComparisonMatchExpressionBase::isInternalExprComparison(matchExpr->matchType())) {
        return {
            createComparisonPredicate(checked_cast<const ComparisonMatchExpressionBase*>(matchExpr),
                                      bucketSpec,
                                      bucketMaxSpanSeconds,
                                      collationMatchesDefault,
                                      pExpCtx,
                                      haveComputedMetaField,
                                      includeMetaField,
                                      assumeNoMixedSchemaData,
                                      policy),
            createTightComparisonPredicate(
                checked_cast<const ComparisonMatchExpressionBase*>(matchExpr),
                bucketSpec,
                collationMatchesDefault)};
    } else if (matchExpr->matchType() == MatchExpression::EXPRESSION) {
        return {
            // The loose predicate will be pushed before the unpacking which will be inspected by
            // the
            // query planner. Since the classic planner doesn't handle the $expr expression, we
            // don't
            // generate the loose predicate.
            nullptr,
            createTightExprTimeFieldPredicate(checked_cast<const ExprMatchExpression*>(matchExpr),
                                              bucketSpec,
                                              collationMatchesDefault,
                                              pExpCtx)};
    } else if (matchExpr->matchType() == MatchExpression::GEO) {
        auto& geoExpr = static_cast<const GeoMatchExpression*>(matchExpr)->getGeoExpression();
        if (geoExpr.getPred() == GeoExpression::WITHIN ||
            geoExpr.getPred() == GeoExpression::INTERSECT) {
            return {std::make_unique<InternalBucketGeoWithinMatchExpression>(
                        geoExpr.getGeometryPtr(), geoExpr.getField()),
                    nullptr};
        }
    } else if (matchExpr->matchType() == MatchExpression::EXISTS) {
        if (assumeNoMixedSchemaData) {
            // We know that every field that appears in an event will also appear in the min/max.
            auto result = std::make_unique<AndMatchExpression>();
            result->add(std::make_unique<ExistsMatchExpression>(StringData(
                std::string{timeseries::kControlMinFieldNamePrefix} + matchExpr->path())));
            result->add(std::make_unique<ExistsMatchExpression>(StringData(
                std::string{timeseries::kControlMaxFieldNamePrefix} + matchExpr->path())));
            return {std::move(result), nullptr};
        } else {
            // At time of writing, we only pass 'kError' when creating a partial index, and
            // we know the collection will have no mixed-schema buckets by the time the index is
            // done building.
            tassert(5916305,
                    "Can't push down {$exists: true} when the collection may have mixed-schema "
                    "buckets.",
                    policy != IneligiblePredicatePolicy::kError);
            return {};
        }
    } else if (matchExpr->matchType() == MatchExpression::MATCH_IN) {
        // {a: {$in: [X, Y]}} is equivalent to {$or: [ {a: X}, {a: Y} ]}.
        // {$in: [/a/]} is interpreted as a regex query.
        // {$in: [null]} matches any nullish value.
        const auto* inExpr = static_cast<const InMatchExpression*>(matchExpr);
        if (inExpr->hasRegex())
            return handleIneligible(
                policy, matchExpr, "can't handle $regex predicate (inside $in predicate)");
        if (inExpr->hasNull())
            return handleIneligible(
                policy, matchExpr, "can't handle {$eq: null} predicate (inside $in predicate)");

        auto result = std::make_unique<OrMatchExpression>();

        bool alwaysTrue = false;
        for (auto&& elem : inExpr->getEqualities()) {
            // If inExpr is {$in: [X, Y]} then the elems are '0: X' and '1: Y'.
            auto eq = std::make_unique<EqualityMatchExpression>(
                inExpr->path(), elem, nullptr /*annotation*/, inExpr->getCollator());
            auto child = createComparisonPredicate(eq.get(),
                                                   bucketSpec,
                                                   bucketMaxSpanSeconds,
                                                   collationMatchesDefault,
                                                   pExpCtx,
                                                   haveComputedMetaField,
                                                   includeMetaField,
                                                   assumeNoMixedSchemaData,
                                                   policy);

            // As with OR, only add the child if it has been succesfully translated, otherwise the
            // $in cannot be correctly mapped to bucket level fields and we should return nullptr.
            if (child) {
                result->add(std::move(child));
            } else {
                alwaysTrue = true;
                if (policy == IneligiblePredicatePolicy::kIgnore)
                    break;
            }
        }
        if (alwaysTrue)
            return {};

        // As above, no special case for an empty IN: returning nullptr would be incorrect because
        // it means 'always-true', here.
        return {std::move(result), nullptr};
    }
    return handleIneligible(policy, matchExpr, "can't handle this predicate");
}

BSONObj BucketSpec::pushdownPredicate(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const TimeseriesOptions& tsOptions,
    ExpressionContext::CollationMatchesDefault collationMatchesDefault,
    const BSONObj& predicate,
    bool haveComputedMetaField,
    bool includeMetaField,
    bool assumeNoMixedSchemaData,
    IneligiblePredicatePolicy policy) {

    auto allowedFeatures = MatchExpressionParser::kDefaultSpecialFeatures;
    auto matchExpr = uassertStatusOK(
        MatchExpressionParser::parse(predicate, expCtx, ExtensionsCallbackNoop(), allowedFeatures));

    auto metaField = haveComputedMetaField ? boost::none : tsOptions.getMetaField();
    auto [metaOnlyPredicate, metricPredicate] = [&] {
        if (!metaField) {
            // If there's no metadata field, then none of the predicates are metadata-only
            // predicates.
            return std::make_pair(std::unique_ptr<MatchExpression>(nullptr), std::move(matchExpr));
        }

        return expression::splitMatchExpressionBy(
            std::move(matchExpr),
            {metaField->toString()},
            {{metaField->toString(), timeseries::kBucketMetaFieldName.toString()}},
            expression::isOnlyDependentOn);
    }();

    int maxSpanSeconds = tsOptions.getBucketMaxSpanSeconds()
        ? *tsOptions.getBucketMaxSpanSeconds()
        : timeseries::getMaxSpanSecondsFromGranularity(tsOptions.getGranularity());

    std::unique_ptr<MatchExpression> bucketMetricPredicate = metricPredicate
        ? createPredicatesOnBucketLevelField(
              metricPredicate.get(),
              BucketSpec{
                  tsOptions.getTimeField().toString(),
                  metaField.map([](StringData s) { return s.toString(); }),
                  // Since we are operating on a collection, not a query-result,
                  // there are no inclusion/exclusion projections we need to apply
                  // to the buckets before unpacking. So we can use default values for the rest of
                  // the arguments.
              },
              maxSpanSeconds,
              collationMatchesDefault,
              expCtx,
              haveComputedMetaField,
              includeMetaField,
              assumeNoMixedSchemaData,
              policy)
              .loosePredicate
        : nullptr;

    BSONObjBuilder result;
    if (metaOnlyPredicate)
        metaOnlyPredicate->serialize(&result);
    if (bucketMetricPredicate)
        bucketMetricPredicate->serialize(&result);
    return result.obj();
}

class BucketUnpacker::UnpackingImpl {
public:
    UnpackingImpl() = default;
    virtual ~UnpackingImpl() = default;

    virtual void addField(const BSONElement& field) = 0;
    virtual int measurementCount(const BSONElement& timeField) const = 0;
    virtual bool getNext(MutableDocument& measurement,
                         const BucketSpec& spec,
                         const Value& metaValue,
                         bool includeTimeField,
                         bool includeMetaField) = 0;
    virtual bool getNext(BSONObjBuilder& builder,
                         const BucketSpec& spec,
                         const BSONElement& metaValue,
                         bool includeTimeField,
                         bool includeMetaField) = 0;
    virtual void extractSingleMeasurement(MutableDocument& measurement,
                                          int j,
                                          const BucketSpec& spec,
                                          const std::set<std::string>& unpackFieldsToIncludeExclude,
                                          const BSONObj& bucket,
                                          const Value& metaValue,
                                          bool includeTimeField,
                                          bool includeMetaField) = 0;

    // Provides an upper bound on the number of fields in each measurement.
    virtual std::size_t numberOfFields() = 0;

protected:
    // Data field count is variable, but time and metadata are fixed.
    constexpr static std::size_t kFixedFieldNumber = 2;
};

namespace {


// Unpacker for V1 uncompressed buckets
class BucketUnpackerV1 : public BucketUnpacker::UnpackingImpl {
public:
    // A table that is useful for interpolations between the number of measurements in a bucket and
    // the byte size of a bucket's data section timestamp column. Each table entry is a pair (b_i,
    // S_i), where b_i is the number of measurements in the bucket and S_i is the byte size of the
    // timestamp BSONObj. The table is bounded by 16 MB (2 << 23 bytes) where the table entries are
    // pairs of b_i and S_i for the lower bounds of the row key digit intervals [0, 9], [10, 99],
    // [100, 999], [1000, 9999] and so on. The last entry in the table, S7, is the first entry to
    // exceed the server BSON object limit of 16 MB.
    static constexpr std::array<std::pair<int32_t, int32_t>, 8> kTimestampObjSizeTable{
        {{0, BSONObj::kMinBSONLength},
         {10, 115},
         {100, 1195},
         {1000, 12895},
         {10000, 138895},
         {100000, 1488895},
         {1000000, 15888895},
         {10000000, 168888895}}};

    static int computeElementCountFromTimestampObjSize(int targetTimestampObjSize);

    BucketUnpackerV1(const BSONElement& timeField);

    void addField(const BSONElement& field) override;
    int measurementCount(const BSONElement& timeField) const override;
    bool getNext(MutableDocument& measurement,
                 const BucketSpec& spec,
                 const Value& metaValue,
                 bool includeTimeField,
                 bool includeMetaField) override;
    bool getNext(BSONObjBuilder& builder,
                 const BucketSpec& spec,
                 const BSONElement& metaValue,
                 bool includeTimeField,
                 bool includeMetaField) override;
    void extractSingleMeasurement(MutableDocument& measurement,
                                  int j,
                                  const BucketSpec& spec,
                                  const std::set<std::string>& unpackFieldsToIncludeExclude,
                                  const BSONObj& bucket,
                                  const Value& metaValue,
                                  bool includeTimeField,
                                  bool includeMetaField) override;
    std::size_t numberOfFields() override;

private:
    // Iterates the timestamp section of the bucket to drive the unpacking iteration.
    BSONObjIterator _timeFieldIter;

    // Iterators used to unpack the columns of the above bucket that are populated during the reset
    // phase according to the provided 'BucketSpec'.
    std::vector<std::pair<std::string, BSONObjIterator>> _fieldIters;
};

// Calculates the number of measurements in a bucket given the 'targetTimestampObjSize' using the
// 'BucketUnpacker::kTimestampObjSizeTable' table. If the 'targetTimestampObjSize' hits a record in
// the table, this helper returns the measurement count corresponding to the table record.
// Otherwise, the 'targetTimestampObjSize' is used to probe the table for the smallest {b_i, S_i}
// pair such that 'targetTimestampObjSize' < S_i. Once the interval is found, the upper bound of the
// pair for the interval is computed and then linear interpolation is used to compute the
// measurement count corresponding to the 'targetTimestampObjSize' provided.
int BucketUnpackerV1::computeElementCountFromTimestampObjSize(int targetTimestampObjSize) {
    auto currentInterval =
        std::find_if(std::begin(BucketUnpackerV1::kTimestampObjSizeTable),
                     std::end(BucketUnpackerV1::kTimestampObjSizeTable),
                     [&](const auto& entry) { return targetTimestampObjSize <= entry.second; });

    if (currentInterval->second == targetTimestampObjSize) {
        return currentInterval->first;
    }
    // This points to the first interval larger than the target 'targetTimestampObjSize', the actual
    // interval that will cover the object size is the interval before the current one.
    tassert(5422104,
            "currentInterval should not point to the first table entry",
            currentInterval > BucketUnpackerV1::kTimestampObjSizeTable.begin());
    --currentInterval;

    auto nDigitsInRowKey = 1 + (currentInterval - BucketUnpackerV1::kTimestampObjSizeTable.begin());

    return currentInterval->first +
        ((targetTimestampObjSize - currentInterval->second) / (10 + nDigitsInRowKey));
}

BucketUnpackerV1::BucketUnpackerV1(const BSONElement& timeField)
    : _timeFieldIter(BSONObjIterator{timeField.Obj()}) {}

void BucketUnpackerV1::addField(const BSONElement& field) {
    _fieldIters.emplace_back(field.fieldNameStringData(), BSONObjIterator{field.Obj()});
}

int BucketUnpackerV1::measurementCount(const BSONElement& timeField) const {
    return computeElementCountFromTimestampObjSize(timeField.objsize());
}

bool BucketUnpackerV1::getNext(MutableDocument& measurement,
                               const BucketSpec& spec,
                               const Value& metaValue,
                               bool includeTimeField,
                               bool includeMetaField) {
    auto&& timeElem = _timeFieldIter.next();
    if (includeTimeField) {
        measurement.addField(spec.timeFieldHashed(), Value{timeElem});
    }

    // Includes metaField when we're instructed to do so and metaField value exists.
    if (includeMetaField && !metaValue.missing()) {
        measurement.addField(*spec.metaFieldHashed(), metaValue);
    }

    auto& currentIdx = timeElem.fieldNameStringData();
    for (auto&& [colName, colIter] : _fieldIters) {
        if (auto&& elem = *colIter; colIter.more() && elem.fieldNameStringData() == currentIdx) {
            measurement.addField(colName, Value{elem});
            colIter.advance(elem);
        }
    }

    return _timeFieldIter.more();
}

bool BucketUnpackerV1::getNext(BSONObjBuilder& builder,
                               const BucketSpec& spec,
                               const BSONElement& metaValue,
                               bool includeTimeField,
                               bool includeMetaField) {
    auto&& timeElem = _timeFieldIter.next();
    if (includeTimeField) {
        builder.appendAs(timeElem, spec.timeField());
    }

    // Includes metaField when we're instructed to do so and metaField value exists.
    if (includeMetaField && !metaValue.eoo()) {
        builder.appendAs(metaValue, *spec.metaField());
    }

    const auto& currentIdx = timeElem.fieldNameStringData();
    for (auto&& [colName, colIter] : _fieldIters) {
        if (auto&& elem = *colIter; colIter.more() && elem.fieldNameStringData() == currentIdx) {
            builder.appendAs(elem, colName);
            colIter.advance(elem);
        }
    }

    return _timeFieldIter.more();
}

void BucketUnpackerV1::extractSingleMeasurement(
    MutableDocument& measurement,
    int j,
    const BucketSpec& spec,
    const std::set<std::string>& unpackFieldsToIncludeExclude,
    const BSONObj& bucket,
    const Value& metaValue,
    bool includeTimeField,
    bool includeMetaField) {
    auto rowKey = std::to_string(j);
    auto targetIdx = StringData{rowKey};
    auto&& dataRegion = bucket.getField(timeseries::kBucketDataFieldName).Obj();

    if (includeMetaField && !metaValue.missing()) {
        measurement.addField(*spec.metaFieldHashed(), metaValue);
    }

    for (auto&& dataElem : dataRegion) {
        auto colName = dataElem.fieldNameStringData();
        if (!determineIncludeField(colName, spec.behavior(), unpackFieldsToIncludeExclude)) {
            continue;
        }
        auto value = dataElem[targetIdx];
        if (value) {
            measurement.addField(dataElem.fieldNameStringData(), Value{value});
        }
    }
}

std::size_t BucketUnpackerV1::numberOfFields() {
    // The data fields are tracked by _fieldIters, but we need to account also for the time field
    // and possibly the meta field.
    return kFixedFieldNumber + _fieldIters.size();
}

// Unpacker for V2 compressed buckets
class BucketUnpackerV2 : public BucketUnpacker::UnpackingImpl {
public:
    BucketUnpackerV2(const BSONElement& timeField, int elementCount);

    void addField(const BSONElement& field) override;
    int measurementCount(const BSONElement& timeField) const override;
    bool getNext(MutableDocument& measurement,
                 const BucketSpec& spec,
                 const Value& metaValue,
                 bool includeTimeField,
                 bool includeMetaField) override;
    bool getNext(BSONObjBuilder& builder,
                 const BucketSpec& spec,
                 const BSONElement& metaValue,
                 bool includeTimeField,
                 bool includeMetaField) override;
    void extractSingleMeasurement(MutableDocument& measurement,
                                  int j,
                                  const BucketSpec& spec,
                                  const std::set<std::string>& unpackFieldsToIncludeExclude,
                                  const BSONObj& bucket,
                                  const Value& metaValue,
                                  bool includeTimeField,
                                  bool includeMetaField) override;
    std::size_t numberOfFields() override;

private:
    struct ColumnStore {
        ColumnStore(BSONElement elem)
            : column(elem),
              it(column.begin()),
              end(column.end()),
              hashedName(FieldNameHasher{}(column.name())) {}
        ColumnStore(ColumnStore&& other)
            : column(std::move(other.column)),
              it(other.it.moveTo(column)),
              end(other.end),
              hashedName(other.hashedName) {}

        BSONColumn column;
        BSONColumn::Iterator it;
        BSONColumn::Iterator end;
        size_t hashedName;
    };

    // Iterates the timestamp section of the bucket to drive the unpacking iteration.
    ColumnStore _timeColumn;

    // Iterators used to unpack the columns of the above bucket that are populated during the reset
    // phase according to the provided 'BucketSpec'.
    std::vector<ColumnStore> _fieldColumns;

    // Element count
    int _elementCount;
};

BucketUnpackerV2::BucketUnpackerV2(const BSONElement& timeField, int elementCount)
    : _timeColumn(timeField), _elementCount(elementCount) {
    if (_elementCount == -1) {
        _elementCount = _timeColumn.column.size();
    }
}

void BucketUnpackerV2::addField(const BSONElement& field) {
    _fieldColumns.emplace_back(field);
}

int BucketUnpackerV2::measurementCount(const BSONElement& timeField) const {
    return _elementCount;
}

bool BucketUnpackerV2::getNext(MutableDocument& measurement,
                               const BucketSpec& spec,
                               const Value& metaValue,
                               bool includeTimeField,
                               bool includeMetaField) {
    // Get element and increment iterator
    const auto& timeElem = *_timeColumn.it;
    if (includeTimeField) {
        measurement.addField(spec.timeFieldHashed(), Value{timeElem});
    }
    ++_timeColumn.it;

    // Includes metaField when we're instructed to do so and metaField value exists.
    if (includeMetaField && !metaValue.missing()) {
        measurement.addField(*spec.metaFieldHashed(), metaValue);
    }

    for (auto& fieldColumn : _fieldColumns) {
        uassert(6067601,
                "Bucket unexpectedly contained fewer values than count",
                fieldColumn.it != fieldColumn.end);
        const BSONElement& elem = *fieldColumn.it;
        // EOO represents missing field
        if (!elem.eoo()) {
            measurement.addField(HashedFieldName{fieldColumn.column.name(), fieldColumn.hashedName},
                                 Value{elem});
        }
        ++fieldColumn.it;
    }

    return _timeColumn.it != _timeColumn.end;
}

bool BucketUnpackerV2::getNext(BSONObjBuilder& builder,
                               const BucketSpec& spec,
                               const BSONElement& metaValue,
                               bool includeTimeField,
                               bool includeMetaField) {
    // Get element and increment iterator
    const auto& timeElem = *_timeColumn.it;
    if (includeTimeField) {
        builder.appendAs(timeElem, spec.timeField());
    }
    ++_timeColumn.it;

    // Includes metaField when we're instructed to do so and metaField value exists.
    if (includeMetaField && !metaValue.eoo()) {
        builder.appendAs(metaValue, *spec.metaField());
    }

    for (auto& fieldColumn : _fieldColumns) {
        uassert(7026803,
                "Bucket unexpectedly contained fewer values than count",
                fieldColumn.it != fieldColumn.end);
        const BSONElement& elem = *fieldColumn.it;
        // EOO represents missing field
        if (!elem.eoo()) {
            builder.appendAs(elem, fieldColumn.column.name());
        }
        ++fieldColumn.it;
    }

    return _timeColumn.it != _timeColumn.end;
}

void BucketUnpackerV2::extractSingleMeasurement(
    MutableDocument& measurement,
    int j,
    const BucketSpec& spec,
    const std::set<std::string>& unpackFieldsToIncludeExclude,
    const BSONObj& bucket,
    const Value& metaValue,
    bool includeTimeField,
    bool includeMetaField) {
    if (includeTimeField) {
        auto val = _timeColumn.column[j];
        uassert(
            6067500, "Bucket unexpectedly contained fewer values than count", val && !val->eoo());
        measurement.addField(spec.timeFieldHashed(), Value{*val});
    }

    if (includeMetaField && !metaValue.missing()) {
        measurement.addField(*spec.metaFieldHashed(), metaValue);
    }

    if (includeTimeField) {
        for (auto& fieldColumn : _fieldColumns) {
            auto val = fieldColumn.column[j];
            uassert(6067600, "Bucket unexpectedly contained fewer values than count", val);
            measurement.addField(HashedFieldName{fieldColumn.column.name(), fieldColumn.hashedName},
                                 Value{*val});
        }
    }
}

std::size_t BucketUnpackerV2::numberOfFields() {
    // The data fields are tracked by _fieldColumns, but we need to account also for the time field
    // and possibly the meta field.
    return kFixedFieldNumber + _fieldColumns.size();
}
}  // namespace

BucketSpec::BucketSpec(const std::string& timeField,
                       const boost::optional<std::string>& metaField,
                       const std::set<std::string>& fields,
                       Behavior behavior,
                       const std::set<std::string>& computedProjections,
                       bool usesExtendedRange)
    : _fieldSet(fields),
      _behavior(behavior),
      _computedMetaProjFields(computedProjections),
      _timeField(timeField),
      _timeFieldHashed(FieldNameHasher().hashedFieldName(_timeField)),
      _metaField(metaField),
      _usesExtendedRange(usesExtendedRange) {
    if (_metaField) {
        _metaFieldHashed = FieldNameHasher().hashedFieldName(*_metaField);
    }
}

BucketSpec::BucketSpec(const BucketSpec& other)
    : _fieldSet(other._fieldSet),
      _behavior(other._behavior),
      _computedMetaProjFields(other._computedMetaProjFields),
      _timeField(other._timeField),
      _timeFieldHashed(HashedFieldName{_timeField, other._timeFieldHashed->hash()}),
      _metaField(other._metaField),
      _usesExtendedRange(other._usesExtendedRange) {
    if (_metaField) {
        _metaFieldHashed = HashedFieldName{*_metaField, other._metaFieldHashed->hash()};
    }
}

BucketSpec::BucketSpec(BucketSpec&& other)
    : _fieldSet(std::move(other._fieldSet)),
      _behavior(other._behavior),
      _computedMetaProjFields(std::move(other._computedMetaProjFields)),
      _timeField(std::move(other._timeField)),
      _timeFieldHashed(HashedFieldName{_timeField, other._timeFieldHashed->hash()}),
      _metaField(std::move(other._metaField)),
      _usesExtendedRange(other._usesExtendedRange) {
    if (_metaField) {
        _metaFieldHashed = HashedFieldName{*_metaField, other._metaFieldHashed->hash()};
    }
}

BucketSpec& BucketSpec::operator=(const BucketSpec& other) {
    if (&other != this) {
        _fieldSet = other._fieldSet;
        _behavior = other._behavior;
        _computedMetaProjFields = other._computedMetaProjFields;
        _timeField = other._timeField;
        _timeFieldHashed = HashedFieldName{_timeField, other._timeFieldHashed->hash()};
        _metaField = other._metaField;
        if (_metaField) {
            _metaFieldHashed = HashedFieldName{*_metaField, other._metaFieldHashed->hash()};
        }
        _usesExtendedRange = other._usesExtendedRange;
    }
    return *this;
}

void BucketSpec::setTimeField(std::string&& name) {
    _timeField = std::move(name);
    _timeFieldHashed = FieldNameHasher().hashedFieldName(_timeField);
}

const std::string& BucketSpec::timeField() const {
    return _timeField;
}

HashedFieldName BucketSpec::timeFieldHashed() const {
    invariant(_timeFieldHashed->key().rawData() == _timeField.data());
    invariant(_timeFieldHashed->key() == _timeField);
    return *_timeFieldHashed;
}

void BucketSpec::setMetaField(boost::optional<std::string>&& name) {
    _metaField = std::move(name);
    if (_metaField) {
        _metaFieldHashed = FieldNameHasher().hashedFieldName(*_metaField);
    } else {
        _metaFieldHashed = boost::none;
    }
}

const boost::optional<std::string>& BucketSpec::metaField() const {
    return _metaField;
}

boost::optional<HashedFieldName> BucketSpec::metaFieldHashed() const {
    return _metaFieldHashed;
}

BucketUnpacker::BucketUnpacker() = default;
BucketUnpacker::BucketUnpacker(BucketUnpacker&& other) = default;
BucketUnpacker::~BucketUnpacker() = default;
BucketUnpacker& BucketUnpacker::operator=(BucketUnpacker&& rhs) = default;

BucketUnpacker::BucketUnpacker(BucketSpec spec) {
    setBucketSpec(std::move(spec));
}

void BucketUnpacker::addComputedMetaProjFields(const std::vector<StringData>& computedFieldNames) {
    for (auto&& field : computedFieldNames) {
        _spec.addComputedMetaProjFields(field);

        // If we're already specifically including fields, we need to add the computed fields to
        // the included field set to indicate they're in the output doc.
        if (_spec.behavior() == BucketSpec::Behavior::kInclude) {
            _spec.addIncludeExcludeField(field);
        } else {
            // Since exclude is applied after addComputedMetaProjFields, we must erase the new field
            // from the include/exclude fields so this doesn't get removed.
            _spec.removeIncludeExcludeField(field.toString());
        }
    }

    // Recalculate _includeTimeField, since both computedMetaProjFields and fieldSet may have
    // changed.
    determineIncludeTimeField();
}

Document BucketUnpacker::getNext() {
    tassert(5521503, "'getNext()' requires the bucket to be owned", _bucket.isOwned());
    tassert(5422100, "'getNext()' was called after the bucket has been exhausted", hasNext());

    // MutableDocument reserves memory based on the number of fields, but uses a fixed size of 25
    // bytes plus an allowance of 7 characters for the field name. Doubling the number of fields
    // should give us enough overhead for longer field names without wasting too much memory.
    auto measurement = MutableDocument{2 * _unpackingImpl->numberOfFields()};
    _hasNext = _unpackingImpl->getNext(
        measurement, _spec, _metaValue, _includeTimeField, _includeMetaField);

    // Add computed meta projections.
    for (auto&& name : _spec.computedMetaProjFields()) {
        measurement.addField(name, Value{_computedMetaProjections[name]});
    }

    if (_includeMinTimeAsMetadata && _minTime) {
        measurement.metadata().setTimeseriesBucketMinTime(*_minTime);
    }

    if (_includeMaxTimeAsMetadata && _maxTime) {
        measurement.metadata().setTimeseriesBucketMaxTime(*_maxTime);
    }

    return measurement.freeze();
}

BSONObj BucketUnpacker::getNextBson() {
    tassert(7026800, "'getNextBson()' requires the bucket to be owned", _bucket.isOwned());
    tassert(7026801, "'getNextBson()' was called after the bucket has been exhausted", hasNext());
    tassert(7026802,
            "'getNextBson()' cannot return max and min time as metadata",
            !_includeMaxTimeAsMetadata && !_includeMinTimeAsMetadata);

    BSONObjBuilder builder;
    _hasNext = _unpackingImpl->getNext(
        builder, _spec, _metaBSONElem, _includeTimeField, _includeMetaField);

    // Add computed meta projections.
    for (auto&& name : _spec.computedMetaProjFields()) {
        builder.appendAs(_computedMetaProjections[name], name);
    }

    return builder.obj();
}

Document BucketUnpacker::extractSingleMeasurement(int j) {
    tassert(5422101,
            "'extractSingleMeasurment' expects j to be greater than or equal to zero and less than "
            "or equal to the number of measurements in a bucket",
            j >= 0 && j < _numberOfMeasurements);

    auto measurement = MutableDocument{};
    _unpackingImpl->extractSingleMeasurement(measurement,
                                             j,
                                             _spec,
                                             fieldsToIncludeExcludeDuringUnpack(),
                                             _bucket,
                                             _metaValue,
                                             _includeTimeField,
                                             _includeMetaField);

    // Add computed meta projections.
    for (auto&& name : _spec.computedMetaProjFields()) {
        measurement.addField(name, Value{_computedMetaProjections[name]});
    }

    return measurement.freeze();
}

void BucketUnpacker::reset(BSONObj&& bucket, bool bucketMatchedQuery) {
    _unpackingImpl.reset();
    _bucket = std::move(bucket);
    _bucketMatchedQuery = bucketMatchedQuery;
    uassert(5346510, "An empty bucket cannot be unpacked", !_bucket.isEmpty());

    auto&& dataRegion = _bucket.getField(timeseries::kBucketDataFieldName).Obj();
    if (dataRegion.isEmpty()) {
        // If the data field of a bucket is present but it holds an empty object, there's nothing to
        // unpack.
        return;
    }

    auto&& timeFieldElem = dataRegion.getField(_spec.timeField());
    uassert(5346700,
            "The $_internalUnpackBucket stage requires the data region to have a timeField object",
            timeFieldElem);

    _metaBSONElem = _bucket[timeseries::kBucketMetaFieldName];
    _metaValue = Value{_metaBSONElem};
    if (_spec.metaField()) {
        // The spec indicates that there might be a metadata region. Missing metadata in
        // measurements is expressed with missing metadata in a bucket. But we disallow undefined
        // since the undefined BSON type is deprecated.
        uassert(5369600,
                "The $_internalUnpackBucket stage allows metadata to be absent or otherwise, it "
                "must not be the deprecated undefined bson type",
                _metaValue.missing() || _metaValue.getType() != BSONType::Undefined);
    } else {
        // If the spec indicates that the time series collection has no metadata field, then we
        // should not find a metadata region in the underlying bucket documents.
        uassert(5369601,
                "The $_internalUnpackBucket stage expects buckets to have missing metadata regions "
                "if the metaField parameter is not provided",
                _metaValue.missing());
    }

    auto&& controlField = _bucket[timeseries::kBucketControlFieldName];
    uassert(5857902,
            "The $_internalUnpackBucket stage requires 'control' object to be present",
            controlField && controlField.type() == BSONType::Object);

    if (_includeMinTimeAsMetadata) {
        auto&& controlMin = controlField.Obj()[timeseries::kBucketControlMinFieldName];
        uassert(6460203,
                str::stream() << "The $_internalUnpackBucket stage requires '"
                              << timeseries::kControlMinFieldNamePrefix << "' object to be present",
                controlMin && controlMin.type() == BSONType::Object);
        auto&& minTime = controlMin.Obj()[_spec.timeField()];
        uassert(6460204,
                str::stream() << "The $_internalUnpackBucket stage requires '"
                              << timeseries::kControlMinFieldNamePrefix << "." << _spec.timeField()
                              << "' to be a date",
                minTime && minTime.type() == BSONType::Date);
        _minTime = minTime.date();
    }

    if (_includeMaxTimeAsMetadata) {
        auto&& controlMax = controlField.Obj()[timeseries::kBucketControlMaxFieldName];
        uassert(6460205,
                str::stream() << "The $_internalUnpackBucket stage requires '"
                              << timeseries::kControlMaxFieldNamePrefix << "' object to be present",
                controlMax && controlMax.type() == BSONType::Object);
        auto&& maxTime = controlMax.Obj()[_spec.timeField()];
        uassert(6460206,
                str::stream() << "The $_internalUnpackBucket stage requires '"
                              << timeseries::kControlMaxFieldNamePrefix << "." << _spec.timeField()
                              << "' to be a date",
                maxTime && maxTime.type() == BSONType::Date);
        _maxTime = maxTime.date();
    }

    auto&& versionField = controlField.Obj()[timeseries::kBucketControlVersionFieldName];
    uassert(5857903,
            "The $_internalUnpackBucket stage requires 'control.version' field to be present",
            versionField && isNumericBSONType(versionField.type()));
    auto version = versionField.Number();

    if (version == 1) {
        _unpackingImpl = std::make_unique<BucketUnpackerV1>(timeFieldElem);
    } else if (version == 2) {
        auto countField = controlField.Obj()[timeseries::kBucketControlCountFieldName];
        _unpackingImpl =
            std::make_unique<BucketUnpackerV2>(timeFieldElem,
                                               countField && isNumericBSONType(countField.type())
                                                   ? static_cast<int>(countField.Number())
                                                   : -1);
    } else {
        uasserted(5857900, "Invalid bucket version");
    }

    // Walk the data region of the bucket, and decide if an iterator should be set up based on the
    // include or exclude case.
    for (auto&& elem : dataRegion) {
        auto& colName = elem.fieldNameStringData();
        if (colName == _spec.timeField()) {
            // Skip adding a FieldIterator for the timeField since the timestamp value from
            // _timeFieldIter can be placed accordingly in the materialized measurement.
            continue;
        }

        // Includes a field when '_spec.behavior()' is 'kInclude' and it's found in 'fieldSet' or
        // _spec.behavior() is 'kExclude' and it's not found in 'fieldSet'.
        if (determineIncludeField(
                colName, _spec.behavior(), fieldsToIncludeExcludeDuringUnpack())) {
            _unpackingImpl->addField(elem);
        }
    }

    // Update computed meta projections with values from this bucket.
    for (auto&& name : _spec.computedMetaProjFields()) {
        _computedMetaProjections[name] = _bucket[name];
    }

    // Save the measurement count for the bucket.
    _numberOfMeasurements = _unpackingImpl->measurementCount(timeFieldElem);
    _hasNext = _numberOfMeasurements > 0;
}

int BucketUnpacker::computeMeasurementCount(const BSONObj& bucket, StringData timeField) {
    auto&& controlField = bucket[timeseries::kBucketControlFieldName];
    uassert(5857904,
            "The $_internalUnpackBucket stage requires 'control' object to be present",
            controlField && controlField.type() == BSONType::Object);

    auto&& versionField = controlField.Obj()[timeseries::kBucketControlVersionFieldName];
    uassert(5857905,
            "The $_internalUnpackBucket stage requires 'control.version' field to be present",
            versionField && isNumericBSONType(versionField.type()));

    auto&& dataField = bucket[timeseries::kBucketDataFieldName];
    if (!dataField || dataField.type() != BSONType::Object)
        return 0;

    auto&& time = dataField.Obj()[timeField];
    if (!time) {
        return 0;
    }

    auto version = versionField.Number();
    if (version == 1) {
        return BucketUnpackerV1::computeElementCountFromTimestampObjSize(time.objsize());
    } else if (version == 2) {
        auto countField = controlField.Obj()[timeseries::kBucketControlCountFieldName];
        if (countField && isNumericBSONType(countField.type())) {
            return static_cast<int>(countField.Number());
        }

        return BSONColumn(time).size();
    } else {
        uasserted(5857901, "Invalid bucket version");
    }
}

void BucketUnpacker::determineIncludeTimeField() {
    const bool isInclude = _spec.behavior() == BucketSpec::Behavior::kInclude;
    const bool fieldSetContainsTime =
        _spec.fieldSet().find(_spec.timeField()) != _spec.fieldSet().end();

    const auto& metaProjFields = _spec.computedMetaProjFields();
    const bool metaProjContains = metaProjFields.find(_spec.timeField()) != metaProjFields.cend();

    // If computedMetaProjFields contains the time field, we exclude it from unpacking no matter
    // what, since it will be overwritten anyway.
    _includeTimeField = isInclude == fieldSetContainsTime && !metaProjContains;
}

void BucketUnpacker::eraseMetaFromFieldSetAndDetermineIncludeMeta() {
    if (!_spec.metaField() ||
        _spec.computedMetaProjFields().find(*_spec.metaField()) !=
            _spec.computedMetaProjFields().cend()) {
        _includeMetaField = false;
    } else if (auto itr = _spec.fieldSet().find(*_spec.metaField());
               itr != _spec.fieldSet().end()) {
        _spec.removeIncludeExcludeField(*_spec.metaField());
        _includeMetaField = _spec.behavior() == BucketSpec::Behavior::kInclude;
    } else {
        _includeMetaField = _spec.behavior() == BucketSpec::Behavior::kExclude;
    }
}

void BucketUnpacker::eraseExcludedComputedMetaProjFields() {
    if (_spec.behavior() == BucketSpec::Behavior::kExclude) {
        for (const auto& field : _spec.fieldSet()) {
            _spec.eraseFromComputedMetaProjFields(field);
        }
    }
}

void BucketUnpacker::setBucketSpec(BucketSpec&& bucketSpec) {
    _spec = std::move(bucketSpec);

    eraseMetaFromFieldSetAndDetermineIncludeMeta();
    determineIncludeTimeField();
    eraseExcludedComputedMetaProjFields();

    _includeMinTimeAsMetadata = _spec.includeMinTimeAsMetadata;
    _includeMaxTimeAsMetadata = _spec.includeMaxTimeAsMetadata;
}

void BucketUnpacker::setIncludeMinTimeAsMetadata() {
    _includeMinTimeAsMetadata = true;
}

void BucketUnpacker::setIncludeMaxTimeAsMetadata() {
    _includeMaxTimeAsMetadata = true;
}

const std::set<std::string>& BucketUnpacker::fieldsToIncludeExcludeDuringUnpack() {
    if (_unpackFieldsToIncludeExclude) {
        return *_unpackFieldsToIncludeExclude;
    }

    _unpackFieldsToIncludeExclude = std::set<std::string>();
    const auto& metaProjFields = _spec.computedMetaProjFields();
    if (_spec.behavior() == BucketSpec::Behavior::kInclude) {
        // For include, we unpack fieldSet - metaProjFields.
        for (auto&& field : _spec.fieldSet()) {
            if (metaProjFields.find(field) == metaProjFields.cend()) {
                _unpackFieldsToIncludeExclude->insert(field);
            }
        }
    } else {
        // For exclude, we unpack everything but fieldSet + metaProjFields.
        _unpackFieldsToIncludeExclude->insert(_spec.fieldSet().begin(), _spec.fieldSet().end());
        _unpackFieldsToIncludeExclude->insert(metaProjFields.begin(), metaProjFields.end());
    }

    return *_unpackFieldsToIncludeExclude;
}

const std::set<StringData> BucketUnpacker::reservedBucketFieldNames = {
    timeseries::kBucketIdFieldName,
    timeseries::kBucketDataFieldName,
    timeseries::kBucketMetaFieldName,
    timeseries::kBucketControlFieldName};

}  // namespace mongo
