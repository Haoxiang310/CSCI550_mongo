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

#include "mongo/db/pipeline/abt/match_expression_visitor.h"
#include "mongo/db/matcher/expression_always_boolean.h"
#include "mongo/db/matcher/expression_array.h"
#include "mongo/db/matcher/expression_expr.h"
#include "mongo/db/matcher/expression_geo.h"
#include "mongo/db/matcher/expression_internal_bucket_geo_within.h"
#include "mongo/db/matcher/expression_internal_expr_comparison.h"
#include "mongo/db/matcher/expression_leaf.h"
#include "mongo/db/matcher/expression_text.h"
#include "mongo/db/matcher/expression_text_noop.h"
#include "mongo/db/matcher/expression_tree.h"
#include "mongo/db/matcher/expression_type.h"
#include "mongo/db/matcher/expression_visitor.h"
#include "mongo/db/matcher/expression_where.h"
#include "mongo/db/matcher/expression_where_noop.h"
#include "mongo/db/matcher/match_expression_walker.h"
#include "mongo/db/matcher/schema/expression_internal_schema_all_elem_match_from_index.h"
#include "mongo/db/matcher/schema/expression_internal_schema_allowed_properties.h"
#include "mongo/db/matcher/schema/expression_internal_schema_cond.h"
#include "mongo/db/matcher/schema/expression_internal_schema_eq.h"
#include "mongo/db/matcher/schema/expression_internal_schema_fmod.h"
#include "mongo/db/matcher/schema/expression_internal_schema_match_array_index.h"
#include "mongo/db/matcher/schema/expression_internal_schema_max_items.h"
#include "mongo/db/matcher/schema/expression_internal_schema_max_length.h"
#include "mongo/db/matcher/schema/expression_internal_schema_max_properties.h"
#include "mongo/db/matcher/schema/expression_internal_schema_min_items.h"
#include "mongo/db/matcher/schema/expression_internal_schema_min_length.h"
#include "mongo/db/matcher/schema/expression_internal_schema_min_properties.h"
#include "mongo/db/matcher/schema/expression_internal_schema_object_match.h"
#include "mongo/db/matcher/schema/expression_internal_schema_root_doc_eq.h"
#include "mongo/db/matcher/schema/expression_internal_schema_unique_items.h"
#include "mongo/db/matcher/schema/expression_internal_schema_xor.h"
#include "mongo/db/pipeline/abt/agg_expression_visitor.h"
#include "mongo/db/pipeline/abt/expr_algebrizer_context.h"
#include "mongo/db/pipeline/abt/utils.h"
#include "mongo/db/query/optimizer/utils/utils.h"

namespace mongo::optimizer {

class ABTMatchExpressionVisitor : public MatchExpressionConstVisitor {
public:
    ABTMatchExpressionVisitor(ExpressionAlgebrizerContext& ctx, const bool allowAggExpressions)
        : _prefixId(), _allowAggExpressions(allowAggExpressions), _ctx(ctx) {}

    void visit(const AlwaysFalseMatchExpression* expr) override {
        generateBoolConstant(false);
    }

    void visit(const AlwaysTrueMatchExpression* expr) override {
        generateBoolConstant(true);
    }

    void visit(const AndMatchExpression* expr) override {
        visitAndOrExpression<PathComposeM, true>(expr);
    }

    void visit(const BitsAllClearMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const BitsAllSetMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const BitsAnyClearMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const BitsAnySetMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const ElemMatchObjectMatchExpression* expr) override {
        generateElemMatch<false /*isValueElemMatch*/>(expr);
    }

    void visit(const ElemMatchValueMatchExpression* expr) override {
        generateElemMatch<true /*isValueElemMatch*/>(expr);
    }

    void visit(const EqualityMatchExpression* expr) override {
        generateSimpleComparison(expr, Operations::Eq);
    }

    void visit(const ExistsMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const ExprMatchExpression* expr) override {
        uassert(6624246, "Cannot generate an agg expression in this context", _allowAggExpressions);

        ABT result = generateAggExpression(
            expr->getExpression().get(), _ctx.getRootProjection(), _ctx.getUniqueIdPrefix());
        _ctx.push<PathConstant>(generateCoerceToBool(std::move(result), getNextId("coerceToBool")));
    }

    void visit(const GTEMatchExpression* expr) override {
        generateSimpleComparison(expr, Operations::Gte);
    }

    void visit(const GTMatchExpression* expr) override {
        generateSimpleComparison(expr, Operations::Gt);
    }

    void visit(const GeoMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const GeoNearMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const InMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const InternalBucketGeoWithinMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const InternalExprEqMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const InternalExprGTMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const InternalExprGTEMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const InternalExprLTMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const InternalExprLTEMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const InternalSchemaAllElemMatchFromIndexMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const InternalSchemaAllowedPropertiesMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const InternalSchemaBinDataEncryptedTypeExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const InternalSchemaBinDataFLE2EncryptedTypeExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const InternalSchemaBinDataSubTypeExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const InternalSchemaCondMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const InternalSchemaEqMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const InternalSchemaFmodMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const InternalSchemaMatchArrayIndexMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const InternalSchemaMaxItemsMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const InternalSchemaMaxLengthMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const InternalSchemaMaxPropertiesMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const InternalSchemaMinItemsMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const InternalSchemaMinLengthMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const InternalSchemaMinPropertiesMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const InternalSchemaObjectMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const InternalSchemaRootDocEqMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const InternalSchemaTypeExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const InternalSchemaUniqueItemsMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const InternalSchemaXorMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const LTEMatchExpression* expr) override {
        generateSimpleComparison(expr, Operations::Lte);
    }

    void visit(const LTMatchExpression* expr) override {
        generateSimpleComparison(expr, Operations::Lt);
    }

    void visit(const ModMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const NorMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const NotMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const OrMatchExpression* expr) override {
        visitAndOrExpression<PathComposeA, false>(expr);
    }

    void visit(const RegexMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const SizeMatchExpression* expr) override {
        const std::string lambdaProjName = _prefixId.getNextId("lambda_sizeMatch");
        ABT result = make<PathLambda>(make<LambdaAbstraction>(
            lambdaProjName,
            make<BinaryOp>(
                Operations::Eq,
                make<FunctionCall>("getArraySize", makeSeq(make<Variable>(lambdaProjName))),
                Constant::int64(expr->getData()))));

        if (!expr->path().empty()) {
            // No traverse.
            result = translateFieldPath(
                FieldPath(expr->path().toString()),
                std::move(result),
                [](const std::string& fieldName, const bool /*isLastElement*/, ABT input) {
                    return make<PathGet>(fieldName, std::move(input));
                });
        }
        _ctx.push(std::move(result));
    }

    void visit(const TextMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const TextNoOpMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const TwoDPtInAnnulusExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const TypeMatchExpression* expr) override {
        const std::string lambdaProjName = _prefixId.getNextId("lambda_typeMatch");
        ABT result = make<PathLambda>(make<LambdaAbstraction>(
            lambdaProjName,
            make<FunctionCall>("typeMatch",
                               makeSeq(make<Variable>(lambdaProjName),
                                       Constant::int32(expr->typeSet().getBSONTypeMask())))));

        if (!expr->path().empty()) {
            result = generateFieldPath(FieldPath(expr->path().toString()), std::move(result));
        }
        _ctx.push(std::move(result));
    }

    void visit(const WhereMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

    void visit(const WhereNoOpMatchExpression* expr) override {
        unsupportedExpression(expr);
    }

private:
    void generateBoolConstant(const bool value) {
        _ctx.push<PathConstant>(Constant::boolean(value));
    }

    template <bool isValueElemMatch>
    void generateElemMatch(const ArrayMatchingMatchExpression* expr) {
        // Returns true if at least one sub-objects matches the condition.

        const size_t childCount = expr->numChildren();
        if (childCount == 0) {
            _ctx.push(Constant::boolean(true));
        }

        _ctx.ensureArity(childCount);
        ABT result = _ctx.pop();
        for (size_t i = 1; i < childCount; i++) {
            maybeComposePath(result, _ctx.pop());
        }
        if constexpr (!isValueElemMatch) {
            // Make sure we consider only objects as elements of the array.
            maybeComposePath(result, make<PathObj>());
        }
        result = make<PathTraverse>(std::move(result));

        // Make sure we consider only arrays fields on the path.
        maybeComposePath(result, make<PathArr>());

        if (!expr->path().empty()) {
            result = translateFieldPath(
                FieldPath{expr->path().toString()},
                std::move(result),
                [&](const std::string& fieldName, const bool isLastElement, ABT input) {
                    if (!isLastElement) {
                        input = make<PathTraverse>(std::move(input));
                    }
                    return make<PathGet>(fieldName, std::move(input));
                });
        }

        _ctx.push(std::move(result));
    }

    /**
     * Return the minimum or maximum value for the "class" of values represented by the input
     * constant. Used to support type bracketing.
     */
    template <bool isMin>
    std::pair<boost::optional<ABT>, bool> getMinMaxBoundForType(const sbe::value::TypeTags& tag) {
        if (isNumber(tag)) {
            if constexpr (isMin) {
                return {Constant::fromDouble(std::numeric_limits<double>::quiet_NaN()), true};
            } else {
                return {Constant::str(""), false};
            };
        } else if (isStringOrSymbol(tag)) {
            if constexpr (isMin) {
                return {Constant::str(""), true};
            } else {
                // TODO: we need limit string from above.
                return {boost::none, false};
            }
        } else if (tag == sbe::value::TypeTags::Null) {
            // Same bound above and below.
            return {Constant::null(), true};
        } else {
            // TODO: compute bounds for other types based on bsonobjbuilder.cpp.
            return {boost::none, false};
        }

        MONGO_UNREACHABLE;
    }

    ABT generateFieldPath(const FieldPath& fieldPath, ABT initial) {
        return translateFieldPath(
            fieldPath,
            std::move(initial),
            [](const std::string& fieldName, const bool /*isLastElement*/, ABT input) {
                return make<PathGet>(fieldName, make<PathTraverse>(std::move(input)));
            });
    }

    void generateSimpleComparison(const ComparisonMatchExpressionBase* expr, const Operations op) {
        auto [tag, val] = convertFrom(Value(expr->getData()));
        ABT result = make<PathCompare>(op, make<Constant>(tag, val));

        switch (op) {
            case Operations::Lt:
            case Operations::Lte: {
                auto&& [constant, inclusive] = getMinMaxBoundForType<true /*isMin*/>(tag);
                if (constant) {
                    maybeComposePath(result,
                                     make<PathCompare>(inclusive ? Operations::Gte : Operations::Gt,
                                                       std::move(constant.get())));
                }
                break;
            }

            case Operations::Gt:
            case Operations::Gte: {
                auto&& [constant, inclusive] = getMinMaxBoundForType<false /*isMin*/>(tag);
                if (constant) {
                    maybeComposePath(result,
                                     make<PathCompare>(inclusive ? Operations::Lte : Operations::Lt,
                                                       std::move(constant.get())));
                }
                break;
            }

            default:
                break;
        }

        if (!expr->path().empty()) {
            result = generateFieldPath(FieldPath(expr->path().toString()), std::move(result));
        }
        _ctx.push(std::move(result));
    }

    template <class Composition, bool defaultResult>
    void visitAndOrExpression(const ListOfMatchExpression* expr) {
        const size_t childCount = expr->numChildren();
        if (childCount == 0) {
            generateBoolConstant(defaultResult);
            return;
        }
        if (childCount == 1) {
            return;
        }

        ABT node = _ctx.pop();
        for (size_t i = 0; i < childCount - 1; i++) {
            node = make<Composition>(_ctx.pop(), std::move(node));
        }
        _ctx.push(std::move(node));
    }

    std::string getNextId(const std::string& key) {
        return _ctx.getUniqueIdPrefix() + "_" + _prefixId.getNextId(key);
    }

    void unsupportedExpression(const MatchExpression* expr) const {
        uasserted(ErrorCodes::InternalErrorNotSupported,
                  str::stream() << "Match expression is not supported: " << expr->matchType());
    }

    PrefixId _prefixId;

    // If we are parsing a partial index filter, we don't allow match expressions.
    const bool _allowAggExpressions;

    // We don't own this
    ExpressionAlgebrizerContext& _ctx;
};

ABT generateMatchExpression(const MatchExpression* expr,
                            const bool allowAggExpressions,
                            const std::string& rootProjection,
                            const std::string& uniqueIdPrefix) {
    ExpressionAlgebrizerContext ctx(
        false /*assertExprSort*/, true /*assertPathSort*/, rootProjection, uniqueIdPrefix);
    ABTMatchExpressionVisitor visitor(ctx, allowAggExpressions);
    MatchExpressionWalker walker(nullptr /*preVisitor*/, nullptr /*inVisitor*/, &visitor);
    tree_walker::walk<true, MatchExpression>(expr, &walker);
    return ctx.pop();
}

}  // namespace mongo::optimizer
