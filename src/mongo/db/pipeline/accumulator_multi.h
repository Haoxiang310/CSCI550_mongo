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

#include <deque>

#include "mongo/db/exec/sort_key_comparator.h"
#include "mongo/db/index/sort_key_generator.h"
#include "mongo/db/pipeline/accumulation_statement.h"

namespace mongo {

/**
 * An AccumulatorN picks 'n' of its input values and returns them in an array. Each derived class
 * has different criteria for how to pick values and order the final array, but any common behavior
 * shared by derived classes is implemented in this class. In particular:
 * - Initializing 'n' during 'startNewGroup'.
 * - Parsing the expressions for 'n' and 'input'.
 */
class AccumulatorN : public AccumulatorState {
public:
    enum AccumulatorType { kMinN, kMaxN, kFirstN, kLastN, kTopN, kTop, kBottomN, kBottom };

    static constexpr auto kFieldNameN = "n"_sd;
    static constexpr auto kFieldNameInput = "input"_sd;

    // Field names related to top/bottom/topN/bottomN.

    // Whereas other 'n' accumulators accept an 'input' parameter, top/bottom/topN/bottomN accept
    // 'output'. This is done in order to disambiguate the expression that will be used to
    // compute the output from the 'sortBy' expression, which will be used to order the output.
    static constexpr auto kFieldNameOutput = "output"_sd;
    // Sort specification given by user.
    static constexpr auto kFieldNameSortBy = "sortBy"_sd;
    // Array containing only the fields needed to generate a sortKey from the input document.
    static constexpr auto kFieldNameSortFields = "sortFields"_sd;
    // A sortKey already generated by a previous call to _processValue.
    static constexpr auto kFieldNameGeneratedSortKey = "generatedSortKey"_sd;

    AccumulatorN(ExpressionContext* expCtx);

    virtual AccumulatorType getAccumulatorType() const = 0;

    /**
     * Verifies that 'input' is a positive integer.
     */
    static long long validateN(const Value& input);

    void processInternal(const Value& input, bool merging) override;

    /**
     * Initialize 'n' with 'input'.
     */
    void startNewGroup(const Value& input) final;

    /**
     * Helper which appends the 'n' and 'input' fields to 'md'.
     */
    static void serializeHelper(const boost::intrusive_ptr<Expression>& initializer,
                                const boost::intrusive_ptr<Expression>& argument,
                                bool explain,
                                MutableDocument& md);

protected:
    // Parses 'args' for the 'n' and 'input' arguments that are common to the 'N' family of
    // accumulators.
    static std::tuple<boost::intrusive_ptr<Expression>, boost::intrusive_ptr<Expression>> parseArgs(
        ExpressionContext* expCtx, const BSONObj& args, VariablesParseState vps);

    // Utility to check that '_maxMemUsageBytes' isn't exceeded after 'memAdded' is counted
    // towards the total memory consumed.
    void updateAndCheckMemUsage(size_t memAdded);

    // Stores the limit of how many values we will return. This value is initialized to
    // 'boost::none' on construction and is only set during 'startNewGroup'.
    boost::optional<long long> _n;

    int _maxMemUsageBytes = 0;

private:
    virtual void _processValue(const Value& val) = 0;
};
class AccumulatorMinMaxN : public AccumulatorN {
public:
    using MinMaxSense = AccumulatorMinMax::Sense;

    AccumulatorMinMaxN(ExpressionContext* expCtx, MinMaxSense sense);

    /**
     * Verifies that 'elem' is an object, delegates argument parsing to 'AccumulatorN::parseArgs',
     * and constructs an AccumulationExpression representing $minN or $maxN depending on 's'.
     */
    template <MinMaxSense s>
    static AccumulationExpression parseMinMaxN(ExpressionContext* expCtx,
                                               BSONElement elem,
                                               VariablesParseState vps);

    /**
     * Constructs an Expression representing $minN or $maxN depending on 's'.
     */
    template <MinMaxSense s>
    static boost::intrusive_ptr<Expression> parseExpression(ExpressionContext* expCtx,
                                                            BSONElement exprElement,
                                                            const VariablesParseState& vps);

    Value getValue(bool toBeMerged) final;

    const char* getOpName() const final;

    Document serialize(boost::intrusive_ptr<Expression> initializer,
                       boost::intrusive_ptr<Expression> argument,
                       bool explain) const final;

    void reset() final;

    bool isAssociative() const final {
        return true;
    }

    bool isCommutative() const final {
        return true;
    }

private:
    void _processValue(const Value& val) final;

    ValueMultiset _set;
    MinMaxSense _sense;
};

class AccumulatorMinN : public AccumulatorMinMaxN {
public:
    static constexpr auto kName = "$minN"_sd;
    explicit AccumulatorMinN(ExpressionContext* expCtx)
        : AccumulatorMinMaxN(expCtx, MinMaxSense::kMin) {}

    static const char* getName();

    AccumulatorType getAccumulatorType() const {
        return AccumulatorType::kMinN;
    }

    static boost::intrusive_ptr<AccumulatorState> create(ExpressionContext* expCtx);
};

class AccumulatorMaxN : public AccumulatorMinMaxN {
public:
    static constexpr auto kName = "$maxN"_sd;
    explicit AccumulatorMaxN(ExpressionContext* expCtx)
        : AccumulatorMinMaxN(expCtx, MinMaxSense::kMax) {}

    static const char* getName();

    AccumulatorType getAccumulatorType() const override {
        return AccumulatorType::kMaxN;
    }

    static boost::intrusive_ptr<AccumulatorState> create(ExpressionContext* expCtx);
};

class AccumulatorFirstLastN : public AccumulatorN {
public:
    enum Sense : int {
        kFirst = 1,
        kLast = -1,
    };

    AccumulatorFirstLastN(ExpressionContext* expCtx, Sense variant);

    /**
     * Verifies that 'elem' is an object, delegates argument parsing to 'AccumulatorN::parseArgs',
     * and constructs an AccumulationExpression representing $firstN or $lastN depending on 's'.
     */
    template <Sense s>
    static AccumulationExpression parseFirstLastN(ExpressionContext* expCtx,
                                                  BSONElement elem,
                                                  VariablesParseState vps);

    const char* getOpName() const final;

    Document serialize(boost::intrusive_ptr<Expression> initializer,
                       boost::intrusive_ptr<Expression> argument,
                       bool explain) const final;

    void reset() final;

    bool isAssociative() const final {
        return true;
    }

    bool isCommutative() const final {
        return true;
    }

    /**
     * Constructs an Expression representing $firstN or $lastN depending on 's'.
     */
    template <Sense s>
    static boost::intrusive_ptr<Expression> parseExpression(ExpressionContext* expCtx,
                                                            BSONElement exprElement,
                                                            const VariablesParseState& vps);

    Value getValue(bool toBeMerged) final;

private:
    // firstN/lastN do NOT ignore null values.
    void _processValue(const Value& val) final;

    std::deque<Value> _deque;
    Sense _variant;
};

class AccumulatorFirstN : public AccumulatorFirstLastN {
public:
    static constexpr auto kName = "$firstN"_sd;
    explicit AccumulatorFirstN(ExpressionContext* expCtx)
        : AccumulatorFirstLastN(expCtx, Sense::kFirst) {}

    static const char* getName();

    AccumulatorType getAccumulatorType() const override {
        return AccumulatorType::kFirstN;
    }

    static boost::intrusive_ptr<AccumulatorState> create(ExpressionContext* expCtx);
};

class AccumulatorLastN : public AccumulatorFirstLastN {
public:
    static constexpr auto kName = "$lastN"_sd;
    explicit AccumulatorLastN(ExpressionContext* expCtx)
        : AccumulatorFirstLastN(expCtx, Sense::kLast) {}

    static const char* getName();

    AccumulatorType getAccumulatorType() const override {
        return AccumulatorType::kLastN;
    }

    static boost::intrusive_ptr<AccumulatorState> create(ExpressionContext* expCtx);
};

enum TopBottomSense {
    kTop,
    kBottom,
};

template <TopBottomSense sense, bool single>
class AccumulatorTopBottomN : public AccumulatorN {
public:
    // pair of (sortKey, output) for storing in AccumulatorTopBottomN's internal multimap.
    using KeyOutPair = std::pair<Value, Value>;

    AccumulatorTopBottomN(ExpressionContext* expCtx, SortPattern sp, bool isRemovable);

    static boost::intrusive_ptr<AccumulatorState> create(ExpressionContext* expCtx,
                                                         BSONObj sortPattern,
                                                         bool isRemovable = false);
    static boost::intrusive_ptr<AccumulatorState> create(ExpressionContext* expCtx,
                                                         SortPattern sortPattern);

    /**
     * Verifies that 'elem' is an object, delegates argument parsing to 'accumulatorNParseArgs',
     * and constructs an AccumulationExpression representing $top, $bottom, $topN or $bottomN
     * depending on 'sense' and 'single'.
     */
    static AccumulationExpression parseTopBottomN(ExpressionContext* expCtx,
                                                  BSONElement elem,
                                                  VariablesParseState vps);

    static constexpr StringData getName() {
        if constexpr (single) {
            if constexpr (sense == TopBottomSense::kTop) {
                return "$top"_sd;
            } else {
                return "$bottom"_sd;
            }
        } else {
            if constexpr (sense == TopBottomSense::kTop) {
                return "$topN"_sd;
            } else {
                return "$bottomN"_sd;
            }
        }
    }

    void processInternal(const Value& input, bool merging) final;

    Value getValueConst(bool toBeMerged) const;
    Value getValue(bool toBeMerged) final {
        return getValueConst(toBeMerged);
    };

    const char* getOpName() const final;

    Document serialize(boost::intrusive_ptr<Expression> initializer,
                       boost::intrusive_ptr<Expression> argument,
                       bool explain) const final;

    void reset() final;

    bool isAssociative() const final {
        return true;
    }

    /**
     * Used for removable version of this operator as a window function.
     */
    void remove(const Value& val);

    const SortPattern getSortPattern() const {
        return _sortPattern;
    }

    AccumulatorType getAccumulatorType() const override {
        if constexpr (single) {
            if constexpr (sense == TopBottomSense::kTop) {
                return AccumulatorType::kTop;
            } else {
                return AccumulatorType::kBottom;
            }
        } else {
            if constexpr (sense == TopBottomSense::kTop) {
                return AccumulatorType::kTopN;
            } else {
                return AccumulatorType::kBottomN;
            }
        }
    }

private:
    // top/bottom/topN/bottomN do NOT ignore null values, but MISSING values will be promoted to
    // null so the users see them.
    void _processValue(const Value& val);

    std::pair<Value, Value> _genKeyOutPair(const Value& val);

    // Set to true if we are allowed to call remove().
    bool _isRemovable;

    SortPattern _sortPattern;
    // internalSortPattern needs to be computed based on _sortPattern before the following can be
    // initialized.
    boost::optional<SortKeyGenerator> _sortKeyGenerator;
    boost::optional<SortKeyComparator> _sortKeyComparator;
    boost::optional<std::multimap<Value, Value, std::function<bool(Value, Value)>>> _map;
};

extern template class AccumulatorTopBottomN<TopBottomSense::kBottom, false>;
extern template class AccumulatorTopBottomN<TopBottomSense::kBottom, true>;
extern template class AccumulatorTopBottomN<TopBottomSense::kTop, false>;
extern template class AccumulatorTopBottomN<TopBottomSense::kTop, true>;

}  // namespace mongo
