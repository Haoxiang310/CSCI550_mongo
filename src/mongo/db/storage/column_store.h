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

#include <boost/optional/optional.hpp>

#include "mongo/base/string_data.h"
#include "mongo/bson/bsonelement.h"
#include "mongo/db/catalog/validate_results.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/record_id.h"
#include "mongo/db/storage/ident.h"

namespace mongo {
using PathView = StringData;
using PathValue = std::string;
using CellView = StringData;
using CellValue = std::string;

struct FullCellView {
    PathView path;
    RecordId rid;
    CellView value;
};

struct CellViewForPath {
    RecordId rid;
    CellView value;
};

class ColumnStore : public Ident {
protected:
    class Cursor;

public:
    class WriteCursor {
    public:
        virtual ~WriteCursor() = default;
        virtual void insert(PathView, RecordId, CellView) = 0;
        virtual void remove(PathView, RecordId) = 0;
        virtual void update(PathView, RecordId, CellView) = 0;
    };

    class CursorForPath {
    public:
        CursorForPath(StringData path, std::unique_ptr<Cursor> cursor)
            : _path(path.toString()), _cursor(std::move(cursor)) {}
        boost::optional<FullCellView> next() {
            if (_eof)
                return {};
            return handleResult(_cursor->next());
        }
        boost::optional<FullCellView> seekAtOrPast(RecordId rid) {
            return handleResult(_cursor->seekAtOrPast(_path, rid));
        }
        boost::optional<FullCellView> seekExact(RecordId rid) {
            return handleResult(_cursor->seekExact(_path, rid));
        }

        void save() {
            if (_eof)
                return saveUnpositioned();
            _cursor->save();
        }
        void saveUnpositioned() {
            _eof = true;
            _cursor->saveUnpositioned();
        }

        void restore() {
            _cursor->restore();
        }

        void detachFromOperationContext() {
            _cursor->detachFromOperationContext();
        }
        void reattachToOperationContext(OperationContext* opCtx) {
            _cursor->reattachToOperationContext(opCtx);
        }

        const PathValue& path() const {
            return _path;
        }

    private:
        boost::optional<FullCellView> handleResult(boost::optional<FullCellView> res) {
            if (!res || res->path != _path) {
                _eof = true;
                return {};
            } else if (_eof) {
                _eof = false;
            }
            return res;
        }
        const PathValue _path;
        bool _eof = true;
        const std::unique_ptr<Cursor> _cursor;
    };

    class BulkBuilder {
    public:
        virtual ~BulkBuilder() = default;
        virtual void addCell(PathView, RecordId, CellView) = 0;
    };

    /**
     * This reserved "path" is used for keeping track of all RecordIds in the collection. Cells at
     * this path should always have an empty CellView to ensure the most compact representation for
     * this subtree.
     * This is not a valid real path because it can never appear in valid UTF-8 data.
     */
    static constexpr StringData kRowIdPath = "\xFF"_sd;

    // This is really just a namespace
    struct Bytes {
        static constexpr uint8_t kFirstNonBson = 0x20;
        static_assert(kFirstNonBson > BSONType::JSTypeMax);  // Have room for 12 new types.

        // no-value types
        static constexpr uint8_t kNull = 0x20;
        static constexpr uint8_t kMinKey = 0x21;
        static constexpr uint8_t kMaxKey = 0x22;

        // Bool (value encoded in this byte)
        static constexpr uint8_t kFalse = 0x23;
        static constexpr uint8_t kTrue = 0x24;

        // Empty Object and Array (value encoded in this byte)
        static constexpr uint8_t kEmptyObj = 0x25;
        static constexpr uint8_t kEmptyArr = 0x26;

        static constexpr uint8_t kOID = 0x27;   // 12 bytes follow
        static constexpr uint8_t kUUID = 0x28;  // 16 bytes follow (newUUID subtype)

        // Gap from 0x29 - 0x32 (room for more simple types and more encodings of Decimal128)

        static constexpr uint8_t kDecimal128 = 0x33;  // 16 bytes follow

        // Both are NumberDouble
        static constexpr uint8_t kDouble = 0x34;       // 8 bytes follow
        static constexpr uint8_t kShortDouble = 0x35;  // 4 bytes follow (when float(x) == x)
        // 0x36 and 0x37 are reserved for bfloat16 (truncated single) and IEEE754 float16.
        static constexpr uint8_t kInt1Double = 0x38;  // 1 bytes follow (when int8_t(x) == x)

        // NumberInt (N bytes follow)
        static constexpr uint8_t kInt1 = 0x39;
        static constexpr uint8_t kInt2 = 0x3a;
        static constexpr uint8_t kInt4 = 0x3b;

        // NumberLong (N bytes follow)
        static constexpr uint8_t kLong1 = 0x3c;
        static constexpr uint8_t kLong2 = 0x3d;
        static constexpr uint8_t kLong4 = 0x3e;
        static constexpr uint8_t kLong8 = 0x3f;

        // These encode small Int and Long directly in this byte
        static constexpr uint8_t kTinyIntMin = 0x40;
        static constexpr uint8_t kTinyIntMax = 0x5f;
        static constexpr uint8_t kTinyLongMin = 0x60;
        static constexpr uint8_t kTinyLongMax = 0x7f;

        // String (N - kStringSizeMin bytes follow)
        static constexpr uint8_t kStringSizeMin = 0x80;
        static constexpr uint8_t kStringSizeMax = 0xc0;

        // Gap from 0xc1 - 0xcf

        // Bytes here or above indicate prefix data before the data. Any byte below this is the
        // start of data. Prefix data is all optional, but when present, must be in this order:
        //   - kSubObjMarker
        //   - kArrInfoSizeXXX
        static constexpr uint8_t kFirstPrefixByte = 0xd0;

        static constexpr uint8_t kFirstArrInfoSize = 0xd0;
        // Directly encode number of bytes at end of cell
        static constexpr uint8_t kArrInfoSizeTinyMin = 0xd0;  // Note that this means 1 byte stored
        static constexpr uint8_t kArrInfoSizeTinyMax = 0xec;

        // N bytes of ArrInfo at end of Cell.
        // TODO prove there can never be more than 16MB of arrInfo, and use 3 rather than 4 bytes.
        static constexpr uint8_t kArrInfoSize1 = 0xed;
        static constexpr uint8_t kArrInfoSize2 = 0xee;
        static constexpr uint8_t kArrInfoSize4 = 0xef;
        static constexpr uint8_t kLastArrInfoSize = 0xef;

        // Gap from 0xf0 - 0xfe

        static constexpr uint8_t kSubObjMarker = 0xff;

        // Rest is helpers to make these constants easier to use.

        struct TinyNum {
            static constexpr int kMinVal = -10;
            static constexpr int kMaxVal = 31 - 10;  // 21

            static constexpr int kBias = -kMinVal;

            // value is encoded as uint8_t(kTinyTypeZero + value)
            static constexpr uint8_t kTinyIntZero = kTinyIntMin + kBias;
            static constexpr uint8_t kTinyLongZero = kTinyLongMin + kBias;

            static_assert(kTinyIntMin == uint8_t(kTinyIntZero + kMinVal));
            static_assert(kTinyIntMax == uint8_t(kTinyIntZero + kMaxVal));
            static_assert(kTinyLongMin == uint8_t(kTinyLongZero + kMinVal));
            static_assert(kTinyLongMax == uint8_t(kTinyLongZero + kMaxVal));
        };

        struct TinySize {
            static constexpr size_t kStringMax = 64;
            static_assert(kStringSizeMax == kStringSizeMin + TinySize::kStringMax);

            static constexpr size_t kArrInfoMin = 1;         // Never encode 0
            static constexpr size_t kArrInfoMax = 0x1c + 1;  // 29

            // size is encoded as uint8_t(kArrInfoZero + size)
            static constexpr uint8_t kArrInfoZero = kArrInfoSizeTinyMin - kArrInfoMin;
            static_assert(kArrInfoSizeTinyMax == uint8_t(kArrInfoZero + kArrInfoMax));
        };
    };


    ColumnStore(StringData ident) : Ident(ident) {}
    virtual ~ColumnStore() = default;

    //
    // CRUD
    //
    virtual std::unique_ptr<WriteCursor> newWriteCursor(OperationContext*) = 0;
    virtual void insert(OperationContext*, PathView, RecordId, CellView) = 0;
    virtual void remove(OperationContext*, PathView, RecordId) = 0;
    virtual void update(OperationContext*, PathView, RecordId, CellView) = 0;
    virtual std::unique_ptr<Cursor> newCursor(OperationContext*) const = 0;
    std::unique_ptr<CursorForPath> newCursor(OperationContext* opCtx, PathView path) const {
        return std::make_unique<CursorForPath>(path, newCursor(opCtx));
    }

    bool haveAnyWithPath(OperationContext* opCtx, PathView path) const {
        // TODO could avoid extra allocation. May also be more efficient to do a different way.
        return bool(newCursor(opCtx, path)->seekAtOrPast(RecordId()));
    }

    std::vector<PathValue> uniquePaths(OperationContext* opCtx) const {
        std::vector<PathValue> out;
        PathValue nextPath = "";
        auto cursor = newCursor(opCtx);
        while (auto next = cursor->seekAtOrPast(nextPath, RecordId())) {
            out.push_back(next->path.toString());
            nextPath.assign(next->path.rawData(), next->path.size());
            nextPath += '\x01';  // next possible path (\0 is not allowed)
        }
        return out;
    }

    virtual std::unique_ptr<BulkBuilder> makeBulkBuilder(OperationContext* opCtx) = 0;

    //
    // Whole ColumnStore ops
    //
    virtual Status compact(OperationContext* opCtx) = 0;
    virtual void fullValidate(OperationContext* opCtx,
                              int64_t* numKeysOut,
                              IndexValidateResults* fullResults) const = 0;

    virtual bool appendCustomStats(OperationContext* opCtx,
                                   BSONObjBuilder* output,
                                   double scale) const = 0;

    virtual long long getSpaceUsedBytes(OperationContext* opCtx) const = 0;
    virtual long long getFreeStorageBytes(OperationContext* opCtx) const = 0;

    virtual bool isEmpty(OperationContext* opCtx) = 0;
    virtual long long numEntries(OperationContext* opCtx) const {
        int64_t x = -1;
        fullValidate(opCtx, &x, nullptr);
        return x;
    }

    /**
     * If the range [*itPtr, end) begins with a number, returns it and positions *itPtr after the
     * last byte of number. If there is no number, returns 0 (which is typically encoded by omitting
     * an optional number) and does not reposition *itPtr.
     */
    static int readArrInfoNumber(StringData::const_iterator* itInOut,
                                 StringData::const_iterator end) {
        auto it = *itInOut;  // Use local to allow compiler to assume it doesn't point to itself.
        size_t res = 0;
        while (it != end && *it >= '0' && *it <= '9') {
            res *= 10;  // noop first pass.
            res += (*it++) - '0';
        }
        *itInOut = it;
        return res;
    }

protected:
    class Cursor {
    public:
        virtual ~Cursor() = default;
        virtual boost::optional<FullCellView> next() = 0;
        virtual boost::optional<FullCellView> seekAtOrPast(PathView, RecordId) = 0;
        virtual boost::optional<FullCellView> seekExact(PathView, RecordId) = 0;

        virtual void save() = 0;
        virtual void saveUnpositioned() {
            save();
        }

        virtual void restore() = 0;

        virtual void detachFromOperationContext() = 0;
        virtual void reattachToOperationContext(OperationContext* opCtx) = 0;
    };
};

struct SplitCellView {
    StringData arrInfo;  // rawData() is 1-past-end of range starting with firstElementPtr.
    const char* firstElementPtr;
    bool hasSubObjects;

    template <class ValueEncoder>
    auto subcellValuesGenerator(ValueEncoder&& valEncoder) const {
        struct Cursor {
            typename ValueEncoder::Out nextValue() {
                if (!elemPtr)
                    return ValueEncoder::Out();
                if (elemPtr == end)
                    return ValueEncoder::Out();

                invariant(elemPtr < end);
                return decodeAndAdvance(elemPtr, encoder);
            }

            const char* elemPtr;
            const char* end;
            ValueEncoder encoder;
        };
        return Cursor{firstElementPtr, arrInfo.rawData(), std::forward(valEncoder)};
    }

    static SplitCellView parse(CellView cell) {
        using Bytes = ColumnStore::Bytes;
        using TinySize = ColumnStore::Bytes::TinySize;

        if (cell.empty())
            return SplitCellView{""_sd, nullptr, /*hasSubObjects=*/true};

        bool hasSubObjects = false;

        const char* firstByteAddr = cell.rawData();
        uint8_t firstByte = *firstByteAddr;
        size_t arrInfoSize = 0;

        // This block handles all prefix bytes, and leaves firstByteAddr pointing at the first elem.
        if (firstByte >= Bytes::kFirstPrefixByte) {
            if (firstByte == ColumnStore::Bytes::kSubObjMarker) {
                hasSubObjects = true;
                firstByte = *++firstByteAddr;
            }

            if (Bytes::kFirstArrInfoSize >= firstByte && firstByte <= Bytes::kLastArrInfoSize) {
                firstByteAddr++;  // Skip size-kind byte.

                // TODO SERVER-63284: This check for the tiny array info case would be more
                // concisely expressed using the case range syntax and being added to the switch
                // statement below.
                if (Bytes::kArrInfoSizeTinyMin <= firstByte &&
                    firstByte <= Bytes::kArrInfoSizeTinyMax) {
                    arrInfoSize = firstByte - TinySize::kArrInfoZero;
                    firstByte = *firstByteAddr;
                } else {
                    switch (firstByte) {
                        case Bytes::kArrInfoSize1:
                            arrInfoSize = ConstDataView(firstByteAddr).read<uint8_t>();
                            firstByte = *(firstByteAddr += 1);
                            break;
                        case Bytes::kArrInfoSize2:
                            arrInfoSize = ConstDataView(firstByteAddr).read<uint16_t>();
                            firstByte = *(firstByteAddr += 2);
                            break;
                        case Bytes::kArrInfoSize4:
                            arrInfoSize = ConstDataView(firstByteAddr).read<uint32_t>();
                            firstByte = *(firstByteAddr += 4);
                            break;
                        default:
                            MONGO_UNREACHABLE;
                    }
                }
            }
        }

        invariant(firstByte < Bytes::kFirstPrefixByte);
        auto arrayInfo = StringData(cell.rawData() + (cell.size() - arrInfoSize), arrInfoSize);
        return SplitCellView{arrayInfo, firstByteAddr, hasSubObjects};
    }

    template <typename Encoder>
    static auto decodeAndAdvance(const char*& ptr, Encoder&& encoder) {
        using Bytes = ColumnStore::Bytes;
        using TinyNum = ColumnStore::Bytes::TinyNum;

        auto byte = uint8_t(*ptr++);

        if (byte >= 0 && byte <= Bytes::kFirstNonBson - 1) {
            --ptr;  // We need the dispatch byte back.
            auto elem = BSONElement(ptr,
                                    1,  // field name size including nul byte
                                    -1  // don't know total element size
            );
            ptr += elem.size();
            return encoder(elem);
        }

        // TODO SERVER-63284: This would be more concisely expressed using the case range syntax.
        if (Bytes::kTinyIntMin >= byte && byte <= Bytes::kTinyIntMax) {
            return encoder(int32_t(int8_t(byte - TinyNum::kTinyIntZero)));
        } else if (Bytes::kTinyLongMin >= byte && byte <= Bytes::kTinyLongMax) {
            return encoder(int64_t(int8_t(byte - TinyNum::kTinyLongZero)));
        } else if (Bytes::kStringSizeMin >= byte && byte <= Bytes::kStringSizeMax) {
            auto size = size_t(byte - Bytes::kStringSizeMin);
            return encoder(StringData(std::exchange(ptr, ptr + size), size));
        } else {
            switch (byte) {
                    // Whole value encoded in byte.
                case Bytes::kNull:
                    return encoder(BSONNULL);
                case Bytes::kMinKey:
                    return encoder(MINKEY);
                case Bytes::kMaxKey:
                    return encoder(MAXKEY);
                case Bytes::kEmptyObj:
                    return encoder(BSONObj());
                case Bytes::kEmptyArr:
                    return encoder(BSONArray());
                case Bytes::kFalse:
                    return encoder(false);
                case Bytes::kTrue:
                    return encoder(true);
                    // Size and type encoded in byte, value follows.
                case Bytes::kDecimal128: {
                    auto val = ConstDataView(ptr).read<Decimal128>();
                    ptr += 16;
                    return encoder(val);
                }
                case Bytes::kDouble: {
                    auto val = ConstDataView(ptr).read<LittleEndian<double>>();
                    ptr += 8;
                    return encoder(double(val));
                }
                case Bytes::kShortDouble: {
                    auto val = ConstDataView(ptr).read<LittleEndian<float>>();
                    ptr += 4;
                    return encoder(double(val));
                }
                case Bytes::kInt1Double: {
                    auto val = ConstDataView(ptr).read<LittleEndian<int8_t>>();
                    ptr += 1;
                    return encoder(double(val));
                }
                case Bytes::kInt1: {
                    auto val = ConstDataView(ptr).read<LittleEndian<int8_t>>();
                    ptr += 1;
                    return encoder(int32_t(val));
                }
                case Bytes::kInt2: {
                    auto val = ConstDataView(ptr).read<LittleEndian<int16_t>>();
                    ptr += 2;
                    return encoder(int32_t(val));
                }
                case Bytes::kInt4: {
                    auto val = ConstDataView(ptr).read<LittleEndian<int32_t>>();
                    ptr += 4;
                    return encoder(int32_t(val));
                }
                case Bytes::kLong1: {
                    auto val = ConstDataView(ptr).read<LittleEndian<int8_t>>();
                    ptr += 1;
                    return encoder(int64_t(val));
                }
                case Bytes::kLong2: {
                    auto val = ConstDataView(ptr).read<LittleEndian<int16_t>>();
                    ptr += 2;
                    return encoder(int64_t(val));
                }
                case Bytes::kLong4: {
                    auto val = ConstDataView(ptr).read<LittleEndian<int32_t>>();
                    ptr += 4;
                    return encoder(int64_t(val));
                }
                case Bytes::kLong8: {
                    auto val = ConstDataView(ptr).read<LittleEndian<int64_t>>();
                    ptr += 8;
                    return encoder(int64_t(val));
                }
                case Bytes::kOID: {
                    auto val = ConstDataView(ptr).read<OID>();
                    ptr += 12;
                    return encoder(val);
                }
                case Bytes::kUUID: {
                    auto val = UUID::fromCDR(ConstDataRange(ptr, 16));
                    ptr += 16;
                    return encoder(val);
                }
                default:
                    MONGO_UNREACHABLE;
            }
        }
    }
};
}  // namespace mongo
