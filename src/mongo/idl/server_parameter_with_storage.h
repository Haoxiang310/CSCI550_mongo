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

#pragma once
/* The contents of this file are meant to be used by
 * code generated from idlc.py.
 *
 * It should not be instantiated directly from mongo code,
 * rather parameters should be defined in .idl files.
 */

#include <functional>
#include <string>

#include "mongo/base/parse_number.h"
#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/idl/idl_parser.h"
#include "mongo/idl/server_parameter.h"
#include "mongo/platform/atomic_proxy.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/util/str.h"
#include "mongo/util/synchronized_value.h"

namespace mongo {
namespace idl_server_parameter_detail {

template <typename T>
inline StatusWith<T> coerceFromString(StringData str) {
    T value;
    Status status = NumberParser{}(str, &value);
    if (!status.isOK()) {
        return status;
    }
    return value;
}

template <>
inline StatusWith<bool> coerceFromString<bool>(StringData str) {
    if ((str == "1") || (str == "true")) {
        return true;
    }
    if ((str == "0") || (str == "false")) {
        return false;
    }
    return {ErrorCodes::BadValue, "Value is not a valid boolean"};
}

template <>
inline StatusWith<std::string> coerceFromString<std::string>(StringData str) {
    return str.toString();
}

template <>
inline StatusWith<std::vector<std::string>> coerceFromString<std::vector<std::string>>(
    StringData str) {
    std::vector<std::string> v;
    str::splitStringDelim(str.toString(), &v, ',');
    return v;
}

// Predicate rules for bounds conditions.
struct GT {
    static constexpr StringData description = "greater than"_sd;
    template <typename T, typename U>
    static constexpr bool evaluate(const T& a, const U& b) {
        return a > b;
    }
};

struct LT {
    static constexpr StringData description = "less than"_sd;
    template <typename T, typename U>
    static constexpr bool evaluate(const T& a, const U& b) {
        return a < b;
    }
};

struct GTE {
    static constexpr StringData description = "greater than or equal to"_sd;
    template <typename T, typename U>
    static constexpr bool evaluate(const T& a, const U& b) {
        return a >= b;
    }
};

struct LTE {
    static constexpr StringData description = "less than or equal to"_sd;
    template <typename T, typename U>
    static constexpr bool evaluate(const T& a, const U& b) {
        return a <= b;
    }
};

// Wrapped type unwrappers.
// e.g. Given AtomicWord<int>, get std::int32_t and normalized store/load methods.
template <typename U>
struct storage_wrapper;

template <typename U>
struct storage_wrapper<AtomicWord<U>> {
    using type = U;
    storage_wrapper(AtomicWord<U>& storage) : _storage(storage), _defaultValue(storage.load()) {}

    void store(const U& value) {
        _storage.store(value);
    }

    U load() const {
        return _storage.load();
    }

    void reset() {
        _storage.store(_defaultValue);
    }

    // Not thread-safe, will only be called once at most per ServerParameter in its initialization
    // block.
    void setDefault(const U& value) {
        _defaultValue = value;
    }

private:
    AtomicWord<U>& _storage;

    // Copy of original value to be read from during resets.
    U _defaultValue;
};

// Covers AtomicDouble
template <typename U, typename P>
struct storage_wrapper<AtomicProxy<U, P>> {
    using type = U;
    storage_wrapper(AtomicProxy<U, P>& storage)
        : _storage(storage), _defaultValue(storage.load()) {}

    void store(const U& value) {
        _storage.store(value);
    }

    U load() const {
        return _storage.load();
    }

    void reset() {
        _storage.store(_defaultValue);
    }

    // Not thread-safe, will only be called once at most per ServerParameter in its initialization
    // block.
    void setDefault(const U& value) {
        _defaultValue = value;
    }

private:
    AtomicProxy<U, P>& _storage;

    // Copy of original value to be read from during resets.
    U _defaultValue;
};

template <typename U>
struct storage_wrapper<synchronized_value<U>> {
    using type = U;
    storage_wrapper(synchronized_value<U>& storage) : _storage(storage), _defaultValue(*storage) {}

    void store(const U& value) {
        *_storage = value;
    }

    U load() const {
        return *_storage;
    }

    void reset() {
        *_storage = _defaultValue;
    }

    // Not thread-safe, will only be called once at most per ServerParameter in its initialization
    // block.
    void setDefault(const U& value) {
        _defaultValue = value;
    }

private:
    synchronized_value<U>& _storage;

    // Copy of original value to be read from during resets.
    U _defaultValue;
};

// All other types will use a mutex to synchronize in a threadsafe manner.
template <typename U>
struct storage_wrapper {
    using type = U;
    storage_wrapper(U& storage) : _storage(storage), _defaultValue(storage) {}

    void store(const U& value) {
        stdx::lock_guard<Latch> lg(_storageMutex);
        _storage = value;
    }

    U load() const {
        stdx::lock_guard<Latch> lg(_storageMutex);
        return _storage;
    }

    void reset() {
        stdx::lock_guard<Latch> lg(_storageMutex);
        _storage = _defaultValue;
    }

    // Not thread-safe, will only be called once at most per ServerParameter in its initialization
    // block.
    void setDefault(const U& value) {
        _defaultValue = value;
    }

private:
    mutable Mutex _storageMutex = MONGO_MAKE_LATCH("IDLServerParameterWithStorage:_storageMutex");
    U& _storage;

    // Copy of original value to be read from during resets.
    U _defaultValue;
};

}  // namespace idl_server_parameter_detail

/**
 * Used to check if the parameter type has the getClusterServerParameter method, which proves
 * that ClusterServerParameter is inline chained to it.
 */
template <typename T>
using HasClusterServerParameter = decltype(std::declval<T>().getClusterServerParameter());
template <typename T>
constexpr bool hasClusterServerParameter = stdx::is_detected_v<HasClusterServerParameter, T>;

/**
 * Specialization of ServerParameter used by IDL generator.
 */
template <ServerParameterType paramType, typename T>
class IDLServerParameterWithStorage : public ServerParameter {
private:
    using SPT = ServerParameterType;
    using SW = idl_server_parameter_detail::storage_wrapper<T>;

public:
    using element_type = typename SW::type;

    IDLServerParameterWithStorage(StringData name, T& storage)
        : ServerParameter(name, paramType), _storage(storage) {
        constexpr bool notClusterParameter = (paramType != SPT::kClusterWide);
        // Compile-time assertion to ensure that IDL-defined in-memory storage for CSPs are
        // chained to the ClusterServerParameter base type.
        static_assert(
            notClusterParameter || hasClusterServerParameter<T>,
            "Cluster server parameter storage must be chained from ClusterServerParameter");
    }

    Status validateValue(const element_type& newValue) const {
        for (const auto& validator : _validators) {
            const auto status = validator(newValue);
            if (!status.isOK()) {
                return status;
            }
        }
        return Status::OK();
    }

    /**
     * Convenience wrapper for storing a value.
     */
    Status setValue(const element_type& newValue) {
        if (auto status = validateValue(newValue); !status.isOK()) {
            return status;
        }

        _storage.store(newValue);

        if (_onUpdate) {
            return _onUpdate(newValue);
        }

        return Status::OK();
    }

    /**
     * Convenience wrapper for fetching value from storage.
     */
    element_type getValue() const {
        return _storage.load();
    }

    /**
     * Allows the default value stored in the underlying storage_wrapper to be changed exactly once
     * after initialization. This should only be called by the IDL generator when creating
     * MONGO_SERVER_PARAMETER_REGISTER blocks for parameters that do not specify a `cpp_vartype`
     * (the storage variable is not defined by the IDL generator).
     */
    Status setDefault(const element_type& newDefaultValue) {
        Status status = Status::OK();
        std::call_once(_setDefaultOnce, [&] {
            // Update the default value.
            _storage.setDefault(newDefaultValue);

            // Update the actual storage, performing validation and any post-update functions as
            // necessary.
            status = reset();
        });
        return status;
    }

    /**
     * Encode the setting into BSON object.
     *
     * Typically invoked by {getParameter:...} or {getClusterParameter:...} to produce a dictionary
     * of SCP settings.
     */
    void append(OperationContext* opCtx,
                BSONObjBuilder& b,
                const std::string& name) override final {
        if (isRedact()) {
            b.append(name, "###");
        } else if constexpr (paramType == SPT::kClusterWide) {
            b.append("_id"_sd, name);
            b.appendElementsUnique(getValue().toBSON());
        } else {
            b.append(name, getValue());
        }
    }

    StatusWith<element_type> parseElement(const BSONElement& newValueElement) const {
        element_type newValue;
        if constexpr (paramType == SPT::kClusterWide) {
            try {
                BSONObj cspObj = newValueElement.Obj();
                newValue = element_type::parse({"ClusterServerParameter"}, cspObj);
            } catch (const DBException& ex) {
                return ex.toStatus().withContext(
                    str::stream() << "Failed parsing ClusterServerParameter '" << name() << "'");
            }
        } else {
            if (auto status = newValueElement.tryCoerce(&newValue); !status.isOK()) {
                return {status.code(),
                        str::stream() << "Failed validating " << name() << ": " << status.reason()};
            }
        }

        return newValue;
    }

    Status validate(const BSONElement& newValueElement) const override final {
        StatusWith<element_type> swNewValue = parseElement(newValueElement);
        if (!swNewValue.isOK()) {
            return swNewValue.getStatus();
        }

        return validateValue(swNewValue.getValue());
    }

    /**
     * Update the underlying value using a BSONElement
     *
     * Allows setting non-basic values (e.g. vector<string>)
     * via the {setParameter: ...} call or {setClusterParameter: ...} call.
     */
    Status set(const BSONElement& newValueElement) override final {
        StatusWith<element_type> swNewValue = parseElement(newValueElement);
        if (!swNewValue.isOK()) {
            return swNewValue.getStatus();
        }

        return setValue(swNewValue.getValue());
    }

    /**
     * Resets the current storage value in storage_wrapper with the default value.
     */
    Status reset() override final {
        _storage.reset();
        if (_onUpdate) {
            return _onUpdate(_storage.load());
        }

        return Status::OK();
    }

    /**
     * Update the underlying value from a string.
     *
     * Typically invoked from commandline --setParameter usage. Prohibited for cluster server
     * parameters.
     */
    Status setFromString(const std::string& str) override final {
        if constexpr (paramType == SPT::kClusterWide) {
            return {ErrorCodes::BadValue,
                    "Unable to set a cluster-wide server parameter from the command line or config "
                    "file. See command 'setClusterParameter'"};
        } else {
            auto swNewValue = idl_server_parameter_detail::coerceFromString<element_type>(str);
            if (!swNewValue.isOK()) {
                return swNewValue.getStatus();
            }

            return setValue(swNewValue.getValue());
        }
    }

    /**
     * Retrieves the cluster parameter time from the chained ClusterServerParameter struct in
     * storage. All other server parameters simply return the uninitialized LogicalTime.
     */
    const LogicalTime getClusterParameterTime() const override final {
        if constexpr (hasClusterServerParameter<T>) {
            return getValue().getClusterParameterTime();
        } else {
            return LogicalTime();
        }
    }

    /**
     * Called *after* updating the underlying storage to its new value.
     */
    using onUpdate_t = Status(const element_type&);
    void setOnUpdate(std::function<onUpdate_t> onUpdate) {
        _onUpdate = std::move(onUpdate);
    }

    // Validators.

    /**
     * Add a callback validator to be invoked when this setting is updated.
     *
     * Callback should return Status::OK() or ErrorCodes::BadValue.
     */
    using validator_t = Status(const element_type&);
    void addValidator(std::function<validator_t> validator) {
        _validators.push_back(std::move(validator));
    }

    /**
     * Sets a validation limit against a predicate function.
     */
    template <class predicate>
    void addBound(const element_type& bound) {
        addValidator([bound, spname = name()](const element_type& value) {
            if (!predicate::evaluate(value, bound)) {
                return Status(ErrorCodes::BadValue,
                              str::stream()
                                  << "Invalid value for parameter " << spname << ": " << value
                                  << " is not " << predicate::description << " " << bound);
            }
            return Status::OK();
        });
    }

private:
    SW _storage;

    std::vector<std::function<validator_t>> _validators;
    std::function<onUpdate_t> _onUpdate;
    std::once_flag _setDefaultOnce;
};

// MSVC has trouble resolving T=decltype(param) through the above class template.
// Avoid that by using this proxy factory to infer storage type.
template <ServerParameterType paramType, typename T>
IDLServerParameterWithStorage<paramType, T>* makeIDLServerParameterWithStorage(StringData name,
                                                                               T& storage) {
    auto p = std::make_unique<IDLServerParameterWithStorage<paramType, T>>(name, storage);
    registerServerParameter(&*p);
    return p.release();
}

}  // namespace mongo
