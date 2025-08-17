#pragma once

// Key handling for Mini-LSM
// Copyright (c) 2025
// Licensed under Apache 2.0.
//
// This header defines the Key class and related types for the LSM storage engine.

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "byte_buffer.hpp"

namespace util {

// Constant to enable/disable timestamp functionality
constexpr bool TS_ENABLED = false;

/**
 * @brief Key wrapper class for LSM storage
 * 
 * This class wraps a byte sequence and provides key-related functionality.
 * It mirrors the Rust Key<T> implementation.
 */
template <typename T>
class Key {
public:
    /**
     * @brief Construct a new Key object
     * 
     * @param data The underlying data
     */
    explicit Key(T data) : data_(std::move(data)) {}

    /**
     * @brief Get the inner data
     * 
     * @return T The inner data
     */
    T IntoInner() && {
        return std::move(data_);
    }

    /**
     * @brief Get the length of the key
     * 
     * @return size_t Key length
     */
    size_t Size() const {
        return AsRef().size();
    }

    /**
     * @brief Check if the key is empty
     * 
     * @return true if empty
     * @return false if not empty
     */
    bool Empty() const {
        return AsRef().empty();
    }

    /**
     * @brief Get the timestamp (for testing)
     * 
     * @return uint64_t Always returns 0 when TS_ENABLED is false
     */
    uint64_t ForTestingTs() const {
        return 0;
    }

    /**
     * @brief Get a reference to the underlying data
     * 
     * @return const ByteBuffer& Reference to the data
     */
    const T& Data() const {
        return data_;
    }

    /**
     * @brief Get a reference to the underlying data as a byte sequence
     * 
     * @return ByteBuffer The data as a byte buffer
     */
    ByteBuffer AsRef() const {
        if constexpr (std::is_same_v<T, ByteBuffer>) {
            return data_;
        } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            return ByteBuffer(data_.data(), data_.size());
        } else if constexpr (std::is_same_v<T, std::string>) {
            return ByteBuffer(reinterpret_cast<const uint8_t*>(data_.data()), data_.size());
        } else {
            static_assert(std::is_same_v<T, ByteBuffer> || 
                          std::is_same_v<T, std::vector<uint8_t>> || 
                          std::is_same_v<T, std::string>,
                          "Unsupported type for Key");
            return ByteBuffer();
        }
    }

    /**
     * @brief Compare two keys for equality
     * 
     * @param other Other key
     * @return true if equal
     * @return false if not equal
     */
    bool operator==(const Key& other) const {
        return AsRef() == other.AsRef();
    }

    /**
     * @brief Compare two keys for inequality
     * 
     * @param other Other key
     * @return true if not equal
     * @return false if equal
     */
    bool operator!=(const Key& other) const {
        return !(*this == other);
    }

    /**
     * @brief Compare two keys for ordering
     * 
     * @param other Other key
     * @return true if this key is less than other
     * @return false otherwise
     */
    bool operator<(const Key& other) const {
        return AsRef() < other.AsRef();
    }

    /**
     * @brief Compare two keys for ordering
     * 
     * @param other Other key
     * @return true if this key is greater than other
     * @return false otherwise
     */
    bool operator>(const Key& other) const {
        return AsRef() > other.AsRef();
    }

    /**
     * @brief Compare two keys for ordering
     * 
     * @param other Other key
     * @return true if this key is less than or equal to other
     * @return false otherwise
     */
    bool operator<=(const Key& other) const {
        return AsRef() <= other.AsRef();
    }

    /**
     * @brief Compare two keys for ordering
     * 
     * @param other Other key
     * @return true if this key is greater than or equal to other
     * @return false otherwise
     */
    bool operator>=(const Key& other) const {
        return AsRef() >= other.AsRef();
    }

private:
    T data_;
};

// Type aliases similar to Rust
using KeyVec = Key<std::vector<uint8_t>>;
using KeyBuffer = Key<ByteBuffer>;
using KeyString = Key<std::string>;

/**
 * @brief Specialization for KeyVec
 */
template <>
class Key<std::vector<uint8_t>> {
public:
    /**
     * @brief Construct a new empty Key
     */
    Key() : data_() {}

    /**
     * @brief Construct a new Key object
     * 
     * @param data The underlying data
     */
    explicit Key(std::vector<uint8_t> data) : data_(std::move(data)) {}

    /**
     * @brief Create a new empty key
     * 
     * @return Key<std::vector<uint8_t>> New key
     */
    static Key<std::vector<uint8_t>> New() {
        return Key<std::vector<uint8_t>>();
    }

    /**
     * @brief Create a key from a vector
     * 
     * @param key Vector of bytes
     * @return Key<std::vector<uint8_t>> New key
     */
    static Key<std::vector<uint8_t>> FromVec(std::vector<uint8_t> key) {
        return Key<std::vector<uint8_t>>(std::move(key));
    }

    /**
     * @brief Get the inner data
     * 
     * @return std::vector<uint8_t> The inner data
     */
    std::vector<uint8_t> IntoInner() && {
        return std::move(data_);
    }

    /**
     * @brief Clear the key
     */
    void Clear() {
        data_.clear();
    }

    /**
     * @brief Append data to the key
     * 
     * @param data Data to append
     */
    void Append(const uint8_t* data, size_t size) {
        data_.insert(data_.end(), data, data + size);
    }

    /**
     * @brief Set the key from a ByteBuffer
     * 
     * @param key_buffer Source key buffer
     */
    void SetFromBuffer(const ByteBuffer& key_buffer) {
        data_.clear();
        data_.insert(data_.end(), key_buffer.Data(), key_buffer.Data() + key_buffer.Size());
    }

    /**
     * @brief Convert to a KeyBuffer
     * 
     * @return KeyBuffer Converted key
     */
    KeyBuffer IntoKeyBuffer() && {
        return KeyBuffer(ByteBuffer(data_.data(), data_.size()));
    }

    /**
     * @brief Get a reference to the underlying data
     * 
     * @return ByteBuffer The data as a byte buffer
     */
    ByteBuffer AsRef() const {
        return ByteBuffer(data_.data(), data_.size());
    }

    /**
     * @brief Get the length of the key
     * 
     * @return size_t Key length
     */
    size_t Size() const {
        return data_.size();
    }

    /**
     * @brief Check if the key is empty
     * 
     * @return true if empty
     * @return false if not empty
     */
    bool Empty() const {
        return data_.empty();
    }

    /**
     * @brief Get the timestamp (for testing)
     * 
     * @return uint64_t Always returns 0 when TS_ENABLED is false
     */
    uint64_t ForTestingTs() const {
        return 0;
    }

    /**
     * @brief Compare two keys for equality
     * 
     * @param other Other key
     * @return true if equal
     * @return false if not equal
     */
    bool operator==(const Key& other) const {
        return data_ == other.data_;
    }

    /**
     * @brief Compare two keys for inequality
     * 
     * @param other Other key
     * @return true if not equal
     * @return false if equal
     */
    bool operator!=(const Key& other) const {
        return !(*this == other);
    }

    /**
     * @brief Compare two keys for ordering
     * 
     * @param other Other key
     * @return true if this key is less than other
     * @return false otherwise
     */
    bool operator<(const Key& other) const {
        return data_ < other.data_;
    }

    /**
     * @brief Compare two keys for ordering
     * 
     * @param other Other key
     * @return true if this key is greater than other
     * @return false otherwise
     */
    bool operator>(const Key& other) const {
        return data_ > other.data_;
    }

    /**
     * @brief Compare two keys for ordering
     * 
     * @param other Other key
     * @return true if this key is less than or equal to other
     * @return false otherwise
     */
    bool operator<=(const Key& other) const {
        return data_ <= other.data_;
    }

    /**
     * @brief Compare two keys for ordering
     * 
     * @param other Other key
     * @return true if this key is greater than or equal to other
     * @return false otherwise
     */
    bool operator>=(const Key& other) const {
        return data_ >= other.data_;
    }

private:
    std::vector<uint8_t> data_;
};

/**
 * @brief Specialization for KeyBuffer
 */
// ByteBuffer specialization is already defined above as the primary template
#if 0
template <>
class Key<ByteBuffer> {
public:
    /**
     * @brief Construct a new Key object
     * 
     * @param data The underlying data
     */
    explicit Key(ByteBuffer data) : data_(std::move(data)) {}

    /**
     * @brief Create a key from a ByteBuffer
     * 
     * @param buffer ByteBuffer
     * @return Key<ByteBuffer> New key
     */
    static Key<ByteBuffer> FromBuffer(ByteBuffer buffer) {
        return Key<ByteBuffer>(std::move(buffer));
    }

    /**
     * @brief Get the inner data
     * 
     * @return ByteBuffer The inner data
     */
    ByteBuffer IntoInner() && {
        return std::move(data_);
    }

    /**
     * @brief Get a reference to the underlying data
     * 
     * @return ByteBuffer The data as a byte buffer
     */
    ByteBuffer AsRef() const {
        return data_;
    }

    /**
     * @brief Get the length of the key
     * 
     * @return size_t Key length
     */
    size_t Size() const {
        return data_.Size();
    }

    /**
     * @brief Check if the key is empty
     * 
     * @return true if empty
     * @return false if not empty
     */
    bool Empty() const {
        return data_.Empty();
    }

    /**
     * @brief Get the timestamp (for testing)
     * 
     * @return uint64_t Always returns 0 when TS_ENABLED is false
     */
    uint64_t ForTestingTs() const {
        return 0;
    }

    /**
     * @brief Compare two keys for equality
     * 
     * @param other Other key
     * @return true if equal
     * @return false if not equal
     */
    bool operator==(const Key& other) const {
        return data_ == other.data_;
    }

    /**
     * @brief Compare two keys for inequality
     * 
     * @param other Other key
     * @return true if not equal
     * @return false if equal
     */
    bool operator!=(const Key& other) const {
        return !(*this == other);
    }

    /**
     * @brief Compare two keys for ordering
     * 
     * @param other Other key
     * @return true if this key is less than other
     * @return false otherwise
     */
    bool operator<(const Key& other) const {
        return data_ < other.data_;
    }

    /**
     * @brief Compare two keys for ordering
     * 
     * @param other Other key
     * @return true if this key is greater than other
     * @return false otherwise
     */
    bool operator>(const Key& other) const {
        return data_ > other.data_;
    }

    /**
     * @brief Compare two keys for ordering
     * 
     * @param other Other key
     * @return true if this key is less than or equal to other
     * @return false otherwise
     */
    bool operator<=(const Key& other) const {
        return data_ <= other.data_;
    }

    /**
     * @brief Compare two keys for ordering
     * 
     * @param other Other key
     * @return true if this key is greater than or equal to other
     * @return false otherwise
     */
    bool operator>=(const Key& other) const {
        return data_ >= other.data_;
    }

private:
    ByteBuffer data_;
};

#endif // End of #if 0 conditional directive

} // namespace util
