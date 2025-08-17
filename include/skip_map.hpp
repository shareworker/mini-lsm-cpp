#pragma once

// SkipMap implementation for MVCC
// Copyright (c) 2025
// Licensed under Apache 2.0.
//
// This header defines a SkipMap class for MVCC transaction local storage.
// It's a simplified wrapper around std::map with additional range query support.

#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <utility>

#include "bound.hpp"
#include "byte_buffer.hpp"

namespace util {

/**
 * @brief SkipMap implementation for MVCC local storage
 * 
 * Provides a map-like interface with range query support.
 */
class SkipMap {
private:
    struct private_passkey {};

public:
    /**
     * @brief Iterator over a range of key-value pairs
     */
    class RangeIterator {
    public:
        /**
         * @brief Check if the iterator is valid
         * 
         * @return true if the iterator points to a valid entry
         * @return false if the iterator is exhausted
         */
        bool Valid() const;

        /**
         * @brief Advance the iterator to the next entry
         */
        void Next();

        /**
         * @brief Get the current key
         * 
         * @return const ByteBuffer& Current key
         */
        const ByteBuffer& Key() const;

        /**
         * @brief Get the current value
         * 
         * @return const ByteBuffer& Current value
         */
        const ByteBuffer& Value() const;

        // Public constructor, but requires a private_passkey that only SkipMap can create.
        RangeIterator(
            private_passkey,
            std::map<ByteBuffer, ByteBuffer>::const_iterator begin,
            std::map<ByteBuffer, ByteBuffer>::const_iterator end);

    private:
        // Iterator state
        std::map<ByteBuffer, ByteBuffer>::const_iterator current_;
        std::map<ByteBuffer, ByteBuffer>::const_iterator end_;
    };

    /**
     * @brief Construct a new SkipMap
     */
    SkipMap() = default;

    /**
     * @brief Insert or update a key-value pair
     * 
     * @param key Key
     * @param value Value
     */
    void Insert(const ByteBuffer& key, const ByteBuffer& value);

    /**
     * @brief Get a value for a key
     * 
     * @param key Key to look up
     * @return std::optional<ByteBuffer> Value if found
     */
    std::optional<ByteBuffer> Get(const ByteBuffer& key) const;

    /**
     * @brief Create a range iterator
     * 
     * @param lower Lower bound
     * @param upper Upper bound
     * @return std::unique_ptr<RangeIterator> Iterator over the range
     */
    std::unique_ptr<RangeIterator> Range(
        const Bound& lower,
        const Bound& upper) const;

private:
    // Underlying storage
    std::map<ByteBuffer, ByteBuffer> map_;
};

} // namespace util
