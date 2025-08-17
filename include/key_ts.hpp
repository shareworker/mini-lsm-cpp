#pragma once

#include "byte_buffer.hpp"
#include <cstdint>
#include <utility>

namespace util {

/**
 * @brief Composite key structure for MVCC that combines a user key with a timestamp.
 * 
 * The KeyTs struct is used for versioned reads in MVCC. Keys are ordered first by
 * the user key in ascending order, then by timestamp in descending order.
 * This allows efficient retrieval of the latest version of a key as of a given timestamp.
 */
class KeyTs {
public:
    /**
     * @brief Constructs a KeyTs with a key and timestamp.
     * 
     * @param key User key
     * @param ts Timestamp (version)
     */
    KeyTs(ByteBuffer key, uint64_t ts) : key_(std::move(key)), ts_(ts) {}

    /**
     * @brief Default constructor creates an empty key with timestamp 0.
     */
    KeyTs() : key_(), ts_(0) {}

    /**
     * @brief Get the user key component.
     * 
     * @return const ByteBuffer& Reference to the user key
     */
    const ByteBuffer& Key() const noexcept { return key_; }
    
    /**
     * @brief Get the timestamp component.
     * 
     * @return uint64_t The timestamp
     */
    uint64_t Timestamp() const noexcept { return ts_; }

    /**
     * @brief Equality comparison operator
     */
    bool operator==(const KeyTs& other) const noexcept {
        return key_ == other.key_ && ts_ == other.ts_;
    }
    
    /**
     * @brief Inequality comparison operator
     */
    bool operator!=(const KeyTs& other) const noexcept {
        return !(*this == other);
    }

    /**
     * @brief Less than comparison operator for ordering in containers
     * 
     * Orders first by key (ascending), then by timestamp (descending).
     * This ensures that when looking up a specific key, we find the newest
     * version first (highest timestamp) that is at or below our read timestamp.
     */
    bool operator<(const KeyTs& other) const noexcept {
        // Primary order: user key (ascending)
        if (key_ < other.key_) return true;
        if (other.key_ < key_) return false;
        
        // Secondary order: timestamp (descending - newer first)
        return ts_ > other.ts_;
    }

    /**
     * @brief Greater than comparison operator
     */
    bool operator>(const KeyTs& other) const noexcept {
        return other < *this;
    }
    
    /**
     * @brief Less than or equal comparison operator
     */
    bool operator<=(const KeyTs& other) const noexcept {
        return !(other < *this);
    }
    
    /**
     * @brief Greater than or equal comparison operator
     */
    bool operator>=(const KeyTs& other) const noexcept {
        return !(*this < other);
    }

private:
    ByteBuffer key_; // User key
    uint64_t ts_;    // Timestamp
};

} // namespace util
