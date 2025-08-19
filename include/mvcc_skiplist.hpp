#pragma once

#include "skiplist.hpp"
#include "key_ts.hpp"
#include "byte_buffer.hpp"
#include <memory>
#include <utility>

/**
 * @brief Specialized skiplist for MVCC that uses KeyTs (composite key + timestamp) for versioning.
 * 
 * MvccSkipList is a wrapper around the standard SkipList that uses KeyTs as its key type.
 * It provides timestamp-aware versions of the standard skiplist operations (Put, Get, Remove).
 */
class MvccSkipList {
public:
    /**
     * @brief Constructs an empty MvccSkipList.
     */
    MvccSkipList() 
        : skiplist_(std::make_shared<SkipList<KeyTs, ByteBuffer>>()) {}

    /**
     * @brief Inserts a key-value pair with the specified timestamp.
     * 
     * @param key The user key
     * @param value The value to insert
     * @param ts The timestamp (version) for this update
     * @return true if the insertion succeeded
     */
    bool Put(const ByteBuffer& key, const ByteBuffer& value, uint64_t ts) {
        KeyTs composite_key(key.Clone(), ts);
        return skiplist_->Insert(std::move(composite_key), value.Clone());
    }

    /**
     * @brief Retrieves the value associated with the key as of the given timestamp.
     * 
     * @param key The user key to look up
     * @param read_ts The timestamp to read as of (only versions <= read_ts are visible)
     * @return std::optional<std::pair<ByteBuffer, uint64_t>> The value and its timestamp if found, nullopt otherwise
     */
    std::optional<std::pair<ByteBuffer, uint64_t>> GetWithTs(const ByteBuffer& key, uint64_t read_ts) {
        // We need a manual implementation since the skiplist doesn't offer STL-style iterators
        std::optional<std::pair<ByteBuffer, uint64_t>> result;
        
        // Use ForEach to scan the skiplist and find the newest version <= read_ts
        skiplist_->ForEach([&](const KeyTs& curr_key, const ByteBuffer& value) {
            // Only process entries matching our key
            if (curr_key.Key() == key) {
                uint64_t ts = curr_key.Timestamp();
                
                // If this version is visible (ts <= read_ts) and is newer than any we've seen
                if (ts <= read_ts && (!result.has_value() || ts > result->second)) {
                    result = std::make_pair(value.Clone(), ts);
                }
            }
        });
        
        return result;
    }
    
    /**
     * @brief Inserts a tombstone value for the key with the specified timestamp.
     * 
     * @param key The user key to remove
     * @param ts The timestamp (version) for this removal
     * @return true if the insertion succeeded
     */
    bool Remove(const ByteBuffer& key, uint64_t ts) {
        KeyTs composite_key(key.Clone(), ts);
        // Insert an empty ByteBuffer as a tombstone
        return skiplist_->Insert(std::move(composite_key), ByteBuffer());
    }
    
    /**
     * @brief Checks if the skiplist is empty.
     * 
     * @return true if empty, false otherwise
     */
    bool IsEmpty() const {
        return skiplist_->IsEmpty();
    }
    
    /**
     * @brief Clears all entries from the skiplist.
     */
    void Clear() {
        skiplist_->Clear();
    }
    
    /**
     * @brief Returns the underlying skiplist.
     * 
     * @return std::shared_ptr<SkipList<KeyTs, ByteBuffer>> The underlying skiplist
     */
    std::shared_ptr<SkipList<KeyTs, ByteBuffer>> GetSkipList() const {
        return skiplist_;
    }
    
    /**
     * @brief Apply a function to each key-value pair in the skiplist.
     * 
     * @param func Function to apply, taking (key, value) as parameters
     */
    template <typename Func>
    void ForEach(Func&& func) const {
        skiplist_->ForEach([&func](const KeyTs& composite_key, const ByteBuffer& value) {
            func(composite_key.Key(), value, composite_key.Timestamp());
        });
    }

private:
    std::shared_ptr<SkipList<KeyTs, ByteBuffer>> skiplist_;
};

