#pragma once

#include <cstddef>
#include <memory>
#include <list>
#include <mutex>
#include <unordered_map>
#include <utility>
#include "block.hpp"


// Thread-safe size-bounded LRU cache for blocks.
// Similar to Rust's `moka::sync::Cache`, eviction is based on the number of
// cached blocks (not their byte size) to keep the implementation simple and
// lock-free friendly.  Operations are O(1): we combine an unordered_map for
// quick lookup and a doubly-linked list (`std::list`) that records usage order.
// The most-recently-used (MRU) entry is kept at the front.  All public methods
// acquire a mutex to ensure thread-safety because blocks are accessed from
// multiple threads (e.g. read path, compaction thread).
class BlockCache {
public:
    using BlockPtr = std::shared_ptr<const Block>;

    explicit BlockCache(size_t capacity = kDefaultCapacity)
        : capacity_(capacity) {}
    BlockCache(const BlockCache&) = delete;
    BlockCache& operator=(const BlockCache&) = delete;
    BlockCache(BlockCache&&) = delete;
    BlockCache& operator=(BlockCache&&) = delete;

    void Insert(size_t key, BlockPtr block) {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = map_.find(key);
        if (it != map_.end()) {
            // Key already present: update value and move to front.
            it->second->second = std::move(block);
            usage_.splice(usage_.begin(), usage_, it->second);
            return;
        }
        usage_.emplace_front(key, std::move(block));
        map_[key] = usage_.begin();
        if (map_.size() > capacity_) {
            // Evict least recently used (tail).
            auto last = usage_.end();
            --last;
            map_.erase(last->first);
            usage_.pop_back();
        }
    }
    BlockPtr Lookup(size_t key) {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = map_.find(key);
        if (it == map_.end()) {
            return nullptr;
        }
        // Move to front to mark as recently used.
        usage_.splice(usage_.begin(), usage_, it->second);
        return it->second->second;
    }

private:
    static constexpr size_t kDefaultCapacity = 4096; // max entries

    // Doubly-linked list of (key, block) with MRU at front, LRU at back.
    std::list<std::pair<size_t, BlockPtr>> usage_;
    // Map key -> iterator into list for O(1) lookup and splice.
    std::unordered_map<size_t, std::list<std::pair<size_t, BlockPtr>>::iterator> map_;
    size_t capacity_;
    mutable std::mutex mu_;
};

