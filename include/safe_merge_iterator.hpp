#pragma once

#include <memory>
#include <queue>
#include <vector>
#include <algorithm>

#include "fused_iterator.hpp"
#include "storage_iterator.hpp"


/**
 * @brief Enhanced merge iterator with FusedIterator safety guarantees
 * 
 * SafeMergeIterator merges N sorted forward iterators producing a single
 * ascending stream without duplicates, while providing enhanced safety:
 * - Once exhausted, remains permanently invalid
 * - Safe to call operations on exhausted iterator
 * - Consistent state guarantees across all operations
 * - Protection against stale iterator references
 */
class SafeMergeIterator : public FusedIterator {
private:
    struct HeapItem {
        size_t idx;
        StorageIterator* iter;
        
        // Store snapshot of key for comparison to avoid stale references
        ByteBuffer key_snapshot;
        
        HeapItem(size_t i, StorageIterator* it) 
            : idx(i), iter(it), key_snapshot(it->Key()) {}
    };

    struct Compare {
        bool operator()(const HeapItem& a, const HeapItem& b) const {
            // Compare using snapshots to avoid stale iterator access
            if (a.key_snapshot == b.key_snapshot) {
                return a.idx > b.idx; // Lower index has priority
            }
            return a.key_snapshot > b.key_snapshot;
        }
    };

    // Immutable state snapshot to guarantee consistency
    struct CurrentSnapshot {
        bool valid = false;
        ByteBuffer key;
        ByteBuffer value;
        size_t source_iter_idx = 0;
    };

    std::vector<std::unique_ptr<StorageIterator>> iters_;
    std::priority_queue<HeapItem, std::vector<HeapItem>, Compare> heap_;
    CurrentSnapshot current_;
    
    // Safety state tracking
    mutable size_t heap_operations_ = 0;
    mutable bool state_corrupted_ = false;

    /**
     * @brief Find the next valid entry with enhanced safety checks
     * 
     * This method includes additional validation to ensure heap consistency
     * and detect potential corruption scenarios.
     */
    void FindNextSafe() noexcept {
        ++heap_operations_;
        
        try {
            while (!heap_.empty()) {
                HeapItem item = heap_.top();
                heap_.pop();

                // Validate iterator before use
                if (!item.iter || item.idx >= iters_.size()) {
                    state_corrupted_ = true;
                    current_.valid = false;
                    return;
                }

                if (!item.iter->IsValid()) {
                    continue;
                }
                
                // Refresh key snapshot in case iterator moved
                ByteBuffer current_key = item.iter->Key();
                
                // Skip duplicate keys (prefer lower index)
                if (current_.valid && current_.key == current_key) {
                    // Advance iterator and re-add to heap if still valid
                    item.iter->Next();
                    if (item.iter->IsValid()) {
                        heap_.emplace(item.idx, item.iter);
                    }
                    continue;
                }
                
                // Capture immutable snapshot of current state
                current_.valid = true;
                current_.key = current_key;
                current_.value = item.iter->Value();
                current_.source_iter_idx = item.idx;
                
                return;
            }
            
            // No more valid entries
            current_.valid = false;
            
        } catch (...) {
            // Exception safety - mark as corrupted and invalid
            state_corrupted_ = true;
            current_.valid = false;
        }
    }

    /**
     * @brief Validate heap consistency for debugging
     * 
     * @return true if heap appears consistent
     */
    bool ValidateHeapConsistency() const noexcept {
        if (state_corrupted_) {
            return false;
        }
        
        // Basic validation: ensure all heap items reference valid iterators
        std::priority_queue<HeapItem, std::vector<HeapItem>, Compare> temp_heap = heap_;
        
        while (!temp_heap.empty()) {
            const HeapItem& item = temp_heap.top();
            temp_heap.pop();
            
            if (item.idx >= iters_.size() || !iters_[item.idx]) {
                return false;
            }
        }
        
        return true;
    }

public:
    /**
     * @brief Constructor with enhanced validation
     * 
     * @param iters Vector of iterators to merge
     */
    explicit SafeMergeIterator(std::vector<std::unique_ptr<StorageIterator>> iters)
        : iters_(std::move(iters)) {
        
        // Handle empty iterator list
        if (iters_.empty()) {
            current_.valid = false;
            return;
        }
        
        // Validate input iterators
        for (size_t i = 0; i < iters_.size(); ++i) {
            if (!iters_[i]) {
                state_corrupted_ = true;
                current_.valid = false;
                return;
            }
            
            if (iters_[i]->IsValid()) {
                heap_.emplace(i, iters_[i].get());
            }
        }
        
        FindNextSafe();
    }

    /**
     * @brief Factory method with validation
     * 
     * @param iters Vector of iterators to merge
     * @return Safe merge iterator instance
     */
    static std::unique_ptr<SafeMergeIterator> Create(
        std::vector<std::unique_ptr<StorageIterator>> iters) {
        return std::make_unique<SafeMergeIterator>(std::move(iters));
    }

    /**
     * @brief Get debugging information about iterator state
     * 
     * @return Number of heap operations performed
     */
    size_t GetHeapOperationCount() const noexcept {
        return heap_operations_;
    }

    /**
     * @brief Check if iterator state is corrupted
     * 
     * @return true if corruption detected
     */
    bool IsStateCorrupted() const noexcept {
        return state_corrupted_;
    }

    /**
     * @brief Get statistics about the merge operation
     * 
     * @return Current heap size and total number of source iterators
     */
    std::pair<size_t, size_t> GetMergeStats() const noexcept {
        return {heap_.size(), iters_.size()};
    }

protected:
    // FusedIterator implementation
    bool IsValidImpl() const noexcept override {
        if (state_corrupted_) {
            return false;
        }
        return current_.valid && ValidateHeapConsistency();
    }

    ByteBuffer KeyImpl() const noexcept override {
        return current_.key;
    }

    const ByteBuffer& ValueImpl() const noexcept override {
        return current_.value;
    }

    void NextImpl() noexcept override {
        if (state_corrupted_ || !current_.valid) {
            return;
        }

        try {
            // Advance the current source iterator
            if (current_.source_iter_idx < iters_.size() && 
                iters_[current_.source_iter_idx]) {
                
                StorageIterator* current_iter = iters_[current_.source_iter_idx].get();
                current_iter->Next();
                
                // Add back to heap if still valid
                if (current_iter->IsValid()) {
                    heap_.emplace(current_.source_iter_idx, current_iter);
                }
            }
            
            FindNextSafe();
            
        } catch (...) {
            // Exception safety
            state_corrupted_ = true;
            current_.valid = false;
        }
    }
};

/**
 * @brief Factory function for creating safe merge iterators
 * 
 * @param iters Vector of iterators to merge safely
 * @return FusedIterator with merge functionality
 */
inline std::unique_ptr<FusedIterator> CreateSafeMergeIterator(
    std::vector<std::unique_ptr<StorageIterator>> iters) {
    return SafeMergeIterator::Create(std::move(iters));
}

