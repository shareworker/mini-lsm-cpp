#pragma once

#include <memory>
#include <queue>
#include <vector>

#include "storage_iterator.hpp"

/**
 * @brief MergeIterator merges N sorted forward iterators producing a single
 *        ascending stream without duplicates (preferring lower iterator index
 *        when keys collide).
 *
 * The behaviour mirrors Rust `MergeIterator` in mini-lsm.
 */
class MergeIterator : public StorageIterator {
private:
    struct HeapItem {
        size_t idx;
        StorageIterator* iter;
    };

    struct Compare {
        bool operator()(const HeapItem& a, const HeapItem& b) const {
            if (a.iter->Key() == b.iter->Key()) {
                return a.idx > b.idx;
            }
            return a.iter->Key() > b.iter->Key();
        }
    };

    // Current state snapshot to avoid stale iterator references
    struct CurrentState {
        bool valid = false;
        ByteBuffer key;
        ByteBuffer value;
        size_t iter_idx = 0;
    };

    std::vector<std::unique_ptr<StorageIterator>> iters_;
    std::priority_queue<HeapItem, std::vector<HeapItem>, Compare> heap_;
    CurrentState current_;

    void FindNext() {
        while (!heap_.empty()) {
            HeapItem item = heap_.top();
            heap_.pop();

            if (!item.iter->IsValid()) {
                continue;
            }

            if (current_.valid && current_.key == item.iter->Key()) {
                // Skip duplicate key, advance the iterator
                item.iter->Next();
                if (item.iter->IsValid()) {
                    heap_.push(item);
                }
                continue;
            }
            
            // Store snapshot of current state
            current_.valid = true;
            current_.key = item.iter->Key();
            current_.value = item.iter->Value();
            current_.iter_idx = item.idx;
            return;
        }
        current_.valid = false;
    }

public:
    explicit MergeIterator(std::vector<std::unique_ptr<StorageIterator>> iters)
        : iters_(std::move(iters)) {
        for (size_t i = 0; i < iters_.size(); ++i) {
            if (iters_[i]->IsValid()) {
                heap_.push({i, iters_[i].get()});
            }
        }
        FindNext();
    }

    ByteBuffer Key() const noexcept override {
        return current_.key;
    }

    const ByteBuffer& Value() const noexcept override {
        return current_.value;
    }

    bool IsValid() const noexcept override {
        return current_.valid;
    }

    void Next() noexcept override {
        if (!IsValid()) {
            return;
        }
        // Advance the iterator that was current and add it back to heap if still valid
        StorageIterator* current_iter = iters_[current_.iter_idx].get();
        current_iter->Next();
        if (current_iter->IsValid()) {
            heap_.push({current_.iter_idx, current_iter});
        }
        FindNext();
    }
};

