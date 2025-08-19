#pragma once

#include <memory>
#include <utility>

#include "fused_iterator.hpp"
#include "storage_iterator.hpp"


/**
 * @brief Enhanced two-merge iterator with FusedIterator safety guarantees
 * 
 * SafeTwoMergeIterator merges two sorted forward iterators into a single stream
 * with enhanced safety:
 * - Permanent invalidation once exhausted
 * - Safe operations on invalid iterators
 * - Protection against stale iterator references
 * - Consistent state across all operations
 */
class SafeTwoMergeIterator : public FusedIterator {
private:
    // Immutable state snapshot for safety
    struct IteratorSnapshot {
        bool valid = false;
        ByteBuffer key;
        ByteBuffer value;
        bool from_a = false; // true if current value is from iterator A
    };

    std::unique_ptr<StorageIterator> a_;
    std::unique_ptr<StorageIterator> b_;
    IteratorSnapshot current_;
    
    // Safety tracking
    mutable size_t comparison_count_ = 0;
    mutable bool state_corrupted_ = false;

    /**
     * @brief Safely skip duplicate keys in iterator B
     * 
     * When both iterators have the same key, iterator A takes priority
     * and we advance iterator B past the duplicate.
     */
    void SkipDuplicatesInB() noexcept {
        if (!a_ || !b_) {
            return;
        }

        try {
            while (a_->IsValid() && b_->IsValid() && a_->Key() == b_->Key()) {
                b_->Next();
            }
        } catch (...) {
            state_corrupted_ = true;
        }
    }

    /**
     * @brief Determine which iterator should be chosen with safety checks
     * 
     * @return true if iterator A should be chosen, false for iterator B
     */
    bool ChooseASafely() noexcept {
        ++comparison_count_;
        
        if (!a_ || !b_) {
            state_corrupted_ = true;
            return false;
        }

        try {
            if (!a_->IsValid()) {
                return false; // Choose B if A is invalid
            }
            if (!b_->IsValid()) {
                return true; // Choose A if B is invalid
            }
            
            // Both valid - compare keys (A has priority on ties)
            return a_->Key() <= b_->Key();
            
        } catch (...) {
            state_corrupted_ = true;
            return false;
        }
    }

    /**
     * @brief Update current snapshot with enhanced safety
     */
    void UpdateCurrentSnapshot() noexcept {
        if (state_corrupted_) {
            current_.valid = false;
            return;
        }

        try {
            bool choose_a = ChooseASafely();
            
            if (choose_a && a_ && a_->IsValid()) {
                current_.valid = true;
                current_.key = a_->Key();
                current_.value = a_->Value();
                current_.from_a = true;
            } else if (!choose_a && b_ && b_->IsValid()) {
                current_.valid = true;
                current_.key = b_->Key();
                current_.value = b_->Value();
                current_.from_a = false;
            } else {
                current_.valid = false;
            }
            
        } catch (...) {
            state_corrupted_ = true;
            current_.valid = false;
        }
    }

public:
    /**
     * @brief Constructor with enhanced validation
     * 
     * @param a First iterator (has priority on duplicate keys)
     * @param b Second iterator
     */
    SafeTwoMergeIterator(std::unique_ptr<StorageIterator> a, 
                        std::unique_ptr<StorageIterator> b)
        : a_(std::move(a)), b_(std::move(b)) {
        
        if (!a_ || !b_) {
            state_corrupted_ = true;
            current_.valid = false;
            return;
        }

        SkipDuplicatesInB();
        UpdateCurrentSnapshot();
    }

    /**
     * @brief Factory method with validation
     * 
     * @param a First iterator
     * @param b Second iterator 
     * @return Safe two-merge iterator instance
     */
    static std::unique_ptr<SafeTwoMergeIterator> Create(
        std::unique_ptr<StorageIterator> a,
        std::unique_ptr<StorageIterator> b) {
        return std::make_unique<SafeTwoMergeIterator>(std::move(a), std::move(b));
    }

    /**
     * @brief Get debugging information
     * 
     * @return Number of key comparisons performed
     */
    size_t GetComparisonCount() const noexcept {
        return comparison_count_;
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
     * @brief Get current source information
     * 
     * @return pair of (is_from_a, both_iterators_valid)
     */
    std::pair<bool, bool> GetSourceInfo() const noexcept {
        bool both_valid = a_ && a_->IsValid() && b_ && b_->IsValid();
        return {current_.from_a, both_valid};
    }

protected:
    // FusedIterator implementation
    bool IsValidImpl() const noexcept override {
        if (state_corrupted_) {
            return false;
        }
        return current_.valid;
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
            // Advance the iterator that was chosen for current position
            if (current_.from_a && a_) {
                a_->Next();
            } else if (!current_.from_a && b_) {
                b_->Next();
            }

            // Skip duplicates and update snapshot
            SkipDuplicatesInB();
            UpdateCurrentSnapshot();
            
        } catch (...) {
            state_corrupted_ = true;
            current_.valid = false;
        }
    }
};

/**
 * @brief Factory function for creating safe two-merge iterators
 * 
 * @param a First iterator (priority on duplicates)
 * @param b Second iterator
 * @return FusedIterator with two-merge functionality
 */
inline std::unique_ptr<FusedIterator> CreateSafeTwoMergeIterator(
    std::unique_ptr<StorageIterator> a,
    std::unique_ptr<StorageIterator> b) {
    return SafeTwoMergeIterator::Create(std::move(a), std::move(b));
}

