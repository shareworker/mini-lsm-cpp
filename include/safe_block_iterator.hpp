#pragma once

#include <memory>

#include "fused_iterator.hpp"
#include "block_iterator.hpp"
#include "block.hpp"


/**
 * @brief Enhanced block iterator with FusedIterator safety guarantees
 * 
 * SafeBlockIterator wraps BlockIterator to provide enhanced safety:
 * - Permanent invalidation once exhausted
 * - Safe operations on invalid iterators
 * - Bounds checking and validation
 * - Consistent state across all operations
 */
class SafeBlockIterator : public FusedIterator {
private:
    std::unique_ptr<BlockIterator> block_iter_;
    
    // Safety state tracking
    mutable size_t access_count_{0};
    mutable bool state_corrupted_{false};
    ByteBuffer empty_buffer_;
    
    // Cached state for consistency
    mutable bool validity_cached_ = false;
    mutable bool is_valid_cache_ = false;

    /**
     * @brief Validate the underlying block iterator state
     * 
     * @return true if iterator appears to be in a valid state
     */
    bool ValidateState() const noexcept {
        if (!block_iter_) {
            state_corrupted_ = true;
            return false;
        }

        try {
            // Basic validation - ensure we can call IsValid without issues
            bool valid = block_iter_->IsValid();
            return true;
        } catch (...) {
            state_corrupted_ = true;
            return false;
        }
    }

    /**
     * @brief Get validity with caching for consistency
     * 
     * @return true if iterator is valid
     */
    bool GetValidityWithCache() const noexcept {
        if (state_corrupted_) {
            return false;
        }

        if (!ValidateState()) {
            return false;
        }

        // Use cached value if available for consistency
        if (validity_cached_) {
            return is_valid_cache_;
        }

        try {
            is_valid_cache_ = block_iter_->IsValid();
            validity_cached_ = true;
            return is_valid_cache_;
        } catch (...) {
            state_corrupted_ = true;
            return false;
        }
    }

    /**
     * @brief Invalidate validity cache
     */
    void InvalidateCache() const noexcept {
        validity_cached_ = false;
        is_valid_cache_ = false;
    }

public:
    /**
     * @brief Constructor with BlockIterator
     * 
     * @param iter Block iterator to wrap safely
     */
    explicit SafeBlockIterator(std::unique_ptr<BlockIterator> iter)
        : block_iter_(std::move(iter)) {
        
        if (!block_iter_) {
            state_corrupted_ = true;
        }
    }

    /**
     * @brief Factory method to create safe block iterator from block
     * 
     * @param block Block to iterate over
     * @return Safe block iterator instance
     */
    static std::unique_ptr<SafeBlockIterator> CreateAndSeekToFirst(
        std::shared_ptr<const Block> block) {
        
        if (!block) {
            return std::make_unique<SafeBlockIterator>(nullptr);
        }

        try {
            auto block_iter = std::make_unique<BlockIterator>(
                BlockIterator::CreateAndSeekToFirst(std::move(block))
            );
            return std::make_unique<SafeBlockIterator>(std::move(block_iter));
        } catch (...) {
            return std::make_unique<SafeBlockIterator>(nullptr);
        }
    }

    /**
     * @brief Factory method to create safe block iterator and seek to key
     * 
     * @param block Block to iterate over
     * @param key Key to seek to
     * @return Safe block iterator instance
     */
    static std::unique_ptr<SafeBlockIterator> CreateAndSeekToKey(
        std::shared_ptr<const Block> block, 
        const ByteBuffer& key) {
        
        if (!block) {
            return std::make_unique<SafeBlockIterator>(nullptr);
        }

        try {
            auto block_iter = std::make_unique<BlockIterator>(
                BlockIterator::CreateAndSeekToKey(std::move(block), key)
            );
            return std::make_unique<SafeBlockIterator>(std::move(block_iter));
        } catch (...) {
            return std::make_unique<SafeBlockIterator>(nullptr);
        }
    }

    /**
     * @brief Get debugging information
     * 
     * @return Number of access operations performed
     */
    size_t GetAccessCount() const noexcept {
        return access_count_;
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
     * @brief Get cache statistics
     * 
     * @return pair of (cache_valid, cached_validity)
     */
    std::pair<bool, bool> GetCacheStats() const noexcept {
        return {validity_cached_, is_valid_cache_};
    }

protected:
    // FusedIterator implementation
    bool IsValidImpl() const noexcept override {
        ++access_count_;
        return GetValidityWithCache();
    }

    ByteBuffer KeyImpl() const noexcept override {
        ++access_count_;
        
        if (state_corrupted_ || !block_iter_) {
            return ByteBuffer{};
        }

        try {
            return block_iter_->Key();
        } catch (...) {
            state_corrupted_ = true;
            return ByteBuffer{};
        }
    }

    const ByteBuffer& ValueImpl() const noexcept override {
        ++access_count_;
        
        if (state_corrupted_ || !block_iter_) {
            return empty_buffer_;
        }

        try {
            const ByteBuffer& value = block_iter_->Value();
            return value;
        } catch (...) {
            state_corrupted_ = true;
            return empty_buffer_;
        }
    }

    void NextImpl() noexcept override {
        ++access_count_;
        
        if (state_corrupted_ || !block_iter_) {
            return;
        }

        try {
            InvalidateCache();
            block_iter_->Next();
        } catch (...) {
            state_corrupted_ = true;
        }
    }
};

/**
 * @brief Factory function for creating safe block iterators
 * 
 * @param block Block to iterate over safely
 * @return FusedIterator with block iteration functionality
 */
inline std::unique_ptr<FusedIterator> CreateSafeBlockIterator(
    std::shared_ptr<const Block> block) {
    return SafeBlockIterator::CreateAndSeekToFirst(std::move(block));
}

/**
 * @brief Factory function for creating safe block iterators with key seek
 * 
 * @param block Block to iterate over safely
 * @param key Key to seek to
 * @return FusedIterator with block iteration functionality
 */
inline std::unique_ptr<FusedIterator> CreateSafeBlockIteratorSeekToKey(
    std::shared_ptr<const Block> block,
    const ByteBuffer& key) {
    return SafeBlockIterator::CreateAndSeekToKey(std::move(block), key);
}

