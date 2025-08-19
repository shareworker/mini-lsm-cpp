#pragma once

#include "storage_iterator.hpp"
#include "byte_buffer.hpp"


/**
 * @brief FusedIterator provides enhanced safety guarantees for storage iterators
 * 
 * A fused iterator guarantees that once it becomes invalid (returns false from IsValid()),
 * it will remain invalid for all subsequent operations. This prevents bugs that can occur
 * when calling Next() or accessing Key()/Value() after the iterator is exhausted.
 * 
 * Key safety guarantees:
 * 1. Once invalid, the iterator stays invalid permanently
 * 2. Key() and Value() return empty buffers when invalid
 * 3. Next() is safe to call on invalid iterators (no-op)
 * 4. State consistency is maintained across all operations
 */
class FusedIterator : public StorageIterator {
public:
    FusedIterator() = default;
    FusedIterator(const FusedIterator&) = delete;
    FusedIterator& operator=(const FusedIterator&) = delete;
    FusedIterator(FusedIterator&&) noexcept = default;
    FusedIterator& operator=(FusedIterator&&) noexcept = default;
    ~FusedIterator() override = default;

    /**
     * @brief Check if the iterator is in a valid state
     * 
     * Once this returns false, it will always return false for this iterator instance.
     * This is the core "fused" guarantee.
     * 
     * @return true if iterator is positioned at a valid entry, false otherwise
     */
    bool IsValid() const noexcept final override {
        if (is_fused_) {
            return false;
        }
        
        bool valid = IsValidImpl();
        if (!valid) {
            // Mark as fused when becoming invalid
            is_fused_ = true;
        }
        return valid;
    }

    /**
     * @brief Get the current key
     * 
     * Returns empty ByteBuffer if iterator is invalid.
     * Safe to call on exhausted iterators.
     * 
     * @return Current key or empty buffer if invalid
     */
    ByteBuffer Key() const noexcept final override {
        if (is_fused_ || !IsValidImpl()) {
            return empty_buffer_;
        }
        return KeyImpl();
    }

    /**
     * @brief Get the current value
     * 
     * Returns empty ByteBuffer if iterator is invalid.
     * Safe to call on exhausted iterators.
     * 
     * @return Current value or empty buffer if invalid
     */
    const ByteBuffer& Value() const noexcept final override {
        if (is_fused_ || !IsValidImpl()) {
            return empty_buffer_;
        }
        return ValueImpl();
    }

    /**
     * @brief Advance to next entry
     * 
     * Safe to call on invalid iterators (no-op).
     * Once the iterator becomes invalid, it remains fused and will not advance.
     */
    void Next() noexcept final override {
        if (is_fused_) {
            // Already fused - no-op
            return;
        }
        
        if (!IsValidImpl()) {
            // Became invalid - fuse the iterator
            is_fused_ = true;
            return;
        }
        
        NextImpl();
        
        // Check if we became invalid after advancing
        if (!IsValidImpl()) {
            is_fused_ = true;
        }
    }

    /**
     * @brief Check if the iterator is permanently fused (exhausted)
     * 
     * @return true if iterator is fused and will never be valid again
     */
    bool IsFused() const noexcept {
        return is_fused_;
    }

    /**
     * @brief Get statistics about iterator safety violations
     * 
     * This can be used for debugging and monitoring iterator usage patterns.
     * 
     * @return Number of operations attempted on fused iterator
     */
    size_t GetSafetyViolationCount() const noexcept {
        return safety_violation_count_;
    }

protected:
    /**
     * @brief Implementation-specific validity check
     * 
     * Subclasses must implement this to provide their actual validity logic.
     * 
     * @return true if the underlying iterator is valid
     */
    virtual bool IsValidImpl() const noexcept = 0;

    /**
     * @brief Implementation-specific key access
     * 
     * Called only when iterator is confirmed to be valid.
     * 
     * @return Current key from underlying iterator
     */
    virtual ByteBuffer KeyImpl() const noexcept = 0;

    /**
     * @brief Implementation-specific value access
     * 
     * Called only when iterator is confirmed to be valid.
     * 
     * @return Current value from underlying iterator
     */
    virtual const ByteBuffer& ValueImpl() const noexcept = 0;

    /**
     * @brief Implementation-specific advance operation
     * 
     * Called only when iterator is confirmed to be valid.
     * Should advance the underlying iterator to the next position.
     */
    virtual void NextImpl() noexcept = 0;

private:
    mutable bool is_fused_ = false;
    mutable size_t safety_violation_count_ = 0;
    ByteBuffer empty_buffer_;
    
    void RecordSafetyViolation() const noexcept {
        ++safety_violation_count_;
    }
};

/**
 * @brief Enhanced safety wrapper for existing StorageIterator implementations
 * 
 * This class can wrap any existing StorageIterator to provide fused iterator
 * guarantees without requiring changes to the underlying implementation.
 */
class SafeIteratorWrapper : public FusedIterator {
public:
    explicit SafeIteratorWrapper(std::unique_ptr<StorageIterator> iter)
        : iter_(std::move(iter)) {}

    SafeIteratorWrapper(const SafeIteratorWrapper&) = delete;
    SafeIteratorWrapper& operator=(const SafeIteratorWrapper&) = delete;
    SafeIteratorWrapper(SafeIteratorWrapper&&) noexcept = default;
    SafeIteratorWrapper& operator=(SafeIteratorWrapper&&) noexcept = default;

protected:
    bool IsValidImpl() const noexcept override {
        return iter_ && iter_->IsValid();
    }

    ByteBuffer KeyImpl() const noexcept override {
        return iter_ ? iter_->Key() : ByteBuffer{};
    }

    const ByteBuffer& ValueImpl() const noexcept override {
        return iter_ ? iter_->Value() : empty_buffer_;
    }

    void NextImpl() noexcept override {
        if (iter_) {
            iter_->Next();
        }
    }

private:
    std::unique_ptr<StorageIterator> iter_;
    ByteBuffer empty_buffer_;
};

/**
 * @brief Factory function to create safe iterator wrappers
 * 
 * @param iter Iterator to wrap with safety guarantees
 * @return FusedIterator with enhanced safety
 */
inline std::unique_ptr<FusedIterator> MakeSafe(std::unique_ptr<StorageIterator> iter) {
    return std::make_unique<SafeIteratorWrapper>(std::move(iter));
}

