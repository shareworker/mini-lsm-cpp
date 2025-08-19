#pragma once

#include "byte_buffer.hpp"


/**
 * @brief Generic interface for forward storage iterators.
 *
 * This is the C++ counterpart of Rust's `StorageIterator` trait.
 * The iterator provides access to key/value pairs and basic cursor
 * navigation. Implementations must guarantee:
 *  - No exceptions are thrown (all methods noexcept).
 *  - After construction the iterator is either valid or invalid; when
 *    invalid, `Key()`/`Value()` return empty `ByteBuffer` references.
 */
class StorageIterator {
public:
    StorageIterator() = default;
    StorageIterator(const StorageIterator&) = delete;
    StorageIterator& operator=(const StorageIterator&) = delete;
    StorageIterator(StorageIterator&&) noexcept = default;
    StorageIterator& operator=(StorageIterator&&) noexcept = default;
    virtual ~StorageIterator() = default;

    /**
     * @return current value buffer; if invalid, returns empty buffer
     */
    virtual const ByteBuffer& Value() const noexcept = 0;

    /**
     * @return current key buffer; if invalid, returns empty buffer
     */
    virtual ByteBuffer Key() const noexcept = 0;

    /**
     * @return whether the iterator is positioned at a valid entry
     */
    virtual bool IsValid() const noexcept = 0;

    /**
     * Advance to next entry. After moving past last element, iterator
     * becomes invalid.
     */
    virtual void Next() noexcept = 0;
};

