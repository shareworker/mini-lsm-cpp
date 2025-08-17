#pragma once

// MVCC Transaction implementation - C++ port of Rust txn.rs
// Copyright (c) 2025
// Licensed under Apache 2.0.
//
// This header defines the Transaction class and related iterators for MVCC.

#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "bound.hpp"
#include "byte_buffer.hpp"
#include "skip_map.hpp"
#include "storage_iterator.hpp"

namespace util {

// Forward declarations
class LsmStorageInner;
class SkipMap;

/**
 * @brief MVCC Transaction class
 * 
 * Provides snapshot isolation or serializable isolation for database operations.
 */
class Transaction : public std::enable_shared_from_this<Transaction> {
    friend class TxnLocalIterator;
    friend class TxnIterator;

public:
    static std::shared_ptr<Transaction> New(std::weak_ptr<LsmStorageInner> storage,
                                            uint64_t start_ts,
                                            bool serializable = false);

    ~Transaction() = default;

    ByteBuffer Get(const ByteBuffer& key);

    void Put(const ByteBuffer& key, const ByteBuffer& value);

    void Delete(const ByteBuffer& key);

    bool Commit();

    std::unique_ptr<StorageIterator> Scan(const Bound& lower, const Bound& upper);

private:
    // Passkey for controlled iterator construction
    struct IteratorPasskey {
        explicit IteratorPasskey() = default;
    };

    Transaction(std::weak_ptr<LsmStorageInner> storage, uint64_t start_ts, bool serializable);

    // Transaction state
    std::weak_ptr<LsmStorageInner> storage_;
    uint64_t start_ts_;
    std::shared_ptr<SkipMap> local_storage_;
    bool committed_{false};
    bool serializable_{false}; // Whether serializable isolation is enabled

    // Read and write sets for serializable transactions
    std::unordered_map<ByteBuffer, ByteBuffer> write_set_;
    std::unordered_set<ByteBuffer> read_set_;
};

/**
 * @brief Iterator over transaction's local storage
 */
class TxnLocalIterator : public StorageIterator {
public:
    // Public constructor, but requires a passkey that only Transaction can create.
    TxnLocalIterator(Transaction::IteratorPasskey,
                     std::shared_ptr<SkipMap> storage,
                     const Bound& lower,
                     const Bound& upper);

    // StorageIterator implementation
    const ByteBuffer& Value() const noexcept override;
    ByteBuffer Key() const noexcept override;
    bool IsValid() const noexcept override;
    void Next() noexcept override;

private:
    // SkipMap storage reference
    std::shared_ptr<SkipMap> storage_;
    
    // Underlying range iterator from the SkipMap.
    std::unique_ptr<SkipMap::RangeIterator> range_iter_;
};

/**
 * @brief Transaction iterator combining local and storage iterators
 */
class TxnIterator : public StorageIterator {
public:
    // Public constructor, but requires a passkey that only Transaction can create.
    TxnIterator(Transaction::IteratorPasskey,
                std::shared_ptr<Transaction> txn,
                std::unique_ptr<StorageIterator> iter);

    // StorageIterator implementation
    const ByteBuffer& Value() const noexcept override;
    ByteBuffer Key() const noexcept override;
    bool IsValid() const noexcept override;
    void Next() noexcept override;

private:
    // Transaction reference (keep alive)
    std::shared_ptr<Transaction> txn_;
    
    // Merged iterator
        std::unique_ptr<StorageIterator> iter_;
};

} // namespace util
