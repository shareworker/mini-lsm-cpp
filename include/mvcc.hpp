#pragma once

// Multi-Version Concurrency Control (MVCC) for Mini-LSM
// Copyright (c) 2025
// Licensed under Apache 2.0.
//
// This header defines the core MVCC functionality for the LSM storage engine,
// providing snapshot isolation and serializable transaction support.

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_set>
#include <map>
#include <atomic> // Added atomic header

#include "byte_buffer.hpp"
#include "mvcc_watermark.hpp"

// Forward declarations
class LsmStorageInner;
class Transaction;
class MvccTransaction;
class Watermark;

/**
 * @brief Data structure to track committed transaction information
 */
struct CommittedTxnData {
    // Set of key hashes that were read by this transaction
    std::unordered_set<uint32_t> read_set;
    // Set of key hashes that were written by this transaction
    std::unordered_set<uint32_t> write_set;
    // Transaction read timestamp
    uint64_t read_ts;
    // Transaction commit timestamp
    uint64_t commit_ts;
};

/**
 * @brief Core MVCC implementation for LSM storage
 * 
 * This class manages transaction timestamps, watermarks for garbage collection,
 * and transaction coordination.
 */
class LsmMvccInner {
public:
    /**
     * @brief Construct a new LsmMvccInner object
     * 
     * @param initial_ts Initial timestamp to start with
     */
    explicit LsmMvccInner(uint64_t initial_ts);

    /**
     * @brief Get the latest commit timestamp
     * 
     * @return uint64_t Current commit timestamp
     */
    uint64_t LatestCommitTs() const;

    /**
     * @brief Update the commit timestamp
     * 
     * @param ts New commit timestamp
     */
    void UpdateCommitTs(uint64_t ts);

    /**
     * @brief Get the watermark timestamp
     * 
     * All timestamps strictly below this can be garbage collected.
     * 
     * @return uint64_t Watermark timestamp
     */
    uint64_t GetWatermark() const;

    /**
     * @brief Get the next available timestamp
     * 
     * This method provides a monotonically increasing timestamp for both
     * read and write operations, ensuring proper MVCC ordering.
     * 
     * @return uint64_t Next available timestamp
     */
    uint64_t GetNextTimestamp();

    /**
     * @brief Create a new transaction
     * 
     * @param inner LSM storage inner reference
     * @param serializable Whether the transaction should be serializable
     * @return std::shared_ptr<MvccTransaction> New MVCC transaction
     */
    std::shared_ptr<MvccTransaction> NewTxn(
        std::shared_ptr<LsmStorageInner> inner, 
        bool serializable);
        
    /**
     * @brief Check if a serializable transaction has conflicts with any committed transaction
     * 
     * This method checks if there are any conflicts between the transaction's read/write set
     * and any transaction that committed between the transaction's start and commit time.
     * 
     * @param start_ts Transaction start timestamp
     * @param read_key_hashes Set of key hashes read by the transaction
     * @param write_key_hashes Set of key hashes written by the transaction
     * @return true if no conflicts exist, false if conflicts detected
     */
    bool CheckSerializableNoConflicts(
        uint64_t start_ts,
        const std::unordered_set<uint32_t>& read_key_hashes,
        const std::unordered_set<uint32_t>& write_key_hashes) const;
        
    /**
     * @brief Store a committed transaction for serializable isolation checks
     * 
     * This method is called after a successful transaction commit to record
     * the keys that were written by the transaction.
     * 
     * @param commit_ts Transaction commit timestamp
     * @param txn_data Transaction data with key hashes
     */
    void AddCommittedTransaction(uint64_t commit_ts, CommittedTxnData txn_data);
    
    /**
     * @brief Remove a reader from the watermark
     * 
     * This method is called when a transaction ends (commits or aborts)
     * to update the watermark and enable garbage collection.
     * 
     * @param read_ts Reader timestamp to remove
     */
    void RemoveReader(uint64_t read_ts);
    
    /**
     * @brief Clean up committed transactions older than the watermark
     * 
     * This method removes committed transaction records that are older
     * than the watermark timestamp, as they are no longer visible to
     * any active readers and can be safely garbage collected.
     * 
     * @param watermark Watermark timestamp - transactions older than this can be removed
     * @return size_t Number of transactions that were cleaned up
     */
    size_t CleanupCommittedTransactionsBelowWatermark(uint64_t watermark);

private:
    // Lock for write operations
    mutable std::mutex write_lock_;
    
    // Lock for commit operations
    mutable std::mutex commit_lock_;
    
    // Timestamp and watermark state
    struct TsState {
        uint64_t ts;
        std::unique_ptr<Watermark> watermark;
    };
    mutable std::mutex ts_mutex_;
    TsState ts_state_;
    
    // Atomic timestamp counter for both read and write timestamps
    std::atomic<uint64_t> next_ts_;
    
    // Committed transactions
    mutable std::mutex committed_txns_mutex_;
    std::map<uint64_t, CommittedTxnData> committed_txns_;
};

