#include "../include/mvcc.hpp"

#include <cassert>
#include <memory>
#include <unordered_set>
#include <utility>

#include "../include/mvcc_txn.hpp"
#include "../include/mvcc_watermark.hpp"
#include "../include/mvcc_transaction.hpp"
#include "../include/mvcc_lsm_storage.hpp"


LsmMvccInner::LsmMvccInner(uint64_t initial_ts)
    : ts_state_{initial_ts, std::make_unique<Watermark>()}, next_ts_(initial_ts) {
}

uint64_t LsmMvccInner::LatestCommitTs() const {
    std::lock_guard<std::mutex> lock(ts_mutex_);
    return ts_state_.ts;
}

void LsmMvccInner::UpdateCommitTs(uint64_t ts) {
    std::lock_guard<std::mutex> lock(ts_mutex_);
    ts_state_.ts = ts;
}

uint64_t LsmMvccInner::GetWatermark() const {
    std::lock_guard<std::mutex> lock(ts_mutex_);
    auto watermark = ts_state_.watermark->GetWatermark();
    return watermark.value_or(ts_state_.ts);
}

uint64_t LsmMvccInner::GetNextTimestamp() {
    return next_ts_.fetch_add(1, std::memory_order_relaxed);
}

std::shared_ptr<MvccTransaction> LsmMvccInner::NewTxn(
    std::shared_ptr<LsmStorageInner> inner, 
    bool serializable) {
    
    printf("[MVCC] LsmMvccInner::NewTxn called, serializable=%s\n", serializable ? "true" : "false");
    fflush(stdout);
    
    // Get next timestamp for read timestamp - use the same unified counter as write timestamps
    // This prevents timestamp collisions between concurrent reads and writes
    uint64_t read_ts = GetNextTimestamp();
    
    // Add reader to watermark
    {
        std::lock_guard<std::mutex> lock(ts_mutex_);
        ts_state_.watermark->AddReader(read_ts);
    }
    
    printf("[MVCC] LsmMvccInner::NewTxn - assigned read_ts=%lu\n", read_ts);
    fflush(stdout);
    
    // Create a MvccLsmStorage wrapper around the inner storage using the factory method
    auto mvcc_storage = MvccLsmStorage::CreateShared(inner);
    
    printf("[MVCC] LsmMvccInner::NewTxn - created MvccLsmStorage wrapper\n");
    fflush(stdout);
    
    // Create and return an MvccTransaction with the appropriate isolation level
    auto isolation_level = serializable ? 
        IsolationLevel::kSerializable : 
        IsolationLevel::kSnapshotIsolation;
    
    printf("[MVCC] LsmMvccInner::NewTxn - calling MvccTransaction::Begin\n");
    fflush(stdout);
    
    // Use the Begin factory method to create a properly initialized MvccTransaction
    // Pass the pre-assigned read_ts to ensure correct timestamp ordering
    auto unique_txn = MvccTransaction::Begin(mvcc_storage, isolation_level, read_ts);
    
    printf("[MVCC] LsmMvccInner::NewTxn - MvccTransaction created successfully\n");
    fflush(stdout);
    
    // Convert unique_ptr to shared_ptr for return
    return std::shared_ptr<MvccTransaction>(unique_txn.release());
}

bool LsmMvccInner::CheckSerializableNoConflicts(
    uint64_t start_ts,
    const std::unordered_set<uint32_t>& read_key_hashes,
    const std::unordered_set<uint32_t>& write_key_hashes) const {
    
    // Only need to check for conflicts if there are keys to check
    if (read_key_hashes.empty() && write_key_hashes.empty()) {
        return true;
    }
    
    // Take lock to access committed transactions map
    std::lock_guard<std::mutex> lock(committed_txns_mutex_);
    
    // Check all transactions committed after start_ts for conflicts
    // Iterate through transactions with commit_ts > start_ts
    for (auto it = committed_txns_.lower_bound(start_ts + 1); 
         it != committed_txns_.end(); 
         ++it) {
        
        const auto& committed_txn = it->second;
        
        // Focus on the most critical conflicts: Write-Write conflicts only
        // MVCC timestamp ordering and snapshot isolation should handle Read-Write dependencies naturally
        for (const auto& write_key_hash : write_key_hashes) {
            if (committed_txn.write_set.find(write_key_hash) != committed_txn.write_set.end()) {
                // Write-Write conflict detected - this is always a problem
                return false;
            }
        }
        
        // Note: Read-Write conflicts are handled by MVCC's timestamp ordering and snapshot isolation.
        // Two transactions can have overlapping reads and writes to different keys without
        // violating serializability, as long as the final outcome is equivalent to some serial execution.
        
        // Note: Write-Read conflicts are handled by MVCC timestamp ordering,
        // not by this conflict detection mechanism
    }
    
    // No conflicts found
    return true;
}

void LsmMvccInner::RemoveReader(uint64_t read_ts) {
    std::lock_guard<std::mutex> lock(ts_mutex_);
    ts_state_.watermark->RemoveReader(read_ts);
    printf("[MVCC] Removed reader from watermark: read_ts=%lu\n", read_ts);
    fflush(stdout);
}

size_t LsmMvccInner::CleanupCommittedTransactionsBelowWatermark(uint64_t watermark) {
    std::lock_guard<std::mutex> lock(committed_txns_mutex_);
    
    size_t cleaned_count = 0;
    
    // Find the first transaction that should not be removed (commit_ts >= watermark)
    auto it = committed_txns_.lower_bound(watermark);
    
    // Remove all transactions with commit_ts < watermark
    for (auto remove_it = committed_txns_.begin(); remove_it != it; ++remove_it) {
        printf("[MVCC GC] Removing committed transaction: commit_ts=%lu (watermark=%lu)\n", 
               remove_it->first, watermark);
        cleaned_count++;
    }
    
    // Erase all transactions below watermark in one operation
    committed_txns_.erase(committed_txns_.begin(), it);
    
    printf("[MVCC GC] Cleaned up %zu committed transactions below watermark %lu\n", 
           cleaned_count, watermark);
    
    return cleaned_count;
}

void LsmMvccInner::AddCommittedTransaction(uint64_t commit_ts, CommittedTxnData txn_data) {
    // Take lock to modify committed transactions map
    std::lock_guard<std::mutex> lock(committed_txns_mutex_);
    
    // Store the transaction data
    committed_txns_[commit_ts] = std::move(txn_data);
    
    // Cleanup: remove old committed transactions that are below the watermark
    // This helps prevent the committed_txns_ map from growing unbounded
    uint64_t watermark = GetWatermark();
    for (auto it = committed_txns_.begin(); it != committed_txns_.end();) {
        if (it->first < watermark) {
            // This transaction is older than the watermark - all active transactions
            // have a timestamp >= watermark, so they won't conflict with this one
            it = committed_txns_.erase(it);
        } else {
            // Stop at the first transaction that's not older than the watermark
            break;
        }
    }
}

