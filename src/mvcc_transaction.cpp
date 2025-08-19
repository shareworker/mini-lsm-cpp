#include "../include/mvcc_transaction.hpp"
#include "../include/mvcc_lsm_storage.hpp"
#include "../include/bound.hpp"
#include "../include/mvcc_lsm_iterator.hpp"
#include "../include/mvcc.hpp"
#include <algorithm>
#include <cassert>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <map>
#include <mutex>
#include <utility>


std::unique_ptr<MvccTransaction> MvccTransaction::Begin(
    std::shared_ptr<MvccLsmStorage> storage,
    IsolationLevel isolation_level,
    std::optional<uint64_t> read_ts) {
    
    // Use provided read_ts or get a new one from storage
    uint64_t actual_read_ts = read_ts.value_or(storage->GetNextTimestamp());
    
    printf("[MVCC] Begin Transaction: read_ts=%lu, isolation=%s\n", 
           actual_read_ts, (isolation_level == IsolationLevel::kSerializable ? "Serializable" : "SnapshotIsolation"));
    fflush(stdout);
    
    return std::unique_ptr<MvccTransaction>(
        new MvccTransaction(std::move(storage), isolation_level, actual_read_ts));
}

MvccTransaction::MvccTransaction(
    std::shared_ptr<MvccLsmStorage> storage,
    IsolationLevel isolation_level,
    uint64_t read_ts)
    : storage_(std::move(storage)), 
      isolation_level_(isolation_level), 
      read_ts_(read_ts) {}

bool MvccTransaction::Put(const ByteBuffer& key, const ByteBuffer& value) {
    printf("[MVCC] Transaction::Put: key=%.*s, value=%.*s\n", 
           (int)key.Size(), key.Data(), (int)value.Size(), value.Data());
    fflush(stdout);
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Check if the transaction is still active
    if (state_ != State::kActive) {
        return false;
    }
    
    // For serializable isolation, we need to record any key we read before
    // If this key is in our read set but not in our write set, we've read it before
    // writing to it, which is fine for snapshot isolation but might create conflicts
    // for serializable isolation. We'll handle this at commit time.
    
    // Add the key-value pair to our write set
    write_set_[key.Clone()] = value.Clone();
    
    return true;
}

ByteBuffer MvccTransaction::Get(const ByteBuffer& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Check if the transaction is still active
    if (state_ != State::kActive) {
        std::cout << "[MVCC] Get Failed: transaction not active, read_ts=" << read_ts_ << ", key=" << key.ToString() << std::endl;
        return ByteBuffer{}; // Return empty ByteBuffer
    }
    std::cout << "[MVCC] Get Start: read_ts=" << read_ts_ << ", key=" << key.ToString() << std::endl;
    
    // First check the local write set
    auto it = write_set_.find(key);
    if (it != write_set_.end()) {
        std::cout << "DEBUG: Found key in write set" << std::endl;
        return it->second;
    }
    
    // For serializable isolation, track keys read
    if (isolation_level_ == IsolationLevel::kSerializable) {
        read_set_.insert(key.Clone());
    }
    
    // Then check the storage
    std::cout << "DEBUG: Checking storage with read_ts: " << read_ts_ << std::endl;
    auto result = storage_->GetWithTs(key, read_ts_);
    if (result.has_value()) {
        std::cout << "DEBUG: Found value in storage, size: " << result->Size() << std::endl;
        return *result; // Return the actual ByteBuffer
    } else {
        std::cout << "DEBUG: No value found in storage" << std::endl;
        return ByteBuffer{}; // Return empty ByteBuffer
    }
}

bool MvccTransaction::Delete(const ByteBuffer& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Check if the transaction is still active
    if (state_ != State::kActive) {
        return false;
    }
    
    // Add a tombstone to our write set
    write_set_[key.Clone()] = ByteBuffer();
    
    return true;
}

std::unique_ptr<StorageIterator> MvccTransaction::Scan(
    const Bound& lower_bound,
    const Bound& upper_bound) {
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Check if the transaction is still active
    if (state_ != State::kActive) {
        return nullptr;
    }
    
    std::cout << "[DEBUG] MvccTransaction::Scan called with read_ts=" << read_ts_ << std::endl;
    
    // Get the storage iterator from the underlying MVCC LSM storage
    auto storage_iter = storage_->ScanWithTs(lower_bound, upper_bound, read_ts_);
    std::cout << "[DEBUG] Storage iterator valid: " << (storage_iter.IsValid() ? "true" : "false") << std::endl;
    
    // For serializable isolation, we need to track all keys that we read
    // This is important for detecting conflicts at commit time
    if (isolation_level_ == IsolationLevel::kSerializable) {
        // Create a tracking iterator that will add keys to the read set
        // We'll implement this later if needed
    }
    
    // Now we need to merge our local write set with the storage iterator
    // First, filter our write set to only include keys in the requested range
    std::map<ByteBuffer, ByteBuffer> filtered_writes;
    
    for (const auto& [key, value] : write_set_) {
        // Check if this key is within the scan range
        bool in_range = true;
        
        // Check lower bound
        if (lower_bound.GetType() != Bound::Type::kUnbounded) {
            if (lower_bound.GetType() == Bound::Type::kIncluded && key < *lower_bound.Key()) {
                in_range = false; // Key is less than lower bound (excluded)
            } else if (lower_bound.GetType() == Bound::Type::kExcluded && 
                      !(key > *lower_bound.Key())) {
                in_range = false; // Key is less than or equal to lower bound (excluded)
            }
        }
        
        // Check upper bound
        if (in_range && upper_bound.GetType() != Bound::Type::kUnbounded) {
            if (upper_bound.GetType() == Bound::Type::kIncluded && key > *upper_bound.Key()) {
                in_range = false; // Key is greater than upper bound (excluded)
            } else if (upper_bound.GetType() == Bound::Type::kExcluded && 
                      !(key < *upper_bound.Key())) {
                in_range = false; // Key is greater than or equal to upper bound (excluded)
            }
        }
        
        if (in_range) {
            filtered_writes[key] = value;
        }
    }
    
    // Always collect data from storage iterator to ensure user keys are properly extracted
    // Even if we have no local writes, we need to convert versioned keys to user keys
    
    // Otherwise, create a merged iterator that combines our local writes with storage data
    // For now, we'll use a simple approach: load all data into memory and create a custom iterator
    // A more efficient implementation would use a true merge iterator
    
    // First, collect all keys and values from the storage
    std::map<ByteBuffer, ByteBuffer> all_data;
    
    // Load data from storage iterator
    while (storage_iter.IsValid()) {
        // Extract user key (without timestamp) from the versioned key
        ByteBuffer versioned_key = storage_iter.Key();
        ByteBuffer user_key;
        
        // Extract user key (strip timestamp)
        // MVCC keys are stored as [user_key][8_byte_timestamp]
        if (versioned_key.Size() > sizeof(uint64_t)) {
            user_key = ByteBuffer(versioned_key.Data(), versioned_key.Size() - sizeof(uint64_t));
        } else {
            user_key = versioned_key.Clone();
        }
        
        ByteBuffer value = storage_iter.Value();
        all_data[user_key] = value.Clone();
        storage_iter.Next();
    }
    
    // Now overlay our local writes (they take precedence)
    for (const auto& [key, value] : filtered_writes) {
        if (value.Empty()) {
            // This is a tombstone, remove the key
            all_data.erase(key);
        } else {
            // This is a regular write, update or insert
            all_data[key] = value;
        }
    }
    
    // Create a custom iterator over the merged data
    std::cout << "[DEBUG] Creating MvccTransactionIterator with " << all_data.size() << " keys" << std::endl;
    for (const auto& [key, value] : all_data) {
        std::string key_str(reinterpret_cast<const char*>(key.Data()), key.Size());
        std::cout << "[DEBUG] Key: " << key_str << std::endl;
    }
    return std::make_unique<MvccTransactionIterator>(std::move(all_data));
}

bool MvccTransaction::Commit() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Check if the transaction is still active
    if (state_ != State::kActive) {
        printf("[MVCC] Commit Failed: transaction not active, read_ts=%lu\n", read_ts_);
        fflush(stdout);
        return false;
    }
    
    // For serializable isolation, check for conflicts with enhanced detection
    if (isolation_level_ == IsolationLevel::kSerializable) {
        printf("[MVCC] Checking serializable conflicts for transaction with read_ts=%lu\n", read_ts_);
        
        // Prepare separate read and write key hash sets for conflict detection
        std::unordered_set<uint32_t> read_key_hashes;
        std::unordered_set<uint32_t> write_key_hashes;
        
        // Add read set key hashes
        for (const auto& key : read_set_) {
            uint32_t hash = std::hash<std::string>{}(key.ToString());
            read_key_hashes.insert(hash);
            printf("[MVCC] Adding read key to conflict check: %s (hash=%u)\n", 
                   key.ToString().c_str(), hash);
        }
        
        // Add write set key hashes
        for (const auto& [key, _] : write_set_) {
            uint32_t hash = std::hash<std::string>{}(key.ToString());
            write_key_hashes.insert(hash);
            printf("[MVCC] Adding write key to conflict check: %s (hash=%u)\n", 
                   key.ToString().c_str(), hash);
        }
        
        // Get the MVCC inner from the storage to check for conflicts
        auto mvcc_inner = storage_->GetMvccInner();
        if (!mvcc_inner) {
            printf("[MVCC] Error: No MVCC inner available for conflict checking\n");
            
            // Clean up watermark on error
            auto mvcc_inner_for_watermark = storage_->GetMvccInner();
            if (mvcc_inner_for_watermark) {
                mvcc_inner_for_watermark->RemoveReader(read_ts_);
            }
            
            state_ = State::kAborted;
            return false;
        }
        
        // Check for conflicts with the prepared key hashes
        bool no_conflicts = mvcc_inner->CheckSerializableNoConflicts(read_ts_, read_key_hashes, write_key_hashes);
        printf("[MVCC] Serializable conflict check result: %s\n", 
               no_conflicts ? "No conflicts" : "Conflicts detected");
        
        if (!no_conflicts) {
            // Conflicts detected, cannot commit
            printf("[MVCC] Serialization conflict detected, aborting transaction\n");
            
            // Clean up watermark on conflict abort
            auto mvcc_inner_for_watermark = storage_->GetMvccInner();
            if (mvcc_inner_for_watermark) {
                mvcc_inner_for_watermark->RemoveReader(read_ts_);
                printf("[MVCC] Removed reader from watermark on conflict abort: read_ts=%lu\n", read_ts_);
            }
            
            state_ = State::kAborted;
            return false;
        }
        
        printf("[MVCC] No serialization conflicts detected, proceeding with commit\n");
    }
    
    // Get a write timestamp from the MVCC inner's global timestamp counter
    // This ensures all transactions use the same counter for both read and write timestamps
    auto mvcc_inner = storage_->GetInner()->GetMvcc();
    if (!mvcc_inner) {
        printf("[MVCC] Error: No MVCC inner available for timestamp assignment\n");
        state_ = State::kAborted;
        return false;
    }
    
    write_ts_ = mvcc_inner->GetNextTimestamp();
    
    // Ensure write_ts > read_ts for MVCC correctness
    // If the assigned write_ts is not greater than read_ts, get a new timestamp
    while (write_ts_ <= read_ts_) {
        write_ts_ = mvcc_inner->GetNextTimestamp();
    }
    
    printf("[MVCC] Transaction commit: read_ts=%lu, write_ts=%lu\n", read_ts_, write_ts_);
    
    // Apply all writes to storage atomically with our write timestamp
    // For better atomicity, we should batch all writes and apply them together
    // If any write fails, we should abort the entire transaction
    
    std::vector<std::pair<ByteBuffer, ByteBuffer>> successful_writes;
    successful_writes.reserve(write_set_.size());
    
    bool all_writes_successful = true;
    
    for (const auto& [key, value] : write_set_) {
        bool write_success = false;
        
        if (value.Empty()) {
            // Tombstone - delete
            write_success = storage_->DeleteWithTs(key, write_ts_);
            printf("[MVCC] Delete operation for key=%s, success=%s\n", 
                   key.ToString().c_str(), write_success ? "true" : "false");
        } else {
            // Normal write
            write_success = storage_->PutWithTs(key, value, write_ts_);
            printf("[MVCC] Put operation for key=%s, success=%s\n", 
                   key.ToString().c_str(), write_success ? "true" : "false");
        }
        
        if (!write_success) {
            printf("[MVCC] Write failed for key=%s, aborting transaction\n", key.ToString().c_str());
            all_writes_successful = false;
            break;
        }
        
        // Track successful writes for potential rollback
        successful_writes.emplace_back(key.Clone(), value.Clone());
    }
    
    // If any write failed, abort the transaction
    if (!all_writes_successful) {
        printf("[MVCC] Transaction commit failed due to write errors, aborting\n");
        state_ = State::kAborted;
        
        // Remove reader from watermark even on abort
        auto mvcc_inner_for_watermark = storage_->GetMvccInner();
        if (mvcc_inner_for_watermark) {
            mvcc_inner_for_watermark->RemoveReader(read_ts_);
        }
        
        return false;
    }
    
    // Record this transaction for serializable conflict checking
    if (isolation_level_ == IsolationLevel::kSerializable && !write_set_.empty()) {
        // Prepare transaction data for conflict tracking
        CommittedTxnData txn_data;
        txn_data.read_ts = read_ts_;
        txn_data.commit_ts = write_ts_;
        
        // Add key hashes to separate read and write sets
        for (const auto& key : read_set_) {
            uint32_t hash = std::hash<std::string>{}(key.ToString());
            txn_data.read_set.insert(hash);
        }
        for (const auto& [key, _] : write_set_) {
            uint32_t hash = std::hash<std::string>{}(key.ToString());
            txn_data.write_set.insert(hash);
        }
        
        mvcc_inner->AddCommittedTransaction(write_ts_, std::move(txn_data));
    }
    
    // Remove reader from watermark to allow garbage collection
    {
        auto mvcc_inner_for_watermark = storage_->GetMvccInner();
        if (mvcc_inner_for_watermark) {
            mvcc_inner_for_watermark->RemoveReader(read_ts_);
            printf("[MVCC] Removed reader from watermark: read_ts=%lu\n", read_ts_);
        }
    }
    
    // Mark as committed
    state_ = State::kCommitted;
    
    return true;
}

void MvccTransaction::Abort() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Remove reader from watermark to allow garbage collection
    {
        auto mvcc_inner = storage_->GetMvccInner();
        if (mvcc_inner) {
            mvcc_inner->RemoveReader(read_ts_);
            printf("[MVCC] Removed reader from watermark on abort: read_ts=%lu\n", read_ts_);
        }
    }
    
    // Mark as aborted
    state_ = State::kAborted;
}

