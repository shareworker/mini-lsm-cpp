#include "../include/mvcc_txn.hpp"

#include <cassert>
#include <iostream>
#include <utility>

#include "../include/lsm_storage.hpp"
#include "../include/two_merge_iterator.hpp"
#include "../include/mvcc.hpp"

namespace {
    /**
     * @brief Encodes a key with timestamp for versioned storage
     * Format: <user_key><timestamp> where timestamp is 8 bytes big-endian
     */
    ByteBuffer EncodeKeyWithTs(const ByteBuffer& key, uint64_t ts) {
        std::vector<uint8_t> encoded_data;
        encoded_data.reserve(key.Size() + sizeof(uint64_t));
        
        // Copy user key
        encoded_data.insert(encoded_data.end(), key.Data(), key.Data() + key.Size());
        
        // Append timestamp as big-endian 8 bytes
        for (int i = 7; i >= 0; --i) {
            encoded_data.push_back(static_cast<uint8_t>((ts >> (i * 8)) & 0xFF));
        }
        
        return ByteBuffer(encoded_data.data(), encoded_data.size());
    }

    /**
     * @brief Decode a versioned key to extract user key and timestamp
     * 
     * @param versioned_key The versioned key (user_key + 8-byte timestamp)
     * @return std::pair<ByteBuffer, uint64_t> User key and timestamp
     */
    [[maybe_unused]] std::pair<ByteBuffer, uint64_t> DecodeKeyWithTs(const ByteBuffer& versioned_key) {
        if (versioned_key.Size() < sizeof(uint64_t)) {
            // Invalid versioned key, return empty key and 0 timestamp
            return {ByteBuffer{}, 0};
        }
        
        size_t user_key_size = versioned_key.Size() - sizeof(uint64_t);
        
        // Extract user key (first part)
        ByteBuffer user_key(versioned_key.Data(), user_key_size);
        
        // Extract timestamp (last 8 bytes, big-endian)
        const uint8_t* ts_data = reinterpret_cast<const uint8_t*>(versioned_key.Data()) + user_key_size;
        uint64_t timestamp = 0;
        for (int i = 0; i < 8; ++i) {
            timestamp = (timestamp << 8) | ts_data[i];
        }
        
        return {user_key, timestamp};
    }

    /**
     * @brief Check if a versioned key matches the given user key
     * 
     * @param versioned_key The versioned key to check
     * @param user_key The user key to match against
     * @return bool True if the versioned key contains the user key
     */
    [[maybe_unused]] bool VersionedKeyMatches(const ByteBuffer& versioned_key, const ByteBuffer& user_key) {
        if (versioned_key.Size() < user_key.Size() + sizeof(uint64_t)) {
            return false;
        }
        
        // Compare the user key portion (first part of versioned key)
        return std::memcmp(versioned_key.Data(), user_key.Data(), user_key.Size()) == 0;
    }
}


// --- Transaction ---

std::shared_ptr<Transaction> Transaction::New(
    std::weak_ptr<LsmStorageInner> storage, uint64_t start_ts, bool serializable) {
    // Note: can't use make_shared because constructor is private
    return std::shared_ptr<Transaction>(new Transaction(std::move(storage), start_ts, serializable));
}

Transaction::Transaction(std::weak_ptr<LsmStorageInner> storage, uint64_t start_ts, bool serializable)
    : storage_(std::move(storage)),
      start_ts_(start_ts),
      local_storage_(std::make_shared<SkipMap>()),
      serializable_(serializable) {}

ByteBuffer Transaction::Get(const ByteBuffer& key) {
    assert(!committed_);
    read_set_.insert(key);
    

    
    // 1. Check local storage for key
    auto value = local_storage_->Get(key);
    if (value.has_value()) {
        std::cout << "DEBUG Transaction::Get: Found in local storage" << std::endl;
        if (value->Empty()) { // Tombstone
            std::cout << "DEBUG Transaction::Get: Found tombstone in local storage" << std::endl;
            return ByteBuffer{};
        }
        std::cout << "DEBUG Transaction::Get: Returning value from local storage, size: " << value->Size() << std::endl;
        return value.value();
    }

    // 2. Check committed storage for key with timestamp awareness
    auto strong_storage = storage_.lock();
    assert(strong_storage);  // Storage should not be dropped
    
    auto result = strong_storage->GetWithTs(key, start_ts_);
    if (result.has_value()) {

        return result.value();
    } else {

        return ByteBuffer{};
    }
}

void Transaction::Put(const ByteBuffer& key, const ByteBuffer& value) {
    assert(!committed_);
    write_set_[key] = value;
    local_storage_->Insert(key, value);
}

void Transaction::Delete(const ByteBuffer& key) {
    assert(!committed_);
    write_set_[key] = ByteBuffer{};
    local_storage_->Insert(key, ByteBuffer{});
}

std::unique_ptr<StorageIterator> Transaction::Scan(const Bound& lower,
                                                     const Bound& upper) {
    assert(!committed_);
    auto local_iter = std::make_unique<TxnLocalIterator>(
        IteratorPasskey{},
        local_storage_,
        lower,
        upper);

    auto strong_storage = storage_.lock();
    assert(strong_storage);
    auto storage_iter = strong_storage->Scan(lower, upper);

    auto merged_iter = TwoMergeIterator::Create(std::move(local_iter), std::move(storage_iter));

    return std::make_unique<TxnIterator>(IteratorPasskey{}, shared_from_this(), std::move(merged_iter));
}

bool Transaction::Commit() {
    assert(!committed_);
    auto strong_storage = storage_.lock();
    if (!strong_storage) {
        return false; // Storage has been dropped
    }
    
    // Implement serializable check if this is a serializable transaction
    if (serializable_) {
        // Get current read/write sets as separate key hashes for efficient conflict checking
        std::unordered_set<uint32_t> read_key_hashes;
        std::unordered_set<uint32_t> write_key_hashes;
        
        // Add read set keys
        for (const auto& key : read_set_) {
            // Use simple hash function for keys
            read_key_hashes.insert(std::hash<ByteBuffer>{}(key));
        }
        
        // Add write set keys
        for (const auto& [key, _] : write_set_) {
            write_key_hashes.insert(std::hash<ByteBuffer>{}(key));
        }
        
        // Check for conflicts with transactions that committed after our start time
        auto mvcc = strong_storage->GetMvcc();
        if (mvcc && !mvcc->CheckSerializableNoConflicts(start_ts_, read_key_hashes, write_key_hashes)) {
            // Serialization conflict detected
            return false;
        }
    }
    
    // No conflicts, proceed with commit
    // Get the commit timestamp first
    auto mvcc = strong_storage->GetMvcc();
    uint64_t commit_ts = mvcc ? mvcc->LatestCommitTs() + 1 : 1;
    
    // Update the MVCC commit timestamp
    if (mvcc) {
        mvcc->UpdateCommitTs(commit_ts);
    }
    
    // Create batch with versioned keys (key + timestamp)
    std::vector<std::pair<ByteBuffer, ByteBuffer>> batch;
    for (const auto& [key, value] : write_set_) {
        ByteBuffer versioned_key = EncodeKeyWithTs(key, commit_ts);
        batch.emplace_back(std::move(versioned_key), value);
    }
    
    // Write batch to storage

    if (!strong_storage->WriteBatch(batch, nullptr)) {  // Don't need commit_ts from WriteBatch since we already have it
        std::cout << "DEBUG Transaction::Commit: Failed to write batch" << std::endl;
        return false;
    }

    
    // If this was a serializable transaction, record it in the committed transactions map
    if (serializable_) {
        auto mvcc = strong_storage->GetMvcc();
        if (mvcc) {
            // Store the committed transaction data in MVCC
            CommittedTxnData txn_data;
            
            // Add read set key hashes
            for (const auto& key : read_set_) {
                txn_data.read_set.insert(std::hash<ByteBuffer>{}(key));
            }
            
            // Add write set key hashes
            for (const auto& [key, _] : write_set_) {
                txn_data.write_set.insert(std::hash<ByteBuffer>{}(key));
            }
            txn_data.read_ts = start_ts_;
            txn_data.commit_ts = commit_ts;
            
            // Have MVCC record this transaction
            mvcc->AddCommittedTransaction(commit_ts, std::move(txn_data));
        }
    }
    
    committed_ = true;
    return true;
}

// --- TxnLocalIterator ---

TxnLocalIterator::TxnLocalIterator(Transaction::IteratorPasskey,
                                   std::shared_ptr<SkipMap> storage,
                                   const Bound& lower, const Bound& upper)
    : storage_(std::move(storage)),
      range_iter_(storage_->Range(lower, upper)) {}

bool TxnLocalIterator::IsValid() const noexcept {
    return range_iter_->Valid();
}

void TxnLocalIterator::Next() noexcept {
    range_iter_->Next();
}

ByteBuffer TxnLocalIterator::Key() const noexcept {
    return range_iter_->Key();
}

const ByteBuffer& TxnLocalIterator::Value() const noexcept {
    return range_iter_->Value();
}


// --- TxnIterator ---

TxnIterator::TxnIterator(Transaction::IteratorPasskey,
                         std::shared_ptr<Transaction> txn,
                         std::unique_ptr<StorageIterator> iter)
    : txn_(std::move(txn)), iter_(std::move(iter)) {
    // Skip deleted entries
    while (iter_->IsValid() && iter_->Value().Empty()) {
        iter_->Next();
    }
}

bool TxnIterator::IsValid() const noexcept {
    return iter_->IsValid();
}

void TxnIterator::Next() noexcept {
    iter_->Next();
    // Skip deleted entries
    while (iter_->IsValid() && iter_->Value().Empty()) {
        iter_->Next();
    }
}

ByteBuffer TxnIterator::Key() const noexcept {
    return iter_->Key();
}

const ByteBuffer& TxnIterator::Value() const noexcept {
    return iter_->Value();
}

