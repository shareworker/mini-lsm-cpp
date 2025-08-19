#pragma once

#include "mvcc_lsm_storage.hpp"
#include "storage_iterator.hpp"
#include "key_ts.hpp"
#include "bound.hpp"
#include "mvcc_txn.hpp"
#include <memory>
#include <optional>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <mutex>
#include <atomic>

/**
 * @brief Iterator over transaction-local data for MVCC scans.
 * 
 * This iterator provides a view over a merged dataset that includes both
 * the transaction's local writes and data from the underlying storage,
 * with local writes taking precedence.
 */
class MvccTransactionIterator : public StorageIterator {
public:
    /**
     * @brief Constructs an iterator over the given data map.
     * 
     * @param data Map of key-value pairs to iterate over
     */
    explicit MvccTransactionIterator(std::map<ByteBuffer, ByteBuffer>&& data)
        : data_(std::move(data)), 
          current_(data_.begin()),
          valid_(!data_.empty()) {}
    
    /**
     * @brief Checks if the iterator is valid (pointing to a valid entry).
     * 
     * @return true if valid, false otherwise
     */
    bool IsValid() const noexcept override { return valid_; }
    
    /**
     * @brief Gets the key at the current position.
     * 
     * @return ByteBuffer reference to the current key
     */
    ByteBuffer Key() const noexcept override { 
        if (!valid_) return empty_buffer_;
        
        // If we have a versioned key (from storage), strip the timestamp
        ByteBuffer key = current_->first;
        if (key.Size() > sizeof(uint64_t)) {
            return ByteBuffer(key.Data(), key.Size() - sizeof(uint64_t));
        }
        return key;
    }
    
    /**
     * @brief Gets the value at the current position.
     * 
     * @return ByteBuffer reference to the current value
     */
    const ByteBuffer& Value() const noexcept override { 
        if (!valid_) return empty_buffer_;
        return current_->second; 
    }
    
    /**
     * @brief Advances the iterator to the next position.
     */
    void Next() noexcept override {
        if (!valid_) return;
        
        ++current_;
        valid_ = (current_ != data_.end());
    }
    
    /**
     * @brief Seeks to the first key that is >= the given key.
     * 
     * @param key The key to seek to
     */
    void Seek(const ByteBuffer& key) noexcept {
        current_ = data_.lower_bound(key);
        valid_ = (current_ != end_);
    }
    
private:
    std::map<ByteBuffer, ByteBuffer> data_; ///< Sorted map of keys and values
    std::map<ByteBuffer, ByteBuffer>::iterator current_; ///< Current position in the map
    std::map<ByteBuffer, ByteBuffer>::iterator end_; ///< End position in the map
    bool valid_; ///< Whether the iterator is valid
    ByteBuffer empty_buffer_; ///< Empty buffer for invalid iterator
};


/**
 * @brief Isolation levels for MVCC transactions.
 */
enum class IsolationLevel {
    kSnapshotIsolation,   ///< Snapshot isolation (no write-write conflicts)
    kSerializable         ///< Serializable isolation (no write-write, no read-write conflicts)
};

/**
 * @brief MVCC transaction providing snapshot isolation or serializable isolation.
 * 
 * This class implements transaction semantics on top of the MvccLsmStorage,
 * ensuring that transactions see a consistent snapshot of the database and
 * can be committed atomically with appropriate isolation guarantees.
 */
class MvccTransaction {
public:
    /**
     * @brief Creates a new transaction with the specified isolation level.
     * 
     * @param storage The underlying MVCC storage engine
     * @param isolation_level The isolation level for the transaction
     * @param read_ts Optional read timestamp; if not provided, gets next timestamp from storage
     * @return std::unique_ptr<MvccTransaction> A new transaction
     */
    static std::unique_ptr<MvccTransaction> Begin(
        std::shared_ptr<MvccLsmStorage> storage,
        IsolationLevel isolation_level = IsolationLevel::kSnapshotIsolation,
        std::optional<uint64_t> read_ts = std::nullopt);

    /**
     * @brief Puts a key-value pair into the transaction.
     * 
     * The change is not visible to other transactions until this transaction
     * is committed.
     * 
     * @param key The key to insert
     * @param value The value to insert
     * @return true if successful, false otherwise
     */
    bool Put(const ByteBuffer& key, const ByteBuffer& value);

    /**
     * @brief Gets the value associated with the key as of the transaction's snapshot.
     * 
     * @param key The key to look up
     * @return ByteBuffer The value if found and visible, empty ByteBuffer otherwise
     */
    ByteBuffer Get(const ByteBuffer& key);

    /**
     * @brief Deletes a key from the transaction.
     * 
     * The deletion is not visible to other transactions until this transaction
     * is committed.
     * 
     * @param key The key to delete
     * @return true if successful, false otherwise
     */
    bool Delete(const ByteBuffer& key);

    /**
     * @brief Creates an iterator over the transaction's snapshot.
     * 
     * @param lower_bound Lower bound for iteration
     * @param upper_bound Upper bound for iteration
     * @return std::unique_ptr<StorageIterator> An iterator over the transaction's snapshot
     */
    std::unique_ptr<StorageIterator> Scan(
        const Bound& lower_bound = Bound::Unbounded(),
        const Bound& upper_bound = Bound::Unbounded());

    /**
     * @brief Commits the transaction.
     * 
     * This makes all changes made in the transaction visible to other transactions.
     * For serializable transactions, this will fail if there are serialization conflicts.
     * 
     * @return true if commit succeeded, false if it failed due to conflicts
     */
    bool Commit();

    /**
     * @brief Aborts the transaction.
     * 
     * This discards all changes made in the transaction.
     */
    void Abort();

    /**
     * @brief Gets the transaction's read timestamp.
     * 
     * @return uint64_t The read timestamp
     */
    uint64_t ReadTs() const { return read_ts_; }

    /**
     * @brief Gets the transaction's isolation level.
     * 
     * @return IsolationLevel The isolation level
     */
    IsolationLevel GetIsolationLevel() const { return isolation_level_; }

private:
    MvccTransaction(
        std::shared_ptr<MvccLsmStorage> storage,
        IsolationLevel isolation_level,
        uint64_t read_ts);

    // Underlying storage engine
    std::shared_ptr<MvccLsmStorage> storage_;
    
    // Isolation level for this transaction
    IsolationLevel isolation_level_;
    
    // Read timestamp (snapshot) for this transaction
    uint64_t read_ts_;
    
    // Write timestamp to be assigned at commit time
    uint64_t write_ts_{0};
    
    // Transaction state
    enum class State {
        kActive,
        kCommitted,
        kAborted
    };
    State state_{State::kActive};
    
    // Pending writes (key -> value)
    std::unordered_map<ByteBuffer, ByteBuffer> write_set_;
    
    // Keys read during the transaction (for serializable isolation)
    std::unordered_set<ByteBuffer> read_set_;
    
    // Mutex for synchronizing transaction operations
    mutable std::mutex mutex_;
};

