#pragma once

#include "lsm_storage.hpp"
#include "mvcc_mem_table.hpp"
#include "key_ts.hpp"
#include "mvcc_lsm_iterator.hpp"
#include <memory>
#include <optional>
#include <vector>
#include <map>
#include <shared_mutex>
#include <atomic>

namespace util {

// Forward declarations
class LsmMvccInner;

/**
 * @brief MVCC-aware LSM storage engine.
 * 
 * This class extends the standard LsmStorage with Multi-Version Concurrency Control
 * functionality, providing snapshot isolation and serializable isolation for transactions.
 * It uses versioned keys (key + timestamp) for all operations to ensure correct MVCC semantics.
 */
class MvccLsmStorage {
public:
    /**
     * @brief Creates a new MVCC-aware LSM storage engine.
     * 
     * @param options Configuration options for the LSM storage
     * @param path Path to the storage directory
     * @return std::unique_ptr<MvccLsmStorage> A new MVCC LSM storage instance
     */
    static std::unique_ptr<MvccLsmStorage> Create(
        const LsmStorageOptions& options, 
        const std::filesystem::path& path);
        
    /**
     * @brief Creates a new MVCC-aware LSM storage engine wrapper around an existing LsmStorageInner.
     * 
     * @param inner The LsmStorageInner to wrap
     * @return std::shared_ptr<MvccLsmStorage> A new MVCC LSM storage wrapper
     */
    static std::shared_ptr<MvccLsmStorage> CreateShared(
        std::shared_ptr<LsmStorageInner> inner);

    /**
     * @brief Puts a key-value pair into the storage with the specified timestamp.
     * 
     * @param key The key to insert
     * @param value The value to insert
     * @param ts The timestamp for this operation
     * @return true if successful, false otherwise
     */
    bool PutWithTs(const ByteBuffer& key, const ByteBuffer& value, uint64_t ts);

    /**
     * @brief Gets the value associated with the key as of the specified timestamp.
     * 
     * @param key The key to look up
     * @param read_ts The timestamp to read as of (only versions <= read_ts are visible)
     * @return std::optional<ByteBuffer> The value if found and visible, nullopt otherwise
     */
    std::optional<ByteBuffer> GetWithTs(const ByteBuffer& key, uint64_t read_ts);

    /**
     * @brief Deletes a key with the specified timestamp (inserts a tombstone).
     * 
     * @param key The key to delete
     * @param ts The timestamp for this operation
     * @return true if successful, false otherwise
     */
    bool DeleteWithTs(const ByteBuffer& key, uint64_t ts);

    /**
     * @brief Creates a new iterator over the storage as of the specified timestamp.
     * 
     * @param lower_bound Lower bound for iteration
     * @param upper_bound Upper bound for iteration
     * @param read_ts The timestamp to read as of (only versions <= read_ts are visible)
     * @return MvccLsmIterator A new iterator over the storage
     */
    MvccLsmIterator ScanWithTs(
        const Bound& lower_bound, 
        const Bound& upper_bound, 
        uint64_t read_ts);

    /**
     * @brief Puts a key-value pair into the storage with the current timestamp.
     * 
     * @param key The key to insert
     * @param value The value to insert
     * @return true if successful, false otherwise
     */
    bool Put(const ByteBuffer& key, const ByteBuffer& value);

    /**
     * @brief Gets the latest value associated with the key.
     * 
     * @param key The key to look up
     * @return std::optional<ByteBuffer> The value if found, nullopt otherwise
     */
    std::optional<ByteBuffer> Get(const ByteBuffer& key);

    /**
     * @brief Deletes a key with the current timestamp (inserts a tombstone).
     * 
     * @param key The key to delete
     * @return true if successful, false otherwise
     */
    bool Delete(const ByteBuffer& key);

    /**
     * @brief Creates a new iterator over the latest state of the storage.
     * 
     * @param lower_bound Lower bound for iteration
     * @param upper_bound Upper bound for iteration
     * @return MvccLsmIterator A new iterator over the storage
     */
    MvccLsmIterator Scan(const Bound& lower_bound, const Bound& upper_bound);

    /**
     * @brief Forces a full compaction of the storage.
     */
    void ForceFullCompaction();

    /**
     * @brief Flushes the active memtable to disk.
     */
    void Flush();

    /**
     * @brief Gets the next timestamp from the global counter.
     * 
     * This method is used by transactions to get a unique, monotonically
     * increasing timestamp for read and write operations.
     * 
     * @return uint64_t The next timestamp
     */
    uint64_t GetNextTimestamp() noexcept;

    /**
     * @brief Gets the underlying LsmStorageInner for transaction creation.
     * 
     * This method is used internally by the MVCC system to create transactions
     * that can access the underlying storage with timestamp awareness.
     * 
     * @return std::shared_ptr<LsmStorageInner> Shared pointer to the underlying storage
     */
    std::shared_ptr<LsmStorageInner> GetInner() const noexcept;

    /**
     * @brief Gets the MVCC inner for conflict checking in serializable transactions.
     * 
     * @return std::shared_ptr<LsmMvccInner> Shared pointer to the MVCC inner, or nullptr if not available
     */
    std::shared_ptr<LsmMvccInner> GetMvccInner() const noexcept;
    
    /**
     * @brief Performs garbage collection to clean up obsolete versions.
     * 
     * This method removes versions of keys that are older than the watermark
     * and are no longer visible to any active transactions. It helps reclaim
     * memory and storage space by removing unnecessary version history.
     * 
     * @return size_t Number of obsolete versions removed
     */
    size_t GarbageCollect();

private:
    // Constructor is private, but LsmMvccInner can access it
    friend class LsmMvccInner;
    MvccLsmStorage(std::shared_ptr<LsmStorageInner> inner);
    
    // Helper methods for garbage collection
    
    /**
     * @brief Perform version cleanup in memtables based on watermark
     * 
     * @param watermark Versions older than this timestamp can be removed
     * @return size_t Number of obsolete versions removed from memtables
     */
    size_t PerformMemtableVersionCleanup(uint64_t watermark);
    
    /**
     * @brief Clean up old committed transaction records below the watermark
     * 
     * @param watermark Transactions older than this timestamp can be removed
     * @return size_t Number of old transaction records cleaned up
     */
    size_t CleanupOldCommittedTransactions(uint64_t watermark);

    /**
     * @brief Scan and identify obsolete versions in memtables based on watermark
     * 
     * This method scans the storage to identify versions that are older than
     * the watermark and can be safely removed during garbage collection.
     * 
     * @param watermark Watermark timestamp for version cleanup threshold
     * @return Number of obsolete versions identified
     */
    size_t ScanAndIdentifyObsoleteVersions(uint64_t watermark);
    
    /**
     * @brief Perform watermark-aware compaction to remove obsolete versions from SSTables
     * 
     * This method triggers compaction with enhanced logic to filter out versions
     * that are older than the watermark during the compaction process.
     * 
     * @param watermark Watermark timestamp for version filtering threshold
     * @return Number of obsolete versions removed during compaction
     */
    size_t PerformWatermarkAwareCompaction(uint64_t watermark);

    // Wrapper around the standard LsmStorageInner
    std::shared_ptr<LsmStorageInner> inner_;

    // Next timestamp to use for operations
    std::atomic<uint64_t> next_ts_{1};

    // Mutex for synchronizing operations
    mutable std::shared_mutex mutex_;

    // Active memtable converted to MVCC-aware version
    std::shared_ptr<MvccMemTable> active_memtable_;

    // Immutable memtables converted to MVCC-aware versions
    std::vector<std::shared_ptr<MvccMemTable>> imm_memtables_;
};

} // namespace util
