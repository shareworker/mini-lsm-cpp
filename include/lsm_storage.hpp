#pragma once

#include <atomic>
#include <filesystem>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "byte_buffer.hpp"
#include "bound.hpp"
#include "mem_table.hpp"
#include "block_cache.hpp"
#include "storage_iterator.hpp"
#include "wal.hpp"
#include "wal_segment.hpp"
#include "manifest.hpp"
#include "compaction.hpp"

using Path = std::filesystem::path;

namespace util {

// Forward declarations
class SsTable;
class Manifest;
class CompactionController;
class LsmMvccInner;

/**
 * @brief Configuration options for LSM storage
 */
struct LsmStorageOptions {
    // Block size in bytes
    size_t block_size = 4096;
    
    // SST size in bytes, also the approximate memtable capacity limit
    size_t target_sst_size = 2 * 1024 * 1024; // 2MB default
    
    // Maximum number of memtables in memory
    size_t num_memtable_limit = 5;
    
    // Compaction strategy
    CompactionStrategy compaction_strategy = CompactionStrategy::kLeveled;
    
    // Whether to enable write-ahead logging
    bool enable_wal = true;
    
    // Whether to enable serializable isolation
    bool serializable = false;
    
    // Whether to sync WAL on every write
    bool sync_on_write = false;
    
    // Tiered compaction options
    size_t tiered_num_tiers = 6;
    size_t tiered_max_size_amplification_percent = 200;
    size_t tiered_size_ratio = 50;
    size_t tiered_min_merge_width = 2;
    std::optional<size_t> tiered_max_merge_width = std::nullopt;
    
    // Simple leveled compaction options
    size_t simple_leveled_size_ratio_percent = 50;
    size_t simple_leveled_level0_file_num_compaction_trigger = 4;
    size_t simple_leveled_max_levels = 7;
};

/**
 * @brief Filter for compaction operations
 */
class CompactionFilter {
public:
    enum class Type {
        kPrefix  // Filter by key prefix
    };
    
    explicit CompactionFilter(Type type, const ByteBuffer& value)
        : type_(type), value_(value) {}
    
    Type GetType() const { return type_; }
    const ByteBuffer& GetValue() const { return value_; }
    
private:
    Type type_;
    ByteBuffer value_;
};

class LsmStorageState {
public:
    // Current active memtable
    std::shared_ptr<MemTable> memtable;
    
    // Immutable memtables, from newest to oldest
    std::vector<std::shared_ptr<MemTable>> imm_memtables;
    
    // L0 SSTs, from newest to oldest
    std::vector<size_t> l0_sstables;
    
    // Levels for leveled compaction or tiers for tiered compaction
    // Each pair contains (level_number, vector of SST IDs)
    std::vector<std::pair<size_t, std::vector<size_t>>> levels;
    
    // Map of SST ID to SST object
    std::unordered_map<size_t, std::shared_ptr<SsTable>> sstables;
    
    // Constructor
    explicit LsmStorageState(size_t initial_memtable_id)
        : memtable(MemTable::Create(initial_memtable_id)) {}

    LsmStorageState(const LsmStorageState& other) = default;

    std::unique_ptr<LsmStorageState> Clone() const {
        return std::make_unique<LsmStorageState>(*this);
    }
};
    


/**
 * @brief Core implementation of LSM tree storage
 * 
 * This class manages the state of the LSM tree, including memtables,
 * SSTs, and compaction operations.
 */
class LsmStorageInner {
public:
    /**
     * @brief Creates a new LSM storage instance
     * 
     * @param path Directory path for the storage
     * @param options Configuration options
     * @return std::unique_ptr<LsmStorageInner> New storage instance
     */
    static std::unique_ptr<LsmStorageInner> Create(
        const std::filesystem::path& path,
        const LsmStorageOptions& options = {});
    
    /**
     * @brief Opens an existing LSM storage
     * 
     * @param path Directory path for the storage
     * @param options Configuration options
     * @return std::unique_ptr<LsmStorageInner> Opened storage instance
     */
    static std::unique_ptr<LsmStorageInner> Open(
        const std::filesystem::path& path,
        const LsmStorageOptions& options = {});
    
    /**
     * @brief Destructor ensures proper cleanup
     */
    ~LsmStorageInner();
    
    /**
     * @brief Prevent copy construction and assignment
     */
    LsmStorageInner(const LsmStorageInner&) = delete;
    LsmStorageInner& operator=(const LsmStorageInner&) = delete;
    
    /**
     * @brief Prevent move construction and assignment
     */
    LsmStorageInner(LsmStorageInner&&) = delete;
    LsmStorageInner& operator=(LsmStorageInner&&) = delete;
    
    /**
     * @brief Puts a key-value pair into the storage
     * 
     * @param key The key to insert
     * @param value The value to associate with the key
     * @return true if successful
     */
    bool Put(const ByteBuffer& key, const ByteBuffer& value);

    /**
     * @brief Returns an iterator over keys in the half-open range [lower, upper).
     *        Semantics follow Rust implementation: lower bound may be Included / Excluded / Unbounded;
     *        upper bound likewise. Tombstones (empty value) are filtered out by the returned
     *        iterator wrapper.
     */
    std::unique_ptr<StorageIterator> Scan(const Bound& lower,
                                                       const Bound& upper) const noexcept;
    
    /**
     * @brief Gets the value associated with a key
     * 
     * @param key The key to look up
     * @return std::optional<ByteBuffer> The value if found
     */
    std::optional<ByteBuffer> Get(const ByteBuffer& key) const;

    /**
     * @brief Get a value for a key considering MVCC timestamp visibility
     *
     * Only returns the latest version of the key visible as of the read_ts timestamp
     * 
     * @param key The key to look up
     * @param read_ts The timestamp to use for visibility (transaction start timestamp)
     * @return std::optional<ByteBuffer> The value, if found and visible
     */
    std::optional<ByteBuffer> GetWithTs(const ByteBuffer& key, uint64_t read_ts) const;
    
    /**
     * @brief Returns an iterator over keys in the half-open range [lower, upper),
     *        considering MVCC timestamp visibility.
     *
     * Only returns versions of keys that are visible as of the read_ts timestamp.
     * Semantics follow Rust implementation with timestamp filtering added.
     * 
     * @param lower The lower bound of the scan range
     * @param upper The upper bound of the scan range
     * @param read_ts The timestamp to use for visibility (transaction start timestamp)
     * @return std::unique_ptr<StorageIterator> Iterator over the visible key-value pairs
     */
    std::unique_ptr<StorageIterator> ScanWithTs(const Bound& lower,
                                                            const Bound& upper,
                                                            uint64_t read_ts) const noexcept;
    
    /**
     * @brief Deletes a key-value pair from the storage
     * 
     * @param key The key to delete
     * @return true if successful
     */
    bool Delete(const ByteBuffer& key);
    
    /**
     * @brief Returns a pointer to the MVCC controller if one exists
     * 
     * @return Pointer to LsmMvccInner or nullptr if MVCC is not enabled
     */
    std::shared_ptr<LsmMvccInner> GetMvcc() const noexcept;
    
    /**
     * @brief Set the MVCC controller for this storage instance
     * 
     * @param mvcc Shared pointer to the MVCC controller
     */
    void SetMvcc(std::shared_ptr<LsmMvccInner> mvcc) noexcept;

    /**
     * @brief Write multiple key-value pairs atomically
     * 
     * @param batch Batch of key-value pairs
     * @param commit_ts Optional pointer to store the commit timestamp
     * @return true on success
     */
    bool WriteBatch(const std::vector<std::pair<ByteBuffer, ByteBuffer>>& batch, uint64_t* commit_ts = nullptr);
    
    /**
     * @brief Adds a compaction filter
     * 
     * @param filter The filter to add
     */
    void AddCompactionFilter(const CompactionFilter& filter);
    
    /**
     * @brief Triggers a flush of the current memtable
     * 
     * @return true if successful
     */
    bool Flush();
    
    /**
     * @brief Triggers a compaction operation
     * 
     * @return true if successful
     */
    bool Compact();

    /**
     * @brief Force a full compaction (merge L0 + L1) when no automatic compaction
     *        is enabled. This is primarily used for testing or shutdown cleanup.
     *
     * The implementation mirrors the Rust `force_full_compaction` logic:
     *   1. Validate that the compaction strategy is `kNoCompaction`.
     *   2. Take a snapshot of the current state.
     *   3. Merge all L0 SSTs with the first level (L1) SSTs into new L1 SSTs.
     *   4. Atomically swap the storage state and persist a MANIFEST record.
     *   5. Remove obsolete SST files from disk.
     *
     * NOTE: The actual SST merging is not yet implemented; for now this method
     *       provides the state-bookkeeping shell so that future SST merge logic
     *       can be plugged in without touching higher-level code.
     */
    bool ForceFullCompaction();

    // Force flush the earliest-created immutable memtable to disk.
    // A minimal stub — full SSTable flush will be implemented once
    // SsTableBuilder and Manifest are finished.
    bool ForceFlushNextImmMemtable();
    // Check imm_memtable count and flush if exceeding limit
    bool TriggerFlush();

    bool SyncDir();
    /**
     * @brief Sync any pending WAL writes to disk
     * 
     * @return true if successful
     */
    bool Sync();
    
    void TryFreeze(size_t estimated_size);
    void ForceFreezeMemtable();
    // Legacy WAL path method
    Path PathOfWal(size_t id) const;
    
    // New WAL segment directory path
    Path WalSegmentDir() const;
    
    // WAL segment name (used for segmentation)
    std::string WalSegmentName() const;
    // Static helper to map an SST id to its on-disk path within a directory.
    static Path PathOfSstStatic(const Path& dir, size_t id);
    // Instance helper using this storage's base directory.
    Path PathOfSst(size_t id) const;
    bool FreezeMemtableWithMemtable(std::unique_ptr<MemTable> memtable);
    std::shared_ptr<LsmStorageOptions> GetOptions() const { return options_; }
    
    /**
     * @brief Get the current active memtable for MVCC wrapping
     * 
     * @return std::shared_ptr<MemTable> The active memtable
     */
    std::shared_ptr<MemTable> GetActiveMemtable() const {
        std::shared_lock<std::shared_mutex> lock(*state_mutex_);
        return state_->memtable;
    }
    
    /**
     * @brief Get the immutable memtables for MVCC wrapping
     * 
     * @return std::vector<std::shared_ptr<MemTable>> The immutable memtables
     */
    std::vector<std::shared_ptr<MemTable>> GetImmutableMemtables() const {
        std::shared_lock<std::shared_mutex> lock(*state_mutex_);
        return state_->imm_memtables;
    }

    // ---------------------------------------------------------------------
    // Compaction helpers
    // ----------------

private:
    /**
     * @brief Check if a key passes all registered compaction filters.
     *        Returns true if the key should be kept, false if it must be dropped.
     */
    bool KeyPassesFilters(const ByteBuffer& key) const;

public:
    /**
     * @brief Stream entries from an iterator into one or more new SSTables.
     *
     * This mirrors the Rust helper `compact_generate_sst_from_iter`. It consumes
     * the iterator, splitting output SSTs according to `options_->target_sst_size`.
     * If `compact_to_bottom_level` is true, tombstone (empty value) entries are
     * filtered out (they have no effect on bottommost level).
     *
     * @param iter                     Forward iterator over key/value pairs.
     * @param compact_to_bottom_level  Whether the compaction target is the
     *                                 bottom-most level.
     * @return Vector of newly built SSTable objects.
     */
    std::vector<std::shared_ptr<SsTable>> GenerateSstFromIter(
        StorageIterator& iter,
        bool compact_to_bottom_level);

    
private:
    bool Compact(const SimpleLeveledCompactionTask& task);

    /**
     * @brief Build an SSTable from the provided immutable memtable.
     *
     * This helper is used during flush and compaction to serialize a memtable
     * into an on-disk SST file. It mirrors the Rust helper `build_sst_from_mem`.
     */
    std::shared_ptr<SsTable> BuildSstFromMemtable(
        std::shared_ptr<MemTable> mem);

    /**
     * @brief Private constructor for factory methods
     * 
     * @param path Directory path for the storage
     * @param options Configuration options
     */
    LsmStorageInner(const Path& path, const LsmStorageOptions& options);
    
    // Thread-safe state management
    std::shared_ptr<std::shared_mutex> state_mutex_;
    std::shared_ptr<LsmStorageState> state_;
    std::mutex state_lock_mutex_;
    
    // Storage path
    Path path_;
    
    // Block cache for SST data
    std::shared_ptr<BlockCache> block_cache_;
    
    // Atomic counter for SST IDs
    std::atomic<size_t> next_sst_id_;
    
    // Storage options
    std::shared_ptr<LsmStorageOptions> options_;
    
    // Compaction controller
    std::unique_ptr<CompactionController> compaction_controller_;
    
    // Manifest for persistent metadata
    std::unique_ptr<Manifest> manifest_;
    
    // Compaction filters
    std::vector<CompactionFilter> compaction_filters_;
    mutable std::mutex compaction_filters_mutex_;
    
    // Write-ahead log
    // Legacy WAL (to be deprecated)
    std::unique_ptr<Wal> wal_;
    
    // Segmented WAL (new implementation)
    std::shared_ptr<WalSegment> wal_segment_;
    
    // MVCC controller (optional, for transaction support)
    std::shared_ptr<LsmMvccInner> mvcc_;
};



} // namespace util
