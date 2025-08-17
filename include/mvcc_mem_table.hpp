#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <optional>
#include <utility>
#include <filesystem>

#include "byte_buffer.hpp"
#include "mvcc_skiplist.hpp"
#include "wal.hpp"
#include "mvcc_wal.hpp"
#include "storage_iterator.hpp"
#include "bound.hpp"
#include "mem_table.hpp"

namespace util {

// Forward declarations
class MvccMemTableIterator;

/**
 * @brief MVCC-aware in-memory table implementation using a versioned skip list.
 * 
 * MvccMemTable extends the standard MemTable with timestamp-aware operations for
 * Multi-Version Concurrency Control (MVCC). Each key is stored with its timestamp
 * to support reading consistent snapshots of data at specific points in time.
 */
class MvccMemTable {
public:
    /**
     * @brief Creates a new MvccMemTable with the given ID.
     * 
     * @param id Unique identifier for this MvccMemTable
     * @return std::unique_ptr<MvccMemTable> A new MvccMemTable instance
     */
    static std::unique_ptr<MvccMemTable> Create(size_t id);

    /**
     * @brief Creates a new MvccMemTable with the given ID and WAL.
     * 
     * @param id Unique identifier for this MvccMemTable
     * @param path Path for the WAL
     * @return std::unique_ptr<MvccMemTable> A new MvccMemTable instance with WAL
     */
    static std::unique_ptr<MvccMemTable> CreateWithWal(
        size_t id, 
        std::filesystem::path path);
        
    /**
     * @brief Recovers a MvccMemTable from an existing WAL file.
     * 
     * @param id Unique identifier for this MvccMemTable
     * @param path Path to the WAL file
     * @return std::unique_ptr<MvccMemTable> Recovered MvccMemTable or nullptr if recovery failed
     */
    static std::unique_ptr<MvccMemTable> RecoverFromWal(
        size_t id,
        const std::filesystem::path& path);

    /**
     * @brief Creates a MvccMemTable from an existing skiplist.
     * 
     * @param id Unique identifier for this MvccMemTable
     * @param skiplist The existing skiplist to use
     * @return std::unique_ptr<MvccMemTable> A new MvccMemTable using the provided skiplist
     */
    static std::unique_ptr<MvccMemTable> CreateFromSkipList(
        size_t id, 
        std::shared_ptr<MvccSkipList> skiplist);

    /**
     * @brief Inserts a key-value pair with the specified timestamp.
     * 
     * @param key The key to insert
     * @param value The value to insert
     * @param ts The timestamp for this operation
     * @return true if insertion succeeded, false otherwise
     */
    bool Put(const ByteBuffer& key, const ByteBuffer& value, uint64_t ts);

    /**
     * @brief Retrieves the value associated with the key as of the given timestamp.
     * 
     * @param key The key to look up
     * @param read_ts The timestamp to read as of (only versions <= read_ts are visible)
     * @return std::optional<ByteBuffer> The value if found and visible, nullopt otherwise
     */
    std::optional<ByteBuffer> GetWithTs(const ByteBuffer& key, uint64_t read_ts);
    
    /**
     * @brief Retrieves the value and its timestamp for a key as of the given timestamp.
     * 
     * @param key The key to look up
     * @param read_ts The timestamp to read as of (only versions <= read_ts are visible)
     * @return std::optional<std::pair<ByteBuffer, uint64_t>> The value and its timestamp if found, nullopt otherwise
     */
    std::optional<std::pair<ByteBuffer, uint64_t>> GetWithTsInfo(
        const ByteBuffer& key, uint64_t read_ts);

    /**
     * @brief Removes a key with the specified timestamp (inserts a tombstone).
     * 
     * @param key The key to remove
     * @param ts The timestamp for this operation
     * @return true if removal succeeded, false otherwise
     */
    bool Remove(const ByteBuffer& key, uint64_t ts);

    /**
     * @brief Gets the approximate size in bytes of all entries in this memtable.
     * 
     * @return size_t The approximate size in bytes
     */
    size_t ApproximateSize() const;

    /**
     * @brief Gets the unique ID of this memtable.
     * 
     * @return size_t The ID
     */
    size_t Id() const;

    /**
     * @brief Checks if the memtable is empty.
     * 
     * @return true if empty, false otherwise
     */
    bool IsEmpty() const;

    /**
     * @brief Sets the WAL segment for this memtable.
     * 
     * @param wal_segment The WAL segment to use
     */
    void SetWalSegment(const std::shared_ptr<WalSegment>& wal_segment);

    /**
     * @brief Syncs the WAL to disk for durability.
     * 
     * @return true if sync succeeded, false otherwise
     */
    bool SyncWal();

    /**
     * @brief Creates a new iterator over this memtable with the given bounds.
     * 
     * @param lower_bound Lower bound for iteration
     * @param upper_bound Upper bound for iteration
     * @param read_ts Timestamp to read as of (only versions <= read_ts are visible)
     * @return MvccMemTableIterator Iterator over the memtable contents
     */
    MvccMemTableIterator NewIterator(const Bound& lower_bound,
                                    const Bound& upper_bound,
                                    uint64_t read_ts);

    /**
     * @brief Gets the underlying skiplist.
     * 
     * @return std::shared_ptr<MvccSkipList> The underlying skiplist
     */
    std::shared_ptr<MvccSkipList> GetSkipList() const {
        return skiplist_;
    }
    
    /**
     * @brief Creates a MvccMemTable wrapper over an existing (non-MVCC) MemTable.
     * 
     * This is primarily used for MVCC reads over immutable memtables. The created
     * MvccMemTable provides a timestamp-aware view of the underlying memtable.
     * 
     * @param memtable The existing memtable to wrap
     * @return std::unique_ptr<MvccMemTable> A new MvccMemTable wrapping the input memtable
     */
    static std::unique_ptr<MvccMemTable> CreateFromMemTable(
        const std::shared_ptr<MemTable>& memtable);
        
    /**
     * @brief Constructs a MvccMemTable from an existing (non-MVCC) MemTable.
     * 
     * @param memtable The existing memtable to wrap
     */
    explicit MvccMemTable(const std::shared_ptr<MemTable>& memtable);

private:
    MvccMemTable(size_t id);
    MvccMemTable(size_t id, std::unique_ptr<Wal> wal);
    MvccMemTable(size_t id, std::unique_ptr<MvccWal> mvcc_wal);

    size_t id_;
    std::shared_ptr<MvccSkipList> skiplist_;
    std::unique_ptr<MvccWal> mvcc_wal_;  // MVCC-aware WAL for timestamp persistence
    std::unique_ptr<Wal> wal_;           // Legacy WAL for backward compatibility
    std::shared_ptr<WalSegment> wal_segment_;
    std::atomic<size_t> approximate_size_{0};
};

/**
 * @brief Iterator over a snapshot of a MvccMemTable with timestamp filtering.
 */
class MvccMemTableIterator : public StorageIterator {
public:
    MvccMemTableIterator(const MvccMemTableIterator&) = delete;
    MvccMemTableIterator& operator=(const MvccMemTableIterator&) = delete;
    MvccMemTableIterator(MvccMemTableIterator&&) noexcept = default;
    MvccMemTableIterator& operator=(MvccMemTableIterator&&) noexcept = default;
    ~MvccMemTableIterator() override = default;
    
    bool IsValid() const noexcept override { 
        return index_ < items_.size(); 
    }
    
    void Next() noexcept override {
        if (index_ < items_.size()) {
            ++index_;
        }
    }
    
    ByteBuffer Key() const noexcept override {
        static const ByteBuffer kEmpty;
        return IsValid() ? items_[index_].first : kEmpty;
    }
    
    const ByteBuffer& Value() const noexcept override {
        static const ByteBuffer kEmpty;
        return IsValid() ? items_[index_].second : kEmpty;
    }

private:
    friend class MvccMemTable;
    
    MvccMemTableIterator(
        std::shared_ptr<MvccSkipList> skiplist,
        const Bound& lower,
        const Bound& upper,
        uint64_t read_ts) noexcept;
    
    std::shared_ptr<MvccSkipList> skiplist_;
    std::vector<std::pair<ByteBuffer, ByteBuffer>> items_;
    size_t index_{0};
};

} // namespace util
