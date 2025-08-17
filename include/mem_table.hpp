#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <optional>
#include <vector>

#include "skiplist.hpp"
#include "byte_buffer.hpp"
#include "storage_iterator.hpp"
#include "wal.hpp"
#include "wal_segment.hpp"
#include "bound.hpp"

namespace util {

// Forward declarations
class MemTableIterator;

/**
 * @brief In-memory table implementation using a concurrent skip list.
 * 
 * MemTable is a core in-memory data structure for storing key-value pairs.
 * It uses a thread-safe skip list for efficient lookups and updates, with
 * optional write-ahead logging for durability.
 */
class MemTable {
public:
    /**
     * @brief Creates a new MemTable with the given ID.
     * 
     * @param id Unique identifier for this MemTable
     * @return std::unique_ptr<MemTable> A new MemTable instance
     */
    static std::unique_ptr<MemTable> Create(size_t id);

    /**
     * @brief Creates a new MemTable with the given ID and WAL.
     * 
     * @param id Unique identifier for this MemTable
     * @param wal Write-ahead log for durability
     * @return std::unique_ptr<MemTable> A new MemTable instance
     */
    static std::unique_ptr<MemTable> CreateWithWal(
        size_t id, 
        std::filesystem::path path);
        
    /**
     * @brief Recovers a MemTable from an existing WAL file.
     * 
     * This method reads the WAL file at the specified path, reconstructs the
     * MemTable state from the log entries, and returns a new MemTable instance
     * that can continue to use the same WAL file for future operations.
     * 
     * @param id Unique identifier for this MemTable
     * @param path Path to the existing WAL file
     * @return std::unique_ptr<MemTable> A recovered MemTable instance, or nullptr on failure
     */
    static std::unique_ptr<MemTable> RecoverFromWal(
        size_t id,
        const std::filesystem::path& path);
        
    /**
     * @brief Creates a new MemTable from an existing SkipList.
     * 
     * This is primarily used during recovery to create a memtable from
     * a skiplist that was populated from a WAL file.
     * 
     * @param id Unique identifier for this MemTable
     * @param skiplist The existing skiplist to use
     * @return std::unique_ptr<MemTable> A new MemTable instance
     */
    static std::unique_ptr<MemTable> CreateFromSkipList(
        size_t id,
        std::shared_ptr<SkipList<ByteBuffer, ByteBuffer>> skiplist);

    /**
     * @brief Destructor ensures proper cleanup.
     */
    ~MemTable() = default;

    /**
     * @brief Prevent copy construction and assignment.
     */
    MemTable(const MemTable&) = delete;
    MemTable& operator=(const MemTable&) = delete;

    /**
     * @brief Prevent move construction and assignment.
     */
    MemTable(MemTable&&) = delete;
    MemTable& operator=(MemTable&&) = delete;

    /**
     * @brief Puts a key-value pair into the MemTable.
     * 
     * @param key The key to insert
     * @param value The value to associate with the key
     * @return true if successful, false otherwise
     */
    bool Put(const ByteBuffer& key, const ByteBuffer& value);

    /**
     * @brief Gets the value associated with a key.
     * 
     * @param key The key to look up
     * @return std::optional<ByteBuffer> The value if found, std::nullopt otherwise
     */
    std::optional<ByteBuffer> Get(const ByteBuffer& key) const;
    
    /**
     * @brief Gets the value and timestamp associated with a key for MVCC.
     * 
     * This method is used for MVCC transactions to find the correct version
     * of a key that is visible to a transaction with a specific read timestamp.
     * 
     * @param key The key to look up
     * @param read_ts The read timestamp (transaction start timestamp)
     * @return std::optional<std::pair<ByteBuffer, uint64_t>> The value and timestamp if found and visible, std::nullopt otherwise
     */
    std::optional<std::pair<ByteBuffer, uint64_t>> GetWithTs(const ByteBuffer& key, uint64_t read_ts) const;

    /**
     * @brief Removes a key-value pair from the MemTable.
     * 
     * @param key The key to remove
     * @return true if successful, false if the key was not found
     */
    bool Remove(const ByteBuffer& key);

    /**
     * @brief Returns the approximate memory usage of the MemTable.
     * 
     * @return size_t The approximate size in bytes
     */
    size_t ApproximateSize() const;

    /**
     * @brief Returns the unique ID of this MemTable.
     * 
     * @return size_t The MemTable ID
     */
    size_t Id() const;

    /**
     * @brief Sync the WAL file to disk if WAL is enabled for this MemTable.
     *
     * This ensures durability before the memtable is frozen or flushed. If WAL
     * is disabled, this function is a no-op that returns true.
     *
     * @return true on success, false otherwise
     */
    bool SyncWal();

    /**
     * @brief Sets the WAL segment for this MemTable.
     * 
     * This allows the MemTable to use a shared WAL segment for durability.
     * 
     * @param wal_segment Shared WAL segment to use
     */
    void SetWalSegment(const std::shared_ptr<WalSegment>& wal_segment);

    /**
     * @brief Checks if the MemTable is empty.
     *
     * This delegates to the underlying skiplist implementation and incurs
     * only a shared lock on the skiplist mutex.
     */
    bool IsEmpty() const;

    /**
     * @brief Applies a function to each key-value pair in the MemTable.
     * 
     * @tparam Func Function type that accepts (const ByteBuffer&, const ByteBuffer&)
     * @param func Function to apply to each key-value pair
     */
    template <typename Func>
    void ForEach(Func func) const {
        skiplist_->ForEach(func);
    }

    MemTableIterator NewIterator(const Bound& lower_bound, const Bound& upper_bound) const;


private:
    /**
     * @brief Private constructor for factory methods.
     * 
     * @param id Unique identifier for this MemTable
     * @param wal Optional write-ahead log (may be nullptr)
     */
    explicit MemTable(size_t id, std::unique_ptr<Wal> wal = nullptr);

    // Core storage using a thread-safe skip list
    std::shared_ptr<SkipList<ByteBuffer, ByteBuffer>> skiplist_;
    
    // Optional write-ahead log for durability
    std::unique_ptr<Wal> wal_;
    
    // Optional segmented write-ahead log for durability
    std::shared_ptr<WalSegment> wal_segment_;
    
    // Unique identifier for this MemTable instance
    const size_t id_;
    
    // Thread-safe counter tracking estimated memory usage
    std::atomic<size_t> approximate_size_;
};

/**
 * @brief Standalone iterator over a snapshot of a MemTable.
 */
class MemTableIterator : public StorageIterator {
public:
    MemTableIterator(const MemTableIterator &) = delete;
    MemTableIterator &operator=(const MemTableIterator &) = delete;

    MemTableIterator(MemTableIterator &&) noexcept = default;
    MemTableIterator &operator=(MemTableIterator &&) noexcept = default;

    ~MemTableIterator() = default;

    bool IsValid() const noexcept override { return index_ < items_.size(); }

    void Next() noexcept override {
        if (index_ < items_.size()) {
            ++index_;
        }
    }

    ByteBuffer Key() const noexcept override {
        static const ByteBuffer kEmpty;
        return IsValid() ? items_[index_].first : kEmpty;
    }

    const ByteBuffer &Value() const noexcept override {
        static const ByteBuffer kEmpty;
        return IsValid() ? items_[index_].second : kEmpty;
    }

private:
    friend class MemTable;

    MemTableIterator(std::shared_ptr<SkipList<ByteBuffer, ByteBuffer>> map,
                     const Bound& lower, const Bound& upper) noexcept;

    std::shared_ptr<SkipList<ByteBuffer, ByteBuffer>> map_;
    std::vector<std::pair<ByteBuffer, ByteBuffer>> items_;
    size_t index_{0};
};

}  // namespace util
