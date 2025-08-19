#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "wal.hpp"
#include "byte_buffer.hpp"
#include "mvcc_skiplist.hpp"

/**
 * @brief MVCC-aware Write-Ahead Log that supports timestamp persistence
 * 
 * This class extends the basic WAL functionality to support MVCC operations
 * by persisting timestamps along with key-value pairs. This ensures that
 * version information is preserved across crashes and restarts.
 */
class MvccWal {
public:
    /**
     * @brief Creates a new MVCC WAL file.
     * 
     * @param path Path to the WAL file
     * @return MvccWal instance on success, nullptr on failure
     */
    static std::unique_ptr<MvccWal> Create(const std::filesystem::path& path);

    /**
     * @brief Recovers data from an existing MVCC WAL file and populates the MVCC skiplist.
     * 
     * @param path Path to the WAL file
     * @param skiplist MVCC skiplist to populate with recovered data
     * @return MvccWal instance on success, nullptr on failure
     */
    static std::unique_ptr<MvccWal> Recover(
        const std::filesystem::path& path,
        std::shared_ptr<MvccSkipList> skiplist);

    /**
     * @brief Writes a key-value pair with timestamp to the MVCC WAL.
     * 
     * @param key The key to write
     * @param value The value to write
     * @param ts The timestamp for this operation
     * @return true on success, false on failure
     */
    bool PutWithTs(const ByteBuffer& key, const ByteBuffer& value, uint64_t ts);

    /**
     * @brief Writes multiple key-value pairs with timestamps to the WAL as a batch.
     * 
     * @param data Vector of (key, value, timestamp) tuples to write
     * @return true on success, false on failure
     */
    bool PutBatchWithTs(
        const std::vector<std::tuple<ByteBuffer, ByteBuffer, uint64_t>>& data);

    /**
     * @brief Writes a delete operation (tombstone) with timestamp to the WAL.
     * 
     * @param key The key to delete
     * @param ts The timestamp for this delete operation
     * @return true on success, false on failure
     */
    bool DeleteWithTs(const ByteBuffer& key, uint64_t ts);

    /**
     * @brief Syncs the WAL to disk.
     * 
     * @return true on success, false on failure
     */
    bool Sync();

    /**
     * @brief Destructor ensures file is properly closed.
     */
    ~MvccWal() = default;

    // Prevent copy and move
    MvccWal(const MvccWal&) = delete;
    MvccWal& operator=(const MvccWal&) = delete;
    MvccWal(MvccWal&&) = delete;
    MvccWal& operator=(MvccWal&&) = delete;

private:
    /**
     * @brief Private constructor for Create and Recover factory methods.
     * 
     * @param file File stream to write to
     * @param path Path to the WAL file for syncing operations
     */
    explicit MvccWal(std::unique_ptr<std::fstream> file, const std::filesystem::path& path);
    
    /**
     * @brief Writes a single MVCC WAL entry to disk.
     * 
     * Format: [entry_type:1][key_len:4][value_len:4][timestamp:8][key_data][value_data][checksum:4]
     * 
     * @param entry_type Type of entry (PUT=1, DELETE=2)
     * @param key The key
     * @param value The value (empty for deletes)
     * @param ts The timestamp
     * @return true on success, false on failure
     */
    bool WriteEntry(uint8_t entry_type, const ByteBuffer& key, 
                                 const ByteBuffer& value, uint64_t ts);
    
    /**
     * @brief Reads and parses a single MVCC WAL entry from disk.
     * 
     * @param key Output parameter for the key
     * @param value Output parameter for the value
     * @param ts Output parameter for the timestamp
     * @param entry_type Output parameter for the entry type
     * @return true if entry was successfully read, false if EOF or error
     */
    bool ReadEntry(ByteBuffer& key, ByteBuffer& value, 
                                uint64_t& ts, uint8_t& entry_type);

    // File stream wrapped in a mutex for thread safety
    std::unique_ptr<std::fstream> file_;
    std::filesystem::path file_path_;
    std::mutex file_mutex_;
    
    // MVCC WAL entry types
    static constexpr uint8_t kEntryTypePut = 1;
    static constexpr uint8_t kEntryTypeDelete = 2;
};

