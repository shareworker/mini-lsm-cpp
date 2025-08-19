#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "skiplist.hpp"
#include "byte_buffer.hpp"


/**
 * @brief Write-Ahead Log (WAL) for durability.
 * 
 * The WAL is used to provide durability guarantees by recording all changes
 * before they are applied to the main data structure. In the event of a crash,
 * the WAL can be used to recover the database to a consistent state.
 */
class Wal {
public:
    /**
     * @brief Creates a new WAL file.
     * 
     * @param path Path to the WAL file
     * @return Wal instance on success
     */
    static std::unique_ptr<Wal> Create(const std::filesystem::path& path);

    /**
     * @brief Recovers data from an existing WAL file and populates the skiplist.
     * 
     * @param path Path to the WAL file
     * @param skiplist Skiplist to populate with recovered data
     * @return Wal instance on success
     */
    static std::unique_ptr<Wal> Recover(
        const std::filesystem::path& path,
        std::shared_ptr<SkipList<ByteBuffer, ByteBuffer>> skiplist);

    /**
     * @brief Writes a key-value pair to the WAL.
     * 
     * @param key The key to write
     * @param value The value to write
     * @return true on success, false on failure
     */
    bool Put(const ByteBuffer& key, const ByteBuffer& value);

    /**
     * @brief Writes multiple key-value pairs to the WAL as a batch.
     * 
     * @param data Vector of key-value pairs to write
     * @return true on success, false on failure
     */
    bool PutBatch(const std::vector<std::pair<ByteBuffer, ByteBuffer>>& data);

    /**
     * @brief Syncs the WAL to disk.
     * 
     * @return true on success, false on failure
     */
    bool Sync();

    /**
     * @brief Destructor ensures file is properly closed.
     */
    ~Wal() = default;

    // Prevent copy and move
    Wal(const Wal&) = delete;
    Wal& operator=(const Wal&) = delete;
    Wal(Wal&&) = delete;
    Wal& operator=(Wal&&) = delete;

private:
    /**
     * @brief Private constructor for Create and Recover factory methods.
     * 
     * @param file File stream to write to
     * @param path Path to the WAL file for syncing operations
     */
    explicit Wal(std::unique_ptr<std::fstream> file, const std::filesystem::path& path);

    // File stream wrapped in a mutex for thread safety
    std::unique_ptr<std::fstream> file_;
    std::filesystem::path file_path_;
    std::mutex file_mutex_;
};

