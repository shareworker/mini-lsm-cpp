#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "byte_buffer.hpp"
#include "skiplist.hpp"

namespace util {

/**
 * @brief Segmented Write-Ahead Log (WAL) for durability.
 * 
 * Enhanced version of WAL that supports segmentation, rolling, and retention.
 * WAL files are split into segments with configurable size limits, and old
 * segments are automatically cleaned up based on retention policy.
 */
class WalSegment {
public:
    /**
     * @brief Configuration options for WAL segments.
     */
    struct Options {
        // Maximum size of a single WAL segment file
        size_t max_segment_size;
        
        // Maximum number of segments to retain
        size_t max_segments;
        
        // Sync on every write (slower but safer)
        bool sync_on_write;
        
        // Constructor with default values
        Options() 
            : max_segment_size(64 * 1024 * 1024),  // 64MB default
              max_segments(4),                     // Keep 4 segments by default
              sync_on_write(false) {}
    };

    /**
     * @brief Creates a new segmented WAL.
     * 
     * @param dir Directory to store WAL segments
     * @param name Base name for WAL segments
     * @param options Configuration options
     * @return WalSegment instance on success, nullptr on failure
     */
    static std::unique_ptr<WalSegment> Create(
        const std::filesystem::path& dir,
        const std::string& name,
        const Options& options = {});

    /**
     * @brief Recovers data from existing WAL segments and populates the skiplist.
     * 
     * @param dir Directory containing WAL segments
     * @param name Base name for WAL segments
     * @param skiplist Skiplist to populate with recovered data
     * @param options Configuration options
     * @return WalSegment instance on success, nullptr on failure
     */
    static std::unique_ptr<WalSegment> Recover(
        const std::filesystem::path& dir,
        const std::string& name,
        std::shared_ptr<SkipList<ByteBuffer, ByteBuffer>> skiplist,
        const Options& options = {});

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
     * @brief Forces rotation to a new segment.
     * 
     * @return true on success, false on failure
     */
    bool RotateSegment();

    /**
     * @brief Cleans up old segments based on retention policy.
     * 
     * @return Number of segments removed
     */
    size_t CleanupSegments();

    /**
     * @brief Gets the current segment ID.
     * 
     * @return Current segment ID
     */
    size_t CurrentSegmentId() const { return current_segment_id_; }

    /**
     * @brief Gets the path to the current segment.
     * 
     * @return Path to current segment
     */
    std::filesystem::path CurrentSegmentPath() const;

    /**
     * @brief Gets the total number of segments.
     * 
     * @return Number of segments
     */
    size_t SegmentCount() const;

    /**
     * @brief Destructor ensures file is properly closed.
     */
    ~WalSegment();

    // Prevent copy and move
    WalSegment(const WalSegment&) = delete;
    WalSegment& operator=(const WalSegment&) = delete;
    WalSegment(WalSegment&&) = delete;
    WalSegment& operator=(WalSegment&&) = delete;

private:
    /**
     * @brief Information about a WAL segment.
     */
    struct SegmentInfo {
        size_t id;
        std::filesystem::path path;
        size_t size;
    };

    /**
     * @brief Private constructor for Create and Recover factory methods.
     * 
     * @param dir Directory for WAL segments
     * @param name Base name for WAL segments
     * @param options Configuration options
     */
    explicit WalSegment(
        const std::filesystem::path& dir,
        const std::string& name,
        const Options& options);

    /**
     * @brief Initializes a new segment.
     * 
     * @return true on success, false on failure
     */
    bool InitNewSegment();

    /**
     * @brief Lists all WAL segments in the directory.
     * 
     * @return Vector of segment information
     */
    std::vector<SegmentInfo> ListSegments() const;

    /**
     * @brief Generates a path for a new segment.
     * 
     * @param segment_id Segment ID
     * @return Path to the segment
     */
    std::filesystem::path MakeSegmentPath(size_t segment_id) const;

    // Configuration
    Options options_;
    std::filesystem::path dir_;
    std::string name_;
    
    // Current state
    size_t current_segment_id_{0};
    std::vector<SegmentInfo> segments_;
    std::unique_ptr<std::fstream> file_;
    std::mutex file_mutex_;
    size_t current_size_{0};
};

}  // namespace util
