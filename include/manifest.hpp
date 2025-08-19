#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <unordered_map>
#include <vector>

#include "byte_buffer.hpp"


/**
 * @brief Manifest for managing LSM tree metadata
 * 
 * The manifest keeps track of which SST files belong to which levels
 * and stores other persistent metadata needed for the LSM tree.
 */
// -----------------------------------------------------------------------------
// Manifest record types – append-only log similar to Rust implementation
// -----------------------------------------------------------------------------

enum class ManifestRecordTag : uint8_t {
    kFlush = 1,        // Flush(memtable_id / sst_id)
    kNewMemtable = 2,  // NewMemtable(memtable_id)
    kCompaction = 3    // Compaction(from_level, from_ids, to_level, to_ids)
};

struct ManifestRecord {
    ManifestRecordTag tag;

    // For kFlush / kNewMemtable: single_id stores the id
    size_t single_id = 0;

    // For kCompaction:
    size_t from_level = 0;
    std::vector<size_t> from_ids;
    size_t to_level = 0;
    std::vector<size_t> to_ids;
};

class Manifest {
public:
    /**
     * @brief Creates a new manifest file
     * 
     * @param path Path to the manifest file
     * @return std::unique_ptr<Manifest> New manifest instance
     */
    static std::unique_ptr<Manifest> Create(const std::filesystem::path& path);
    
    /**
     * @brief Opens an existing manifest file
     * 
     * @param path Path to the manifest file
     * @return std::unique_ptr<Manifest> Manifest instance
     */
    static std::unique_ptr<Manifest> Open(const std::filesystem::path& path);
    
    /**
     * @brief Destructor ensures file is properly closed
     */
    ~Manifest() = default;
    
    /**
     * @brief Prevent copying
     */
    Manifest(const Manifest&) = delete;
    Manifest& operator=(const Manifest&) = delete;
    
    /**
     * @brief Prevent moving
     */
    Manifest(Manifest&&) = delete;
    Manifest& operator=(Manifest&&) = delete;

    /**
     * @brief Adds a new SST file to the manifest
     * 
     * @param id ID of the SST file
     * @param level Level to add the SST to
     * @return true on success
     */
    bool AddSst(size_t id, size_t level);
    
    /**
     * @brief Moves SST files during compaction
     * 
     * @param from_level Source level
     * @param from_ids Source SST IDs
     * @param to_level Target level
     * @param to_ids Target SST IDs
     * @return true on success
     */
    bool MoveSsts(
        size_t from_level,
        const std::vector<size_t>& from_ids,
        size_t to_level,
        const std::vector<size_t>& to_ids);
    
    /**
     * @brief Returns the current version of the manifest data
     * 
     * @return Current version data
     */
    std::vector<std::pair<size_t, std::vector<size_t>>> GetCurrentVersion() const;
    
    /**
     * @brief Returns the path to the manifest file
     */
    std::filesystem::path GetPath() const;
    
    // Flush snapshot to disk (currently no-op as we use append-only records)
    bool Flush() { return true; }

    // ---------------------------------------------------------------------
    // Append a manifest record (immediately fsync).
    // ---------------------------------------------------------------------
    bool AddRecord(const ManifestRecord& rec);

private:
    /**
     * @brief Private constructor for Create/Open factory methods
     * 
     * @param path Path to the manifest file
     * @param create Whether to create a new file or open existing
     */
    explicit Manifest(const std::filesystem::path& path, bool create);
    
    // Path to the manifest file
    std::filesystem::path path_;
    
    // Current SST version data, mapping level numbers to SST IDs
    std::vector<std::pair<size_t, std::vector<size_t>>> current_version_;
    

};

