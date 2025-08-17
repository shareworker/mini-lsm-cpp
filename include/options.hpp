#pragma once

#include <cstddef>
#include <string>

namespace util {

// Forward declarations
class CompactionOptions;

/**
 * @brief Options for configuring the LSM storage engine
 */
class Options {
public:
    // Block size in bytes
    std::size_t block_size{4096};
    
    // SST size in bytes, also the approximate memtable capacity limit
    std::size_t target_sst_size{2 * 1024 * 1024}; // 2MB default
    
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    std::size_t num_memtable_limit{5};
    
    // Whether to enable write-ahead logging
    bool enable_wal{true};
    
    // Whether to enable serializable isolation level for MVCC
    bool serializable{false};
    
    // Whether to create the database if it doesn't exist
    bool create_if_missing{false};
    
    // Working directory for the database
    std::string work_dir;

    // Simple Leveled Compaction Options
    size_t simple_leveled_size_ratio_percent{50};
    size_t simple_leveled_level0_file_num_compaction_trigger{4};
    size_t simple_leveled_max_levels{7};
    
    // Default constructor
    Options() = default;
};

} // namespace util
