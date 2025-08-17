#pragma once

#include <cstddef>
#include <optional>
#include <vector>

namespace util {

// Defines the compaction strategy.
enum class CompactionStrategy {
    kNoCompaction,
    kSimple,
    kLeveled,
    kTiered,
};

// Represents a compaction task for simple leveled compaction.
struct SimpleLeveledCompactionTask {
    // `upper_level` is `std::nullopt` if L0 is involved in this compaction.
    std::optional<size_t> upper_level;
    std::vector<size_t> upper_level_sst_ids;
    size_t lower_level;
    std::vector<size_t> lower_level_sst_ids;
    bool is_lower_level_bottom_level;
};

} // namespace util
