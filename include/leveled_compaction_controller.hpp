#pragma once

#include <memory>
#include <vector>
#include <optional>

#include "compaction_controller.hpp"

namespace util {

// Leveled compaction task
struct LeveledCompactionTask {
    // If upper_level is std::nullopt, then it is L0 compaction
    std::optional<size_t> upper_level;
    std::vector<size_t> upper_level_sst_ids;
    size_t lower_level;
    std::vector<size_t> lower_level_sst_ids;
    bool is_lower_level_bottom_level;
};

// Leveled compaction options
struct LeveledCompactionOptions {
    size_t level_size_multiplier;
    size_t level0_file_num_compaction_trigger;
    size_t max_levels;
    size_t base_level_size_mb;
};

// Leveled compaction controller
class LeveledCompactionController final : public CompactionController {
public:
    explicit LeveledCompactionController(const std::shared_ptr<LsmStorageOptions>& options);

    std::optional<SimpleLeveledCompactionTask> GenerateCompactionTask(
        const LsmStorageState& state) const override;

    bool NeedsCompaction(const LsmStorageState& state) const override;

    bool ApplyCompaction(
        LsmStorageState& state,
        const SimpleLeveledCompactionTask& task,
        const std::vector<size_t>& sst_ids,
        size_t& max_sst_id) override;

private:
    // Find overlapping SSTs
    std::vector<size_t> FindOverlappingSsts(
        const LsmStorageState& state,
        const std::vector<size_t>& sst_ids,
        size_t in_level) const;

    // Generate a compaction task
    std::optional<LeveledCompactionTask> GenerateInternalTask(const LsmStorageState& state) const;
    
    // Get the target size for a level
    size_t GetLevelSize(size_t level) const;
    
    // Get the maximum size for a level
    size_t GetMaxLevelSize(size_t level) const;
    
    // Apply compaction result
    std::pair<LsmStorageState, std::vector<size_t>> ApplyCompactionResult(
        const LsmStorageState& state,
        const LeveledCompactionTask& task,
        const std::vector<size_t>& output,
        bool in_recovery = false) const;

    LeveledCompactionOptions options_;
};

} // namespace util
