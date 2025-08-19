#pragma once

#include <memory>
#include <vector>
#include <optional>

#include "compaction_controller.hpp"
#include "lsm_storage.hpp"

// Forward declarations
class LsmStorageState;

// Tiered compaction task
struct TieredCompactionTask {
    std::vector<std::pair<size_t, std::vector<size_t>>> tiers;
    bool bottom_tier_included;
};

// Tiered compaction options
struct TieredCompactionOptions {
    size_t num_tiers;
    size_t max_size_amplification_percent;
    size_t size_ratio;
    size_t min_merge_width;
    std::optional<size_t> max_merge_width;
};

// Tiered compaction controller
class TieredCompactionController final : public CompactionController {
public:
    explicit TieredCompactionController(const std::shared_ptr<LsmStorageOptions>& options);

    std::optional<SimpleLeveledCompactionTask> GenerateCompactionTask(
        const LsmStorageState& state) const override;

    bool NeedsCompaction(const LsmStorageState& state) const override;

    bool ApplyCompaction(LsmStorageState& state,
                         const SimpleLeveledCompactionTask& task,
                         const std::vector<size_t>& sst_ids,
                         size_t& max_sst_id) override;

    void FlushToL0(LsmStorageState& state, size_t sst_id);

private:
    // Generate an internal tiered compaction task
    std::optional<TieredCompactionTask> GenerateTieredTask(
        const LsmStorageState& state) const;

    // Apply compaction result
    std::pair<LsmStorageState, std::vector<size_t>> ApplyCompactionResult(
        const LsmStorageState& state,
        const TieredCompactionTask& task,
        const std::vector<size_t>& output) const;

    TieredCompactionOptions options_;
};

