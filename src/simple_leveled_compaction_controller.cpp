#include "../include/simple_leveled_compaction_controller.hpp"

#include <cassert>
#include <iostream>

#include <unordered_set>

#include "../include/lsm_storage.hpp"
#include "../include/sstable.hpp"


// Helper function to calculate total size of SSTs in a level
static uint64_t CalculateLevelSize(const std::vector<size_t>& sst_ids,
                                   const std::unordered_map<size_t, std::shared_ptr<SsTable>>& sstables) {
    uint64_t total_size = 0;
    for (size_t sst_id : sst_ids) {
        auto it = sstables.find(sst_id);
        if (it != sstables.end() && it->second) {
            total_size += it->second->FileSize();
        }
    }
    return total_size;
}

SimpleLeveledCompactionController::SimpleLeveledCompactionController(
    const std::shared_ptr<LsmStorageOptions>& options) {
    if (options) {
        options_.size_ratio_percent = options->simple_leveled_size_ratio_percent;
        options_.level0_file_num_compaction_trigger =
            options->simple_leveled_level0_file_num_compaction_trigger;
        options_.max_levels = options->simple_leveled_max_levels;
    }
}

std::optional<SimpleLeveledCompactionTask> SimpleLeveledCompactionController::GenerateCompactionTask(
    const LsmStorageState& state) const {
    if (options_.max_levels == 0) {
        return std::nullopt;
    }

    // L0 to L1 compaction - use file size instead of count
    uint64_t l0_total_size = CalculateLevelSize(state.l0_sstables, state.sstables);
    uint64_t l0_size_threshold = static_cast<uint64_t>(options_.level0_file_num_compaction_trigger) * 64 * 1024 * 1024; // 64MB per "file unit"
    
    if (l0_total_size >= l0_size_threshold) {
        std::cout << "compaction triggered at level 0 because L0 total size " << l0_total_size
                  << " bytes >= " << l0_size_threshold << " bytes" << std::endl;
        return SimpleLeveledCompactionTask{
            .upper_level = std::nullopt,
            .upper_level_sst_ids = state.l0_sstables,
            .lower_level = 1,
            .lower_level_sst_ids = state.levels.empty() ? std::vector<size_t>{} : state.levels[0].second,
            .is_lower_level_bottom_level = (options_.max_levels == 1),
        };
    }

    // Compaction for other levels
    for (size_t i = 1; i < options_.max_levels; ++i) {
        size_t lower_level = i + 1;
        if (i - 1 >= state.levels.size() || lower_level - 1 >= state.levels.size()) {
            continue;
        }

        uint64_t upper_level_size = CalculateLevelSize(state.levels[i - 1].second, state.sstables);
        uint64_t lower_level_size = CalculateLevelSize(state.levels[lower_level - 1].second, state.sstables);
        
        // Avoid division by zero
        if (upper_level_size == 0) {
            continue;
        }
        
        double size_ratio = static_cast<double>(lower_level_size) / static_cast<double>(upper_level_size);

        if (size_ratio < static_cast<double>(options_.size_ratio_percent) / 100.0) {
            std::cout << "compaction triggered at level " << i << " and " << lower_level
                       << " with size ratio " << size_ratio
                       << " (upper: " << upper_level_size << " bytes, lower: " << lower_level_size << " bytes)" << std::endl;
            return SimpleLeveledCompactionTask{
                .upper_level = i,
                .upper_level_sst_ids = state.levels[i - 1].second,
                .lower_level = lower_level,
                .lower_level_sst_ids = state.levels[lower_level - 1].second,
                .is_lower_level_bottom_level = (lower_level == options_.max_levels),
            };
        }
    }

    return std::nullopt;
}

bool SimpleLeveledCompactionController::NeedsCompaction(const LsmStorageState& state) const {
    return GenerateCompactionTask(state).has_value();
}

// Helper to apply compaction results and return files to be removed
static std::vector<size_t> ApplyCompactionResult(
    LsmStorageState& state,
    const SimpleLeveledCompactionTask& task,
    const std::vector<size_t>& output_ssts) {
    std::vector<size_t> files_to_remove;

    if (task.upper_level.has_value()) { // Compaction between two levels
        size_t upper_level_idx = task.upper_level.value() - 1;
        assert(upper_level_idx < state.levels.size());
        files_to_remove.insert(files_to_remove.end(), state.levels[upper_level_idx].second.begin(),
                               state.levels[upper_level_idx].second.end());
        state.levels[upper_level_idx].second.clear();
    } else { // L0 to L1 compaction
        std::unordered_set<size_t> l0_compacted(task.upper_level_sst_ids.begin(),
                                                  task.upper_level_sst_ids.end());
        files_to_remove.insert(files_to_remove.end(), task.upper_level_sst_ids.begin(),
                               task.upper_level_sst_ids.end());

        std::vector<size_t> new_l0_sstables;
        for (auto sst_id : state.l0_sstables) {
            if (l0_compacted.find(sst_id) == l0_compacted.end()) {
                new_l0_sstables.push_back(sst_id);
            }
        }
        state.l0_sstables = std::move(new_l0_sstables);
    }

    size_t lower_level_idx = task.lower_level - 1;
    assert(lower_level_idx < state.levels.size());
    files_to_remove.insert(files_to_remove.end(), state.levels[lower_level_idx].second.begin(),
                           state.levels[lower_level_idx].second.end());
    state.levels[lower_level_idx].second = output_ssts;

    return files_to_remove;
}

bool SimpleLeveledCompactionController::ApplyCompaction(
    LsmStorageState& state, const SimpleLeveledCompactionTask& task,
    const std::vector<size_t>& sst_ids, size_t& max_sst_id) {
    // Note: The original rust implementation returns the files to remove.
    // Here we assume the caller will handle the actual file deletion.
    ApplyCompactionResult(state, task, sst_ids);
    
    // Update max_sst_id if any of the new SST IDs are higher
    for (size_t sst_id : sst_ids) {
        max_sst_id = std::max(max_sst_id, sst_id);
    }
    
    return true;
}

