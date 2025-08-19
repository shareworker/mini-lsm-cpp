#include "../include/leveled_compaction_controller.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <memory>
#include <optional>
#include <vector>
#include <unordered_set>

#include "../include/lsm_storage.hpp"
#include "../include/sstable.hpp"


LeveledCompactionController::LeveledCompactionController(
    const std::shared_ptr<LsmStorageOptions>& options)
    : options_{
          .level_size_multiplier = 10,  // Default: each level is 10x the size of the previous level
          .level0_file_num_compaction_trigger = 4,  // Default: trigger at 4 files
          .max_levels = 7,  // Default: 7 levels (L0-L6)
          .base_level_size_mb = 10  // Default: 10MB base level size
      } {
    // Override defaults with options from LsmStorageOptions if provided
    // Note: Since LsmStorageOptions doesn't have these specific fields,
    // we're using the defaults defined above
}

bool LeveledCompactionController::NeedsCompaction(const LsmStorageState& state) const {
    return GenerateCompactionTask(state).has_value();
}

std::optional<SimpleLeveledCompactionTask> LeveledCompactionController::GenerateCompactionTask(
    const LsmStorageState& state) const {
    auto task = GenerateInternalTask(state);
    if (task) {
        // Convert the internal task to the public SimpleLeveledCompactionTask.
        // NOTE: This is a lossy conversion. The `upper_level_sst_ids` are lost,
        // which will be a problem for `ApplyCompaction`. This is a temporary
        // fix to make the code compile.
        return SimpleLeveledCompactionTask{
            .upper_level = task->upper_level,
            .upper_level_sst_ids = std::move(task->upper_level_sst_ids),
            .lower_level = task->lower_level,
            .lower_level_sst_ids = std::move(task->lower_level_sst_ids),
            .is_lower_level_bottom_level = (task->lower_level == options_.max_levels - 1),
        };
    }
    return std::nullopt;
}

bool LeveledCompactionController::ApplyCompaction(
    LsmStorageState& state,
    const SimpleLeveledCompactionTask& task,
    const std::vector<size_t>& sst_ids,
    size_t& max_sst_id) {
    // Convert to sets for efficient removal
    std::unordered_set<size_t> upper_level_sst_ids_set(
        task.upper_level_sst_ids.begin(), task.upper_level_sst_ids.end());
    std::unordered_set<size_t> lower_level_sst_ids_set(
        task.lower_level_sst_ids.begin(), task.lower_level_sst_ids.end());
    
    // Remove SSTs from upper level (or L0 if upper_level is None)
    if (task.upper_level.has_value()) {
        // Find the upper level in state.levels
        auto upper_level_it = std::find_if(
            state.levels.begin(), state.levels.end(),
            [&task](const auto& level_pair) {
                return level_pair.first == task.upper_level.value();
            }
        );
        
        if (upper_level_it != state.levels.end()) {
            std::vector<size_t> new_upper_level_ssts;
            for (auto sst_id : upper_level_it->second) {
                if (upper_level_sst_ids_set.find(sst_id) == upper_level_sst_ids_set.end()) {
                    new_upper_level_ssts.push_back(sst_id);
                }
            }
            upper_level_it->second = std::move(new_upper_level_ssts);
        }
    } else {
        // Remove from L0
        std::vector<size_t> new_l0_ssts;
        for (auto sst_id : state.l0_sstables) {
            if (upper_level_sst_ids_set.find(sst_id) == upper_level_sst_ids_set.end()) {
                new_l0_ssts.push_back(sst_id);
            }
        }
        state.l0_sstables = std::move(new_l0_ssts);
    }
    
    // Find the lower level in state.levels
    auto lower_level_it = std::find_if(
        state.levels.begin(), state.levels.end(),
        [&task](const auto& level_pair) {
            return level_pair.first == task.lower_level;
        }
    );
    
    // Create the lower level if it doesn't exist
    if (lower_level_it == state.levels.end()) {
        state.levels.emplace_back(task.lower_level, std::vector<size_t>{});
        // Keep levels sorted by level number
        std::sort(state.levels.begin(), state.levels.end(),
                  [](const auto& a, const auto& b) {
                      return a.first < b.first;
                  });
        
        // Find the newly inserted level
        lower_level_it = std::find_if(
            state.levels.begin(), state.levels.end(),
            [&task](const auto& level_pair) {
                return level_pair.first == task.lower_level;
            }
        );
    }
    
    // Remove old SSTs from lower level and add new ones
    if (lower_level_it != state.levels.end()) {
        std::vector<size_t> new_lower_level_ssts;
        for (auto sst_id : lower_level_it->second) {
            if (lower_level_sst_ids_set.find(sst_id) == lower_level_sst_ids_set.end()) {
                new_lower_level_ssts.push_back(sst_id);
            }
        }
        
        // Add the new compaction output SSTs
        new_lower_level_ssts.insert(new_lower_level_ssts.end(), sst_ids.begin(), sst_ids.end());
        
        // Sort SSTs by first key (like in Rust implementation)
        std::sort(new_lower_level_ssts.begin(), new_lower_level_ssts.end(),
                  [&state](size_t a, size_t b) {
                      auto sst_a_it = state.sstables.find(a);
                      auto sst_b_it = state.sstables.find(b);
                      if (sst_a_it != state.sstables.end() && sst_b_it != state.sstables.end()) {
                          return sst_a_it->second->FirstKey() < sst_b_it->second->FirstKey();
                      }
                      return a < b; // Fallback to ID comparison
                  });
        
        lower_level_it->second = std::move(new_lower_level_ssts);
    }
    
    // Update max_sst_id if any of the new SST IDs are higher
    for (auto sst_id : sst_ids) {
        max_sst_id = std::max(max_sst_id, sst_id);
    }
    
    return true;
}

std::vector<size_t> LeveledCompactionController::FindOverlappingSsts(
    const LsmStorageState& state,
    const std::vector<size_t>& sst_ids,
    size_t in_level) const {
    
    // Get the SST metadata for the input SSTs
    std::vector<std::pair<ByteBuffer, ByteBuffer>> key_ranges;
    for (auto sst_id : sst_ids) {
        auto sst_it = state.sstables.find(sst_id);
        if (sst_it != state.sstables.end()) {
            const auto& sst = sst_it->second;
            key_ranges.push_back({sst->FirstKey(), sst->LastKey()});
        }
    }
    
    // Find the target level
    auto level_it = std::find_if(
        state.levels.begin(),
        state.levels.end(),
        [in_level](const auto& level_pair) {
            return level_pair.first == in_level;
        }
    );
    
    if (level_it == state.levels.end()) {
        // Level not found
        return {};
    }
    
    // Find overlapping SSTs in the target level
    std::vector<size_t> overlapping_ssts;
    for (auto sst_id : level_it->second) {
        auto sst_it = state.sstables.find(sst_id);
        if (sst_it != state.sstables.end()) {
            const auto& sst = sst_it->second;
            
            // Check if this SST overlaps with any of the key ranges
            for (const auto& [first_key, last_key] : key_ranges) {
                if (!(last_key < sst->FirstKey() || sst->LastKey() < first_key)) {
                    // Overlap found
                    overlapping_ssts.push_back(sst_id);
                    break;
                }
            }
        }
    }
    
    return overlapping_ssts;
}

std::optional<LeveledCompactionTask> LeveledCompactionController::GenerateInternalTask(
    const LsmStorageState& state) const {
    
    // Check if L0 needs compaction
    if (state.l0_sstables.size() >= options_.level0_file_num_compaction_trigger) {
        // L0 compaction
        size_t target_level = 1;
        
        // Find the target level
        auto level_it = std::find_if(
            state.levels.begin(),
            state.levels.end(),
            [target_level](const auto& level_pair) {
                return level_pair.first == target_level;
            }
        );
        
        // Create the compaction task
        LeveledCompactionTask task;
        task.upper_level = std::nullopt;  // L0 compaction
        task.upper_level_sst_ids = state.l0_sstables;
        task.lower_level = target_level;
        
        if (level_it != state.levels.end()) {
            // Find overlapping SSTs in the target level
            task.lower_level_sst_ids = FindOverlappingSsts(
                state,
                state.l0_sstables,
                target_level
            );
        }
        
        // Check if this is the bottom level
        task.is_lower_level_bottom_level = (target_level == options_.max_levels - 1);
        
        return task;
    }
    
    // Check if any other level needs compaction
    for (size_t level = 1; level < options_.max_levels - 1; ++level) {
        // Find the level in the state
        auto level_it = std::find_if(
            state.levels.begin(),
            state.levels.end(),
            [level](const auto& level_pair) {
                return level_pair.first == level;
            }
        );
        
        if (level_it == state.levels.end()) {
            // Level not found, skip
            continue;
        }
        
        // Calculate the level size in bytes
        size_t level_size = 0;
        for (auto sst_id : level_it->second) {
            auto sst_it = state.sstables.find(sst_id);
            if (sst_it != state.sstables.end()) {
                // Estimate file size based on SST ID for now
                // In a real implementation, we would track file sizes in the state
                level_size += 1024 * 1024; // Assume 1MB per SST
            }
        }
        
        // Calculate the target size for this level
        size_t target_size = GetLevelSize(level);
        
        if (level_size <= target_size) {
            // Level size is within limits, skip
            continue;
        }
        
        // Level needs compaction
        size_t next_level = level + 1;
        
        // Find the next level in the state
        auto next_level_it = std::find_if(
            state.levels.begin(),
            state.levels.end(),
            [next_level](const auto& level_pair) {
                return level_pair.first == next_level;
            }
        );
        
        // Create the compaction task
        LeveledCompactionTask task;
        task.upper_level = level;
        
        // Select SSTs from the current level
        // For simplicity, we'll just select the first few SSTs
        // In a real implementation, we would select based on key ranges
        size_t num_ssts_to_compact = std::min(
            level_it->second.size(),
            static_cast<size_t>(std::ceil((level_size - target_size) / static_cast<double>(1024 * 1024))) // Assume 1MB per SST
        );
        
        task.upper_level_sst_ids.assign(
            level_it->second.begin(),
            level_it->second.begin() + num_ssts_to_compact
        );
        
        task.lower_level = next_level;
        
        if (next_level_it != state.levels.end()) {
            // Find overlapping SSTs in the next level
            task.lower_level_sst_ids = FindOverlappingSsts(
                state,
                task.upper_level_sst_ids,
                next_level
            );
        }
        
        // Check if this is the bottom level
        task.is_lower_level_bottom_level = (next_level == options_.max_levels - 1);
        
        return task;
    }
    
    // No compaction needed
    return std::nullopt;
}

size_t LeveledCompactionController::GetLevelSize(size_t level) const {
    if (level == 0) {
        // L0 doesn't have a size limit
        return std::numeric_limits<size_t>::max();
    }
    
    // Calculate the target size for this level
    // base_level_size_mb * (level_size_multiplier ^ (level - 1))
    return options_.base_level_size_mb * 1024 * 1024 *
           std::pow(options_.level_size_multiplier, level - 1);
}

size_t LeveledCompactionController::GetMaxLevelSize(size_t level) const {
    if (level == 0) {
        // L0 doesn't have a size limit
        return std::numeric_limits<size_t>::max();
    }
    
    // Calculate the maximum size for this level
    // base_level_size_mb * (level_size_multiplier ^ level)
    return options_.base_level_size_mb * 1024 * 1024 *
           std::pow(options_.level_size_multiplier, level);
}

