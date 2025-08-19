#include "../include/tiered_compaction_controller.hpp"

#include <algorithm>
#include <cassert>
#include <iostream>
#include <limits>
#include <unordered_map>
#include <unordered_set>


TieredCompactionController::TieredCompactionController(
    const std::shared_ptr<LsmStorageOptions>& options)
    : options_{
          .num_tiers = 6,  // Default: 6 tiers
          .max_size_amplification_percent = 200,  // Default: 200%
          .size_ratio = 50,  // Default: 50% size ratio
          .min_merge_width = 2,  // Default: minimum 2 tiers to merge
          .max_merge_width = std::nullopt  // Default: no maximum
      } {
    // Override defaults with options from LsmStorageOptions if provided
    if (options) {
        if (options->tiered_num_tiers > 0) {
            options_.num_tiers = options->tiered_num_tiers;
        }
        if (options->tiered_max_size_amplification_percent > 0) {
            options_.max_size_amplification_percent = options->tiered_max_size_amplification_percent;
        }
        if (options->tiered_size_ratio > 0) {
            options_.size_ratio = options->tiered_size_ratio;
        }
        if (options->tiered_min_merge_width > 0) {
            options_.min_merge_width = options->tiered_min_merge_width;
        }
        if (options->tiered_max_merge_width > 0) {
            options_.max_merge_width = options->tiered_max_merge_width;
        }
    }
}

bool TieredCompactionController::NeedsCompaction(const LsmStorageState& state) const {
    return GenerateCompactionTask(state).has_value();
}

bool TieredCompactionController::ApplyCompaction(LsmStorageState& state,
                                                 const SimpleLeveledCompactionTask& task,
                                                 const std::vector<size_t>& sst_ids,
                                                 size_t& max_sst_id) {
    // NOTE: This is a workaround for the interface mismatch.
    // The base class uses SimpleLeveledCompactionTask, but tiered compaction
    // needs TieredCompactionTask. For now, we reconstruct the tiered task
    // from the simple task, but this is not ideal.
    
    // In tiered compaction, L0 should be empty
    if (!state.l0_sstables.empty()) {
        std::cerr << "L0 should be empty in tiered compaction" << std::endl;
        return false;
    }
    
    // Create a map of tiers to remove based on the task
    std::unordered_map<size_t, std::vector<size_t>> tier_to_remove;
    
    // Add all SST IDs from the task to the removal map
    // NOTE: This is a simplified approach since we don't have the full tier structure
    std::unordered_set<size_t> ssts_to_remove;
    ssts_to_remove.insert(task.upper_level_sst_ids.begin(), task.upper_level_sst_ids.end());
    ssts_to_remove.insert(task.lower_level_sst_ids.begin(), task.lower_level_sst_ids.end());
    
    // Find which tiers contain the SSTs to remove
    for (const auto& level_pair : state.levels) {
        size_t tier_id = level_pair.first;
        const auto& tier_ssts = level_pair.second;
        
        std::vector<size_t> ssts_in_this_tier;
        for (size_t sst_id : tier_ssts) {
            if (ssts_to_remove.find(sst_id) != ssts_to_remove.end()) {
                ssts_in_this_tier.push_back(sst_id);
            }
        }
        
        if (!ssts_in_this_tier.empty()) {
            tier_to_remove[tier_id] = ssts_in_this_tier;
        }
    }
    
    // Apply the compaction result
    std::vector<std::pair<size_t, std::vector<size_t>>> new_levels;
    bool new_tier_added = false;
    
    for (const auto& level_pair : state.levels) {
        size_t tier_id = level_pair.first;
        const auto& tier_ssts = level_pair.second;
        
        if (tier_to_remove.find(tier_id) != tier_to_remove.end()) {
            // This tier should be removed (or partially removed)
            const auto& ssts_to_remove_from_tier = tier_to_remove[tier_id];
            std::unordered_set<size_t> remove_set(ssts_to_remove_from_tier.begin(), 
                                                   ssts_to_remove_from_tier.end());
            
            std::vector<size_t> remaining_ssts;
            for (size_t sst_id : tier_ssts) {
                if (remove_set.find(sst_id) == remove_set.end()) {
                    remaining_ssts.push_back(sst_id);
                }
            }
            
            // Only keep the tier if it has remaining SSTs
            if (!remaining_ssts.empty()) {
                new_levels.emplace_back(tier_id, std::move(remaining_ssts));
            }
        } else {
            // Retain the tier as-is
            new_levels.push_back(level_pair);
        }
        
        // Add the compacted tier after processing all tiers to remove
        if (tier_to_remove.empty() && !new_tier_added && !sst_ids.empty()) {
            new_tier_added = true;
            // Use the first SST ID as the tier ID (following Rust logic)
            new_levels.emplace_back(sst_ids[0], sst_ids);
        }
    }
    
    // If we haven't added the new tier yet, add it now
    if (!new_tier_added && !sst_ids.empty()) {
        new_levels.emplace_back(sst_ids[0], sst_ids);
    }
    
    // Update the state
    state.levels = std::move(new_levels);
    
    // Keep levels sorted by tier ID
    std::sort(state.levels.begin(), state.levels.end(),
              [](const auto& a, const auto& b) {
                  return a.first < b.first;
              });
    
    // Update max_sst_id
    for (size_t sst_id : sst_ids) {
        max_sst_id = std::max(max_sst_id, sst_id);
    }
    
    return true;
}

void TieredCompactionController::FlushToL0(LsmStorageState& state, size_t sst_id) {
    size_t next_tier_id = 0;
    if (!state.levels.empty()) {
        for (const auto& level : state.levels) {
            next_tier_id = std::max(next_tier_id, level.first + 1);
        }
    }
    state.levels.emplace_back(next_tier_id, std::vector<size_t>{sst_id});
    // Keep the levels sorted by tier ID, similar to Rust's BTreeMap
    std::sort(state.levels.begin(), state.levels.end(), 
              [](const auto& a, const auto& b) {
                  return a.first < b.first;
              });
}

// The public interface, which must return a SimpleLeveledCompactionTask to match the base class.
std::optional<SimpleLeveledCompactionTask> TieredCompactionController::GenerateCompactionTask(
    const LsmStorageState& state) const {
    auto tiered_task = GenerateTieredTask(state);
    if (!tiered_task) {
        return std::nullopt;
    }

    // NOTE: This is a lossy conversion. Tiered compaction is fundamentally
    // different from leveled compaction. The base class interface needs to be
    // generalized to properly support different strategies.
    SimpleLeveledCompactionTask simple_task;
    simple_task.lower_level = 0; // Not applicable
    simple_task.upper_level = 0; // Not applicable
    simple_task.is_lower_level_bottom_level = tiered_task->bottom_tier_included;

    for (const auto& tier : tiered_task->tiers) {
        // HACK: Stuff all SSTs into the lower_level_sst_ids field
        // because SimpleLeveledCompactionTask cannot represent tiered compaction.
        simple_task.lower_level_sst_ids.insert(simple_task.lower_level_sst_ids.end(),
                                               tier.second.begin(),
                                               tier.second.end());
    }

    return simple_task;
}

std::optional<TieredCompactionTask> 
TieredCompactionController::GenerateTieredTask(const LsmStorageState& state) const {
    // In tiered compaction, L0 should be empty
    if (!state.l0_sstables.empty()) {
        std::cerr << "L0 should be empty in tiered compaction" << std::endl;
        return std::nullopt;
    }
    
    // Not enough tiers to compact
    if (state.levels.size() < options_.num_tiers) {
        return std::nullopt;
    }
    
    // Compaction triggered by space amplification ratio
    size_t size_except_bottom = 0;
    size_t bottom_size = 0;
    
    // Calculate sizes
    auto levels_vec = std::vector<std::pair<size_t, std::vector<size_t>>>(
        state.levels.begin(), state.levels.end());
    
    // Sort levels by tier ID
    std::sort(levels_vec.begin(), levels_vec.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });
    
    // Calculate size of all tiers except the bottom one
    if (levels_vec.size() > 1) {
        for (size_t i = 0; i < levels_vec.size() - 1; ++i) {
            size_except_bottom += levels_vec[i].second.size();
        }
    }
    
    // Calculate size of the bottom tier
    if (!levels_vec.empty()) {
        bottom_size = levels_vec.back().second.size();
    } else {
        return std::nullopt;  // No levels to compact
    }
    
    // Calculate space amplification ratio
    double space_amp_ratio = 0.0;
    if (bottom_size > 0) {
        space_amp_ratio = static_cast<double>(size_except_bottom) / bottom_size * 100.0;
    }
    
    // If space amplification ratio is too high, compact all tiers
    if (space_amp_ratio >= options_.max_size_amplification_percent) {
        std::cout << "compaction triggered by space amplification ratio: " 
                  << space_amp_ratio << "%" << std::endl;
        
        return TieredCompactionTask{
            .tiers = levels_vec,
            .bottom_tier_included = true
        };
    }
    
    // Compaction triggered by size ratio
    double size_ratio_trigger = (100.0 + options_.size_ratio) / 100.0;
    size_t size_so_far = 0;
    
    if (levels_vec.size() > 1) {
        for (size_t i = 0; i < levels_vec.size() - 1; ++i) {
            size_so_far += levels_vec[i].second.size();
            size_t next_level_size = levels_vec[i + 1].second.size();
            
            double current_size_ratio = 0.0;
            if (size_so_far > 0) {
                current_size_ratio = static_cast<double>(next_level_size) / size_so_far;
            }
            
            if (current_size_ratio > size_ratio_trigger && i + 1 >= options_.min_merge_width) {
                std::cout << "compaction triggered by size ratio: " 
                          << current_size_ratio * 100.0 << "% > " 
                          << size_ratio_trigger * 100.0 << "%" << std::endl;
                
                // Ensure we don't go out of bounds
                size_t end_idx = std::min(i + 1, levels_vec.size());
                return TieredCompactionTask{
                    .tiers = std::vector<std::pair<size_t, std::vector<size_t>>>(
                        levels_vec.begin(), levels_vec.begin() + end_idx),
                    .bottom_tier_included = (end_idx >= levels_vec.size())
                };
            }
        }
    }
    
    // Trying to reduce sorted runs without respecting size ratio
    if (levels_vec.empty()) {
        return std::nullopt;
    }
    
    size_t num_tiers_to_take = std::min(
        levels_vec.size(),
        options_.max_merge_width.value_or(std::numeric_limits<size_t>::max())
    );
    
    // Ensure num_tiers_to_take is valid
    if (num_tiers_to_take == 0 || num_tiers_to_take > levels_vec.size()) {
        return std::nullopt;
    }
    
    std::cout << "compaction triggered by reducing sorted runs" << std::endl;
    
    return TieredCompactionTask{
        .tiers = std::vector<std::pair<size_t, std::vector<size_t>>>(
            levels_vec.begin(), levels_vec.begin() + num_tiers_to_take),
        .bottom_tier_included = (levels_vec.size() <= num_tiers_to_take)
    };
}

std::pair<LsmStorageState, std::vector<size_t>>
TieredCompactionController::ApplyCompactionResult(
    const LsmStorageState& state,
    const TieredCompactionTask& task,
    const std::vector<size_t>& output_ssts) const {

    LsmStorageState new_state = state;
    std::vector<size_t> files_to_remove;

    std::unordered_set<size_t> task_tier_ids;
    size_t max_tier_id = 0;
    for (const auto& tier : task.tiers) {
        task_tier_ids.insert(tier.first);
        if (tier.first > max_tier_id) {
            max_tier_id = tier.first;
        }
    }

    std::vector<std::pair<size_t, std::vector<size_t>>> new_levels;
    for (const auto& level : state.levels) {
        if (task_tier_ids.count(level.first)) {
            // This tier was compacted, so its SSTs should be removed.
            files_to_remove.insert(files_to_remove.end(), level.second.begin(), level.second.end());
        } else {
            // This tier was not part of the compaction.
            new_levels.push_back(level);
        }
    }

    // Add the new merged tier.
    if (!output_ssts.empty()) {
        new_levels.emplace_back(max_tier_id, output_ssts);
    }

    // Keep the levels sorted by tier ID.
    std::sort(new_levels.begin(), new_levels.end());

    new_state.levels = std::move(new_levels);

    return {std::move(new_state), files_to_remove};
}

