#pragma once

// Simple leveled compaction controller – C++ port of Rust simple_leveled.rs
// Copyright (c) 2025
// Licensed under Apache 2.0.

#include <cstddef>
#include <memory>
#include <optional>
#include <vector>

#include "compaction_controller.hpp"


// Forward declarations
class LsmStorageState;
class LsmStorageOptions;

// Simple leveled compaction options
struct SimpleLeveledCompactionOptions {
    size_t size_ratio_percent = 50; // Default: 50%
    size_t level0_file_num_compaction_trigger = 4; // Default: trigger at 4 files
    size_t max_levels = 7; // Default: 7 levels (L0-L6)
};

// Simple leveled compaction controller
class SimpleLeveledCompactionController : public CompactionController {
public:
    explicit SimpleLeveledCompactionController(const std::shared_ptr<LsmStorageOptions>& options);

    std::optional<SimpleLeveledCompactionTask> GenerateCompactionTask(
        const LsmStorageState& state) const override;

    bool NeedsCompaction(const LsmStorageState& state) const override;

    bool ApplyCompaction(
        LsmStorageState& state,
        const SimpleLeveledCompactionTask& task,
        const std::vector<size_t>& sst_ids,
        size_t& max_sst_id) override;

private:
    SimpleLeveledCompactionOptions options_;
};

