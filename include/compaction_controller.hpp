#pragma once

#include <memory>
#include <vector>
#include <optional>

#include "options.hpp"
#include "compaction.hpp"

namespace util {

// Forward declarations
class LsmStorageState;
class LsmStorageOptions;

class CompactionController {
public:
    virtual ~CompactionController() = default;

    static std::unique_ptr<CompactionController> Create(
        CompactionStrategy strategy,
        const std::shared_ptr<LsmStorageOptions>& options);

    // Generate a compaction task if needed
    virtual std::optional<SimpleLeveledCompactionTask> GenerateCompactionTask(
        const LsmStorageState& state) const = 0;

    // Check if compaction is needed
    virtual bool NeedsCompaction(const LsmStorageState& state) const = 0;

    // Apply compaction result to the state
    virtual bool ApplyCompaction(
            LsmStorageState& state,
            const SimpleLeveledCompactionTask& task,
            const std::vector<size_t>& sst_ids,
            size_t& max_sst_id) = 0;
};

} // namespace util
