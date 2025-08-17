#include "../include/compaction_controller.hpp"

#include <memory>

#include "../include/lsm_storage.hpp"
#include "../include/leveled_compaction_controller.hpp"
#include "../include/simple_leveled_compaction_controller.hpp"
#include "../include/tiered_compaction_controller.hpp"

namespace util {

namespace {

// A trivial controller that performs no background compaction.
class NoCompactionController final : public CompactionController {
public:
    explicit NoCompactionController(const std::shared_ptr<LsmStorageOptions>& /*options*/) {}

    std::optional<SimpleLeveledCompactionTask> GenerateCompactionTask(
        const LsmStorageState& /*state*/) const override {
        return std::nullopt;
    }

    bool NeedsCompaction(const LsmStorageState& /*state*/) const override {
        // No compaction ever triggered.
        return false;
    }

    bool ApplyCompaction(
            LsmStorageState& /*state*/,
            const SimpleLeveledCompactionTask& /*task*/,
            const std::vector<size_t>& /*sst_ids*/,
            size_t& /*max_sst_id*/) override {
        // This should not be called for NoCompactionController.
        return false;
    }
};

} // namespace

// static
std::unique_ptr<CompactionController> CompactionController::Create(
    CompactionStrategy strategy, const std::shared_ptr<LsmStorageOptions>& options) {
    switch (strategy) {
        case CompactionStrategy::kNoCompaction:
            return std::make_unique<NoCompactionController>(options);
        case CompactionStrategy::kSimple:
            return std::make_unique<SimpleLeveledCompactionController>(options);
        case CompactionStrategy::kLeveled:
            return std::make_unique<LeveledCompactionController>(options);
        case CompactionStrategy::kTiered:
            return std::make_unique<TieredCompactionController>(options);
        default:
            // Fall back to the no-op controller if strategy is unknown.
            return std::make_unique<NoCompactionController>(options);
    }
}

// Backward compatibility methods implementation
// Variant-based interface methods removed - using standard SimpleLeveledCompactionTask interface

} // namespace util
