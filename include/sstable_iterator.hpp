#pragma once

#include <memory>
#include "block_iterator.hpp"
#include "storage_iterator.hpp"
#include "sstable.hpp"
#include "bound.hpp"

// Forward declaration; full definition provided in `sstable.hpp` (future).
class SsTable;

/**
 * @brief Iterator over the key-value pairs of a single SSTable.
 *
 * Analogous to Rust's `SsTableIterator` implementation.  The iterator keeps
 * an index of the current block and a `BlockIterator` for fast in-block
 * traversal.  When the in-block iterator becomes invalid we seamlessly
 * advance to the next block.
 */
class SsTableIterator final : public StorageIterator {
public:
    using SsTablePtr = std::shared_ptr<SsTable>;

    // Disable copy; allow move.
    SsTableIterator(const SsTableIterator&) = delete;
    SsTableIterator& operator=(const SsTableIterator&) = delete;
    SsTableIterator(SsTableIterator&&) noexcept = default;
    SsTableIterator& operator=(SsTableIterator&&) noexcept = default;

    /**
     * Create a new iterator positioned at the first key of the table.
     */
    static SsTableIterator CreateAndSeekToFirst(SsTablePtr table) {
        auto [blk_idx, blk_iter] = SeekToFirstInner(table);
        return SsTableIterator(std::move(table), blk_idx, std::move(blk_iter));
    }

    /**
     * Create a new iterator positioned at the first key that is >= `target`.
     */
    static SsTableIterator CreateAndSeekToKey(SsTablePtr table, const Bound& key_bound) {
        auto [blk_idx, blk_iter] = SeekToKeyInner(table, key_bound);
        return SsTableIterator(std::move(table), blk_idx, std::move(blk_iter));
    }

    /** Seek to first key. */
    void SeekToFirst() {
        auto [blk_idx, blk_iter] = SeekToFirstInner(table_);
        blk_idx_ = blk_idx;
        blk_iter_ = std::move(blk_iter);
    }

    /** Seek to first key >= `target`. */
    void SeekToKey(const Bound& key_bound) {
        auto [blk_idx, blk_iter] = SeekToKeyInner(table_, key_bound);
        blk_idx_ = blk_idx;
        blk_iter_ = std::move(blk_iter);
    }

    // ---- StorageIterator interface implementation ----
    const ByteBuffer& Value() const noexcept override {
        value_cache_ = blk_iter_.Value();
        return value_cache_;
    }

    ByteBuffer Key() const noexcept override {
        return blk_iter_.Key();
    }

    bool IsValid() const noexcept override { return blk_iter_.IsValid(); }

    void Next() noexcept override {
        blk_iter_.Next();
        if (!blk_iter_.IsValid()) {
            ++blk_idx_;
            // Assume SsTable::NumOfBlocks() returns total block count.
            if (blk_idx_ < table_->NumOfBlocks()) {
                blk_iter_ = BlockIterator::CreateAndSeekToFirst(table_->ReadBlockCached(blk_idx_));
            }
        }
    }

private:
    SsTableIterator(SsTablePtr table, size_t blk_idx, BlockIterator blk_iter)
        : table_(std::move(table)), blk_iter_(std::move(blk_iter)), blk_idx_(blk_idx) {}

    // Helper: seek to first entry.
    static std::pair<size_t, BlockIterator> SeekToFirstInner(const SsTablePtr& table) {
        constexpr size_t kFirstIdx = 0;
        auto blk = table->ReadBlockCached(kFirstIdx);
        BlockIterator iter = BlockIterator::CreateAndSeekToFirst(std::move(blk));
        return {kFirstIdx, std::move(iter)};
    }

    // Helper: seek to first key >= target.
    static std::pair<size_t, BlockIterator> SeekToKeyInner(const SsTablePtr& table,
                                                           const Bound& key_bound) {
        if (key_bound.GetType() == Bound::Type::kUnbounded) {
            return SeekToFirstInner(table);
        }

        const auto& target = *key_bound.Key();
        size_t blk_idx = table->FindBlockIdx(target);
        BlockIterator blk_iter = BlockIterator::CreateAndSeekToKey(table->ReadBlockCached(blk_idx),
                                                                   target);
        if (!blk_iter.IsValid()) {
            ++blk_idx;
            if (blk_idx < table->NumOfBlocks()) {
                blk_iter = BlockIterator::CreateAndSeekToFirst(table->ReadBlockCached(blk_idx));
            }
        }

        // Now handle kExcluded
        if (key_bound.GetType() == Bound::Type::kExcluded && blk_iter.IsValid() && blk_iter.Key() == target) {
            blk_iter.Next();
            if (!blk_iter.IsValid()) {
                ++blk_idx;
                if (blk_idx < table->NumOfBlocks()) {
                    blk_iter = BlockIterator::CreateAndSeekToFirst(table->ReadBlockCached(blk_idx));
                }
            }
        }

        return {blk_idx, std::move(blk_iter)};
    }

    SsTablePtr table_;
    BlockIterator blk_iter_;
    size_t blk_idx_{0};
    mutable ByteBuffer value_cache_;
};

