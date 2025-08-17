#pragma once

#include <cassert>
#include <cstddef>
#include <memory>
#include <utility>
#include <vector>
#include <optional>

#include "byte_buffer.hpp"
#include "storage_iterator.hpp"
#include "sstable_iterator.hpp"
#include "sstable.hpp"
#include "bound.hpp"

namespace util {

/**
 * @brief Concatenate multiple `SsTableIterator` instances whose key ranges do
 *        NOT overlap and are ordered by key.
 *
 * This is the C++ port of Rust's `SstConcatIterator`.  To minimise iterator
 * creation cost we create the underlying iterator lazily – only the current
 * iterator is kept alive.
 */
class SstConcatIterator final : public StorageIterator {
public:
    using SsTablePtr = std::shared_ptr<SsTable>;

    // Disable copy; allow move.
    SstConcatIterator(const SstConcatIterator&) = delete;
    SstConcatIterator& operator=(const SstConcatIterator&) = delete;
    SstConcatIterator(SstConcatIterator&&) noexcept = default;
    SstConcatIterator& operator=(SstConcatIterator&&) noexcept = default;

    /**
     * Create iterator positioned at the first key of the first SSTable.
     */
    static SstConcatIterator CreateAndSeekToFirst(std::vector<SsTablePtr> sstables) {
        CheckSstValid(sstables);
        if (sstables.empty()) {
            return SstConcatIterator(std::move(sstables));
        }
        SstConcatIterator it(std::move(sstables));
        it.current_.emplace(SsTableIterator::CreateAndSeekToFirst(it.sstables_[0]));
        it.next_sst_idx_ = 1;
        it.MoveUntilValid();
        return it;
    }

    /**
     * Create iterator positioned at the first key that is >= `target`.
     */
    static SstConcatIterator CreateAndSeekToKey(std::vector<SsTablePtr> sstables,
                                               const Bound& key_bound) {
        CheckSstValid(sstables);
        if (key_bound.GetType() == Bound::Type::kUnbounded) {
            return CreateAndSeekToFirst(std::move(sstables));
        }
        if (sstables.empty()) {
            return SstConcatIterator(std::move(sstables));
        }

        const auto& target = *key_bound.Key();
        // Find the last SSTable with first_key <= target (saturating).
        size_t idx = 0;
        while (idx < sstables.size() && !(target < sstables[idx]->FirstKey())) {
            ++idx;
        }
        if (idx == 0) {
            // target < first_table.first_key. Use first table.
            idx = 0;
        } else {
            idx -= 1;
        }
        if (idx >= sstables.size()) {
            return SstConcatIterator(std::move(sstables));
        }
        SstConcatIterator it(std::move(sstables));
        it.current_.emplace(SsTableIterator::CreateAndSeekToKey(it.sstables_[idx], key_bound));
        it.next_sst_idx_ = idx + 1;
        it.MoveUntilValid();
        return it;
    }

    // ---- StorageIterator interface implementation ----
    const ByteBuffer& Value() const noexcept override {
        return current_.value().Value();
    }

    ByteBuffer Key() const noexcept override {
        return current_.value().Key();
    }

    bool IsValid() const noexcept override {
        return current_.has_value() && current_.value().IsValid();
    }

    void Next() noexcept override {
        assert(IsValid());
        current_.value().Next();
        MoveUntilValid();
    }

private:
    explicit SstConcatIterator(std::vector<SsTablePtr> sstables)
        : sstables_(std::move(sstables)) {}

    static void CheckSstValid(const std::vector<SsTablePtr>& sstables) {
        for (const auto& s : sstables) {
            assert(s->FirstKey() <= s->LastKey());
        }
        for (size_t i = 1; i < sstables.size(); ++i) {
            assert(sstables[i - 1]->LastKey() < sstables[i]->FirstKey());
        }
    }

    void MoveUntilValid() noexcept {
        while (current_.has_value() && !current_.value().IsValid()) {
            if (next_sst_idx_ >= sstables_.size()) {
                current_.reset();
                break;
            }
            current_.emplace(SsTableIterator::CreateAndSeekToFirst(sstables_[next_sst_idx_]));
            ++next_sst_idx_;
        }
    }

    std::vector<SsTablePtr> sstables_;
    std::optional<SsTableIterator> current_;
    size_t next_sst_idx_{0};
};

}  // namespace util
