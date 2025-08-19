#pragma once

#include <memory>
#include <utility>

#include "storage_iterator.hpp"


/**
 * @brief Merge two sorted forward iterators into a single stream.
 *
 * If both iterators contain the same key, the entry from iterator `A` is
 * preferred and the duplicated key from iterator `B` is skipped.
 *
 * The behaviour mirrors Rust's `TwoMergeIterator`.
 */
class TwoMergeIterator : public StorageIterator {
public:
    TwoMergeIterator(std::unique_ptr<StorageIterator> a, std::unique_ptr<StorageIterator> b)
        : a_(std::move(a)), b_(std::move(b)) {
        SkipB();
        choose_a_ = ChooseA();
    }

    // Factory helper mirroring Rust's `create`.
    static std::unique_ptr<TwoMergeIterator> Create(
        std::unique_ptr<StorageIterator> a, 
        std::unique_ptr<StorageIterator> b) {
        return std::make_unique<TwoMergeIterator>(std::move(a), std::move(b));
    }

    // ---- StorageIterator interface implementation ----
    ByteBuffer Key() const noexcept override {
        return choose_a_ ? a_->Key() : b_->Key();
    }

    const ByteBuffer& Value() const noexcept override {
        return choose_a_ ? a_->Value() : b_->Value();
    }

    bool IsValid() const noexcept override {
        return choose_a_ ? a_->IsValid() : b_->IsValid();
    }

    void Next() noexcept override {
        if (choose_a_) {
            a_->Next();
        } else {
            b_->Next();
        }
        SkipB();
        choose_a_ = ChooseA();
    }

private:
    bool ChooseA() const noexcept {
        if (!a_->IsValid()) {
            return false;
        }
        if (!b_->IsValid()) {
            return true;
        }
        return a_->Key() < b_->Key();
    }

    void SkipB() noexcept {
        while (a_->IsValid() && b_->IsValid() && a_->Key() == b_->Key()) {
            b_->Next();
        }
    }

    std::unique_ptr<StorageIterator> a_;
    std::unique_ptr<StorageIterator> b_;
    bool choose_a_{false};
};

