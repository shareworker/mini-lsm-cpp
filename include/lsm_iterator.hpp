#pragma once

#include <memory>

#include "storage_iterator.hpp"
#include "bound.hpp"

namespace util {

/**
 * @brief Generic LSM iterator wrapper that enforces end bound and filters delete
 *        tombstones (empty value) similar to Rust's `LsmIterator`.
 *
 * Template parameter `InnerIter` must implement `StorageIterator`.
 */
template <typename InnerIter>
class LsmIterator : public StorageIterator {
    static_assert(std::is_base_of_v<StorageIterator, InnerIter>,
                  "InnerIter must implement StorageIterator");

public:
    using InnerPtr = std::unique_ptr<InnerIter>;

    LsmIterator(InnerPtr inner, Bound end_bound) noexcept
        : inner_(std::move(inner)), end_bound_(std::move(end_bound)) {
        is_valid_ = inner_ && inner_->IsValid();
        MoveToNonDelete();
    }

    bool IsValid() const noexcept override { return is_valid_; }

    ByteBuffer Key() const noexcept override {
        static const ByteBuffer kEmpty;
        return is_valid_ ? inner_->Key() : kEmpty;
    }

    const ByteBuffer &Value() const noexcept override {
        static const ByteBuffer kEmpty;
        return is_valid_ ? inner_->Value() : kEmpty;
    }

    void Next() noexcept override {
        if (!is_valid_) {
            return;
        }
        NextInner();
        MoveToNonDelete();
    }

private:
    void NextInner() noexcept {
        inner_->Next();
        if (!inner_->IsValid()) {
            is_valid_ = false;
            return;
        }
        switch (end_bound_.GetType()) {
            case Bound::Type::kUnbounded:
                break;
            case Bound::Type::kIncluded:
                is_valid_ = inner_->Key() <= *end_bound_.Key();
                break;
            case Bound::Type::kExcluded:
                is_valid_ = inner_->Key() < *end_bound_.Key();
                break;
        }
    }

    void MoveToNonDelete() noexcept {
        while (is_valid_ && inner_->Value().Empty()) {
            NextInner();
        }
    }

    InnerPtr inner_;
    Bound end_bound_;
    bool is_valid_{false};
};

}  // namespace util
