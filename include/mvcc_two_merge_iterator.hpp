#pragma once

#include <memory>
#include <utility>
#include "storage_iterator.hpp"
#include "byte_buffer.hpp"

namespace util {

/**
 * @brief MVCC-aware two-way merge iterator for versioned key-value storage.
 *
 * MvccTwoMergeIterator merges two underlying iterators with timestamp awareness.
 * When the same key is found in both iterators, the one with the higher timestamp is preferred.
 * This ensures that the newest version of each key (within the read timestamp visibility) is returned.
 */
class MvccTwoMergeIterator : public StorageIterator {
public:
    /**
     * @brief Creates a new MVCC-aware two-way merge iterator.
     *
     * @param iter1 First iterator (higher priority)
     * @param iter2 Second iterator (lower priority)
     * @param read_ts Read timestamp (only versions <= read_ts are visible)
     * @return MvccTwoMergeIterator New merge iterator instance
     */
    static MvccTwoMergeIterator Create(
        std::unique_ptr<StorageIterator> iter1,
        std::unique_ptr<StorageIterator> iter2,
        uint64_t read_ts);

    MvccTwoMergeIterator(const MvccTwoMergeIterator&) = delete;
    MvccTwoMergeIterator& operator=(const MvccTwoMergeIterator&) = delete;
    MvccTwoMergeIterator(MvccTwoMergeIterator&&) noexcept = default;
    MvccTwoMergeIterator& operator=(MvccTwoMergeIterator&&) noexcept = default;
    ~MvccTwoMergeIterator() override = default;

    bool IsValid() const noexcept override;
    void Next() noexcept override;
    ByteBuffer Key() const noexcept override;
    const ByteBuffer& Value() const noexcept override;

private:
    MvccTwoMergeIterator(
        std::unique_ptr<StorageIterator> iter1,
        std::unique_ptr<StorageIterator> iter2,
        uint64_t read_ts);

    /**
     * @brief Moves the current selected iterator forward and updates the state.
     */
    void ForwardCurrent() noexcept;

    /**
     * @brief Extracts the timestamp from a key in an iterator.
     * 
     * @param iter Iterator with the current key
     * @return uint64_t Timestamp extracted from the key
     */
    uint64_t ExtractTimestamp(const StorageIterator& iter) const noexcept;

    /**
     * @brief Extracts the user key from a composite key.
     * 
     * @param composite_key Key containing user key and timestamp
     * @return ByteBuffer User key portion
     */
    ByteBuffer ExtractUserKey(const ByteBuffer& composite_key) const noexcept;

    /**
     * @brief Updates which iterator is currently selected based on their keys.
     * 
     * This method examines both iterators and selects the one with the lower key,
     * or if keys are equal, the one with the higher timestamp (newer version).
     */
    void UpdateCurrent() noexcept;

    std::unique_ptr<StorageIterator> iter1_;
    std::unique_ptr<StorageIterator> iter2_;
    uint64_t read_ts_;
    int current_{0};  // 0: invalid, 1: iter1, 2: iter2
    ByteBuffer last_key_;  // Last returned key to avoid duplicates
    bool has_last_key_{false};
};

} // namespace util
