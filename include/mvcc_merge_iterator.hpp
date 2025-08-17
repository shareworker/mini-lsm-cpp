#pragma once

#include <memory>
#include <vector>
#include "storage_iterator.hpp"
#include "byte_buffer.hpp"

namespace util {

/**
 * @brief MVCC-aware merge iterator for multiple versioned iterators.
 *
 * MvccMergeIterator merges multiple underlying iterators with timestamp awareness.
 * It ensures that only the newest version of each key (visible as of the read timestamp)
 * is returned during iteration.
 */
class MvccMergeIterator : public StorageIterator {
public:
    /**
     * @brief Creates a new MVCC-aware merge iterator from multiple iterators.
     *
     * @param iters Vector of iterators to merge
     * @param read_ts Read timestamp (only versions <= read_ts are visible)
     * @return MvccMergeIterator New merge iterator instance
     */
    static MvccMergeIterator Create(
        std::vector<std::unique_ptr<StorageIterator>> iters,
        uint64_t read_ts);
        
    /**
     * @brief Constructor that accepts multiple MVCC iterators and builds a merge tree.
     * 
     * @param iters Vector of MVCC iterators to merge
     */
    explicit MvccMergeIterator(std::vector<std::unique_ptr<StorageIterator>> iters);

    MvccMergeIterator(const MvccMergeIterator&) = delete;
    MvccMergeIterator& operator=(const MvccMergeIterator&) = delete;
    MvccMergeIterator(MvccMergeIterator&&) noexcept = default;
    MvccMergeIterator& operator=(MvccMergeIterator&&) noexcept = default;
    ~MvccMergeIterator() override = default;

    bool IsValid() const noexcept override;
    void Next() noexcept override;
    ByteBuffer Key() const noexcept override;
    const ByteBuffer& Value() const noexcept override;
    
    /**
     * @brief Seeks the iterator to the specified key.
     * 
     * @param key The key to seek to
     */
    void Seek(const ByteBuffer& key);
    
    /**
     * @brief Seeks to the first key in the iterator's range.
     */
    void SeekToFirst();

private:
    MvccMergeIterator(std::unique_ptr<StorageIterator> iter);

    /**
     * @brief Builds a balanced merge tree from a vector of iterators.
     * 
     * @param iters Vector of iterators to merge
     * @param read_ts Read timestamp (only versions <= read_ts are visible)
     * @return std::unique_ptr<StorageIterator> The root of the merge tree
     */
    static std::unique_ptr<StorageIterator> BuildMergeTree(
        std::vector<std::unique_ptr<StorageIterator>> iters,
        uint64_t read_ts);
        
    std::unique_ptr<StorageIterator> iter_;
};

} // namespace util
