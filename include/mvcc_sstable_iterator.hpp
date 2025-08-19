#pragma once

#include "storage_iterator.hpp"
#include "key_ts.hpp"
#include "sstable.hpp"
#include "block_iterator.hpp"
#include "bound.hpp"
#include <memory>


/**
 * @brief MVCC-aware SSTable iterator that provides timestamp filtering during iteration.
 * 
 * This iterator extends the standard SsTableIterator to support versioning and timestamp filtering.
 * It ensures that only versions visible as of a specific timestamp are returned when iterating.
 */
class MvccSsTableIterator : public StorageIterator {
public:
    /**
     * @brief Create a new MVCC-aware SSTable iterator with timestamp filtering.
     * 
     * @param sstable The SSTable to iterate over
     * @param read_ts The timestamp to read as of (only versions <= read_ts are visible)
     */
    static MvccSsTableIterator Create(std::shared_ptr<SsTable> sstable, uint64_t read_ts);

    /**
     * @brief Create a new MVCC-aware SSTable iterator with bounds and timestamp filtering.
     * 
     * @param sstable The SSTable to iterate over
     * @param lower_bound Lower bound for iteration
     * @param upper_bound Upper bound for iteration
     * @param read_ts The timestamp to read as of (only versions <= read_ts are visible)
     */
    static MvccSsTableIterator CreateWithBounds(
        std::shared_ptr<SsTable> sstable,
        const Bound& lower_bound,
        const Bound& upper_bound,
        uint64_t read_ts);
        
    MvccSsTableIterator(const MvccSsTableIterator&) = delete;
    MvccSsTableIterator& operator=(const MvccSsTableIterator&) = delete;
    MvccSsTableIterator(MvccSsTableIterator&&) noexcept = default;
    MvccSsTableIterator& operator=(MvccSsTableIterator&&) noexcept = default;
    ~MvccSsTableIterator() override = default;

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
    
    /**
     * @brief Returns the timestamp of the current key-value pair.
     * 
     * @return uint64_t The timestamp of the current entry
     */
    uint64_t Timestamp() const noexcept;

private:
    MvccSsTableIterator(
        std::shared_ptr<SsTable> sstable,
        const Bound& lower_bound,
        const Bound& upper_bound,
        uint64_t read_ts);

    /**
     * @brief Extracts a KeyTs (key + timestamp) from a raw key buffer.
     * 
     * @param raw_key The raw key buffer that contains both user key and timestamp
     * @return KeyTs The extracted composite key
     */
    KeyTs ExtractKeyTs(const ByteBuffer& raw_key) const;

    /**
     * @brief Extracts just the user key from a raw key buffer.
     * 
     * @param raw_key The raw key buffer that contains both user key and timestamp
     * @return ByteBuffer The extracted user key
     */
    ByteBuffer ExtractUserKey(const ByteBuffer& raw_key) const;

    /**
     * @brief Advances to the next valid entry that meets the timestamp criteria.
     */
    void FindNextValidEntry() noexcept;

    std::shared_ptr<SsTable> sstable_;
    std::shared_ptr<const Block> current_block_;
    BlockIterator block_iter_;
    uint64_t read_ts_;
    Bound lower_bound_;
    Bound upper_bound_;
    size_t current_block_idx_{0};
    ByteBuffer current_key_;       // Cached user key (without timestamp)
    ByteBuffer current_value_;     // Cached value
    ByteBuffer last_user_key_;     // Last seen user key (for skipping older versions)
    bool has_last_key_{false};     // Whether we have seen a user key before
    ByteBuffer empty_buffer_; // Empty buffer for invalid states
};

