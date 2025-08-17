#pragma once

#include "storage_iterator.hpp"
#include "bound.hpp"
#include "mvcc_mem_table.hpp"
#include <memory>
#include <vector>
#include <map>
#include <unordered_set>

namespace util {

class SsTable;

/**
 * @brief MVCC-aware LSM iterator that handles timestamp-based versioning.
 *
 * This iterator provides a unified view over the entire LSM structure (memtables and SSTables),
 * with proper timestamp filtering to ensure correct MVCC snapshot isolation.
 */
class MvccLsmIterator : public StorageIterator {
public:
    /**
     * @brief Creates a new MVCC-aware LSM iterator with the specified bounds and read timestamp.
     *
     * @param memtable Active memtable
     * @param imm_memtables Immutable memtables
     * @param l0_sstables Level-0 SSTables
     * @param leveled_sstables Leveled SSTables (level > 0)
     * @param lower_bound Lower bound for iteration
     * @param upper_bound Upper bound for iteration
     * @param read_ts Read timestamp (only versions <= read_ts are visible)
     * @return MvccLsmIterator A new LSM iterator
     */
    static MvccLsmIterator Create(
        std::shared_ptr<MvccMemTable> memtable,
        const std::vector<std::shared_ptr<MvccMemTable>>& imm_memtables,
        const std::vector<std::shared_ptr<SsTable>>& l0_sstables,
        const std::vector<std::shared_ptr<SsTable>>& leveled_sstables,
        const Bound& lower_bound,
        const Bound& upper_bound,
        uint64_t read_ts);

    MvccLsmIterator(const MvccLsmIterator&) = delete;
    MvccLsmIterator& operator=(const MvccLsmIterator&) = delete;
    MvccLsmIterator(MvccLsmIterator&&) noexcept = default;
    MvccLsmIterator& operator=(MvccLsmIterator&&) noexcept = default;
    ~MvccLsmIterator() override = default;

    /**
     * @brief Constructor that accepts a StorageIterator to wrap and a read timestamp.
     * 
     * @param iter Iterator to wrap
     * @param read_ts Read timestamp (only versions <= read_ts are visible)
     */
    MvccLsmIterator(std::unique_ptr<StorageIterator> iter, uint64_t read_ts);
    
    /**
     * @brief Constructor that accepts just a StorageIterator to wrap.
     * 
     * @param iter Iterator to wrap
     */
    explicit MvccLsmIterator(std::unique_ptr<StorageIterator> iter);
    
    /**
     * @brief Seeks to the first key that is visible as of the read timestamp.
     * This method is called internally to skip versions that are not visible.
     */
    void SeekToVisibleVersion();
    
    bool IsValid() const noexcept override;
    void Next() noexcept override;
    ByteBuffer Key() const noexcept override;
    const ByteBuffer& Value() const noexcept override;

private:

    /**
     * @brief Creates iterators for all components of the LSM tree.
     *
     * @param memtable Active memtable
     * @param imm_memtables Immutable memtables
     * @param l0_sstables Level-0 SSTables
     * @param leveled_sstables Leveled SSTables (level > 0)
     * @param lower_bound Lower bound for iteration
     * @param upper_bound Upper bound for iteration
     * @param read_ts Read timestamp (only versions <= read_ts are visible)
     * @return std::vector<std::unique_ptr<StorageIterator>> Vector of iterators
     */
    static std::vector<std::unique_ptr<StorageIterator>> CreateComponentIterators(
        std::shared_ptr<MvccMemTable> memtable,
        const std::vector<std::shared_ptr<MvccMemTable>>& imm_memtables,
        const std::vector<std::shared_ptr<SsTable>>& l0_sstables,
        const std::vector<std::shared_ptr<SsTable>>& leveled_sstables,
        const Bound& lower_bound,
        const Bound& upper_bound,
        uint64_t read_ts);

    std::unique_ptr<StorageIterator> iter_;
    uint64_t read_ts_ = UINT64_MAX; // Default to maximum timestamp for latest version
    std::map<ByteBuffer, bool> visited_keys_; // Track keys we've already processed
};

} // namespace util
