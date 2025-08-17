#include "../include/mvcc_merge_iterator.hpp"
#include "../include/mvcc_sstable_iterator.hpp"
#include "../include/mvcc_two_merge_iterator.hpp" 
#include <memory>
#include <utility>
#include <vector>


namespace util {

MvccMergeIterator MvccMergeIterator::Create(
    std::vector<std::unique_ptr<StorageIterator>> iters,
    uint64_t read_ts) {
    // Remove any invalid iterators
    auto it = iters.begin();
    while (it != iters.end()) {
        if (!(*it)->IsValid()) {
            it = iters.erase(it);
        } else {
            ++it;
        }
    }
    
    // If no valid iterators, return an empty iterator
    if (iters.empty()) {
        return MvccMergeIterator(nullptr);
    }
    
    // If only one iterator, return it directly
    if (iters.size() == 1) {
        return MvccMergeIterator(std::move(iters[0]));
    }
    
    // Otherwise, build a balanced merge tree
    return MvccMergeIterator(BuildMergeTree(std::move(iters), read_ts));
}

MvccMergeIterator::MvccMergeIterator(std::unique_ptr<StorageIterator> iter)
    : iter_(std::move(iter)) {}

MvccMergeIterator::MvccMergeIterator(std::vector<std::unique_ptr<StorageIterator>> iters) {
    // Filter out invalid iterators
    auto it = iters.begin();
    while (it != iters.end()) {
        if (!(*it)->IsValid()) {
            it = iters.erase(it);
        } else {
            ++it;
        }
    }
    
    // If no valid iterators, leave iter_ as nullptr
    if (iters.empty()) {
        return;
    }
    
    // If only one iterator, use it directly
    if (iters.size() == 1) {
        iter_ = std::move(iters[0]);
        return;
    }
    
    // Otherwise, build a balanced merge tree (use 0 as read_ts since this constructor
    // doesn't specify timestamp - the caller must ensure proper timestamp filtering)
    iter_ = BuildMergeTree(std::move(iters), UINT64_MAX);
}

bool MvccMergeIterator::IsValid() const noexcept {
    return iter_ && iter_->IsValid();
}

void MvccMergeIterator::Next() noexcept {
    if (IsValid()) {
        iter_->Next();
    }
}

ByteBuffer MvccMergeIterator::Key() const noexcept {
    static const ByteBuffer kEmpty;
    return IsValid() ? iter_->Key() : kEmpty;
}

const ByteBuffer& MvccMergeIterator::Value() const noexcept {
    static const ByteBuffer kEmpty;
    return IsValid() ? iter_->Value() : kEmpty;
}

std::unique_ptr<StorageIterator> MvccMergeIterator::BuildMergeTree(
    std::vector<std::unique_ptr<StorageIterator>> iters,
    uint64_t read_ts) {
    // Base cases
    if (iters.empty()) {
        return nullptr;
    }
    
    if (iters.size() == 1) {
        return std::move(iters[0]);
    }
    
    if (iters.size() == 2) {
        return std::make_unique<MvccTwoMergeIterator>(
            MvccTwoMergeIterator::Create(
                std::move(iters[0]),
                std::move(iters[1]),
                read_ts));
    }
    
    // Recursive case: split the iterators in half and merge the results
    size_t mid = iters.size() / 2;
    
    // Create vectors for the two halves
    std::vector<std::unique_ptr<StorageIterator>> left_half;
    std::vector<std::unique_ptr<StorageIterator>> right_half;
    
    // Move first half to left_half
    for (size_t i = 0; i < mid; ++i) {
        left_half.push_back(std::move(iters[i]));
    }
    
    // Move second half to right_half
    for (size_t i = mid; i < iters.size(); ++i) {
        right_half.push_back(std::move(iters[i]));
    }
    
    // Recursively build merge trees for each half
    auto left_tree = BuildMergeTree(std::move(left_half), read_ts);
    auto right_tree = BuildMergeTree(std::move(right_half), read_ts);
    
    // Combine the two halves with a MvccTwoMergeIterator
    return std::make_unique<MvccTwoMergeIterator>(
        MvccTwoMergeIterator::Create(
            std::move(left_tree),
            std::move(right_tree),
            read_ts));
}

void MvccMergeIterator::Seek(const ByteBuffer& key) {
    if (!iter_) {
        return;
    }
    
    // Try to cast to different iterator types and call their Seek methods
    
    // Check if it's an MvccSsTableIterator
    if (auto* mvcc_sst_iter = dynamic_cast<MvccSsTableIterator*>(iter_.get())) {
        mvcc_sst_iter->Seek(key);
        return;
    }
    
    // MvccTwoMergeIterator doesn't have direct Seek support
    // Skip this case for now
    
    // Check if it's another MvccMergeIterator (recursive case)
    if (auto* mvcc_merge_iter = dynamic_cast<MvccMergeIterator*>(iter_.get())) {
        mvcc_merge_iter->Seek(key);
        return;
    }
    
    // Check for other specific iterator types as needed
    // MvccMemTableIterator would be handled here if it supported Seek
    
    // For other iterator types, we need to manually seek by iterating
    // This is a fallback for iterators that don't have native Seek support
    if (!iter_->IsValid()) {
        return;
    }
    
    // If the current key is >= target key, we're done
    if (iter_->Key() >= key) {
        return;
    }
    
    // Otherwise, iterate until we find a key >= target key
    while (iter_->IsValid() && iter_->Key() < key) {
        iter_->Next();
    }
}

void MvccMergeIterator::SeekToFirst() {
    if (!iter_) {
        return;
    }
    
    // Try to cast to different iterator types and call their SeekToFirst methods
    
    // Check if it's an MvccSsTableIterator
    if (auto* mvcc_sst_iter = dynamic_cast<MvccSsTableIterator*>(iter_.get())) {
        mvcc_sst_iter->SeekToFirst();
        return;
    }
    
    // MvccTwoMergeIterator doesn't have direct SeekToFirst support
    // Skip this case for now
    
    // Check if it's another MvccMergeIterator (recursive case)
    if (auto* mvcc_merge_iter = dynamic_cast<MvccMergeIterator*>(iter_.get())) {
        mvcc_merge_iter->SeekToFirst();
        return;
    }
    
    // Check for other specific iterator types as needed
    // MvccMemTableIterator would be handled here if it supported SeekToFirst
    
    // For other iterator types that might already be at the first position,
    // we assume they're already positioned correctly
    // This covers cases like iterators that are created in a "first" state
}

} // namespace util
