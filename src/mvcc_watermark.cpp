#include "mvcc_watermark.hpp"


Watermark::Watermark() = default;

void Watermark::AddReader(uint64_t ts) {
    // Increment reader count for this timestamp
    readers_[ts]++;
}

void Watermark::RemoveReader(uint64_t ts) {
    // Decrement reader count for this timestamp
    auto it = readers_.find(ts);
    if (it != readers_.end()) {
        if (--it->second == 0) {
            // Remove entry if count reaches zero
            readers_.erase(it);
        }
    }
}

std::optional<uint64_t> Watermark::GetWatermark() const {
    // If no readers, return nullopt
    if (readers_.empty()) {
        return std::nullopt;
    }
    
    // Return the lowest timestamp that has active readers
    // This is the lowest key in the map (std::map is ordered)
    return readers_.begin()->first;
}

