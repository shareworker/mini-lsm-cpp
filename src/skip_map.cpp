#include "../include/skip_map.hpp"

#include <algorithm>
#include <utility>

namespace util {

// RangeIterator implementation
SkipMap::RangeIterator::RangeIterator(
    SkipMap::private_passkey,
    std::map<ByteBuffer, ByteBuffer>::const_iterator begin,
    std::map<ByteBuffer, ByteBuffer>::const_iterator end)
    : current_(begin), end_(end) {}

bool SkipMap::RangeIterator::Valid() const {
    return current_ != end_;
}

void SkipMap::RangeIterator::Next() {
    if (Valid()) {
        ++current_;
    }
}

const ByteBuffer& SkipMap::RangeIterator::Key() const {
    return current_->first;
}

const ByteBuffer& SkipMap::RangeIterator::Value() const {
    return current_->second;
}

// SkipMap implementation
void SkipMap::Insert(const ByteBuffer& key, const ByteBuffer& value) {
    map_[key] = value;
}

std::optional<ByteBuffer> SkipMap::Get(const ByteBuffer& key) const {
    auto it = map_.find(key);
    if (it != map_.end()) {
        return it->second;
    }
    return std::nullopt;
}

std::unique_ptr<SkipMap::RangeIterator> SkipMap::Range(
    const Bound& lower,
    const Bound& upper) const {
    
    // Determine lower bound iterator
    auto lower_it = map_.begin();
    switch (lower.GetType()) {
        case Bound::Type::kIncluded:
            if (lower.Key()) {
                lower_it = map_.lower_bound(*lower.Key());
            }
            break;
        case Bound::Type::kExcluded:
            if (lower.Key()) {
                lower_it = map_.upper_bound(*lower.Key());
            }
            break;
        case Bound::Type::kUnbounded:
            // Already set to begin()
            break;
    }
    
    // Determine upper bound iterator
    auto upper_it = map_.end();
    switch (upper.GetType()) {
        case Bound::Type::kIncluded:
            if (upper.Key()) {
                upper_it = map_.upper_bound(*upper.Key());
            }
            break;
        case Bound::Type::kExcluded:
            if (upper.Key()) {
                upper_it = map_.lower_bound(*upper.Key());
            }
            break;
        case Bound::Type::kUnbounded:
            // Already set to end()
            break;
    }
    
    return std::make_unique<RangeIterator>(private_passkey{},
                                            lower_it,
                                            upper_it);
}

} // namespace util
