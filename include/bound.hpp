#pragma once

#include <optional>
#include "byte_buffer.hpp"

namespace util {

// Simple C++ counterpart of Rust's std::ops::Bound.
class Bound {
public:
    enum class Type { kUnbounded, kIncluded, kExcluded };

    static Bound Unbounded() { return Bound(Type::kUnbounded, std::nullopt); }
    static Bound Included(ByteBuffer key) { return Bound(Type::kIncluded, std::move(key)); }
    static Bound Excluded(ByteBuffer key) { return Bound(Type::kExcluded, std::move(key)); }

    Type GetType() const noexcept { return type_; }
    const std::optional<ByteBuffer>& Key() const noexcept { return key_; }
    
    /**
     * @brief Check if a key is contained within this bound
     * 
     * For unbounded, always returns true.
     * For included bound, returns true if key >= bound key
     * For excluded bound, returns true if key > bound key
     * 
     * @param key The key to check
     * @return true if the key is contained within this bound
     */
    bool Contains(const ByteBuffer& key) const {
        switch (type_) {
            case Type::kUnbounded:
                return true;
            case Type::kIncluded:
                return key >= *key_;
            case Type::kExcluded:
                return key > *key_;
        }
        // Should never reach here
        return false;
    }

private:
    Bound(Type t, std::optional<ByteBuffer> k) : type_(t), key_(std::move(k)) {}

    Type type_;
    std::optional<ByteBuffer> key_;
};

}  // namespace util
