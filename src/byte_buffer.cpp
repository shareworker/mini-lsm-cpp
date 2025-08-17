#include "../include/byte_buffer.hpp"

#include <algorithm>
#include <cstring>
#include <utility>

namespace util {

ByteBuffer::ByteBuffer()
    : data_(std::make_shared<std::vector<char>>()),
      offset_(0),
      size_(0) {
}

ByteBuffer::ByteBuffer(const char* cstr)
    : ByteBuffer(cstr, std::strlen(cstr)) {
}

ByteBuffer::ByteBuffer(const char* data, size_t size)
    : data_(std::make_shared<std::vector<char>>(data, data + size)),
      offset_(0),
      size_(size) {
}

ByteBuffer::ByteBuffer(const uint8_t* data, size_t size)
    : data_(std::make_shared<std::vector<char>>(reinterpret_cast<const char*>(data),
                                             reinterpret_cast<const char*>(data + size))),
      offset_(0),
      size_(size) {
}

ByteBuffer::ByteBuffer(const std::string& str)
    : data_(std::make_shared<std::vector<char>>(str.begin(), str.end())),
      offset_(0),
      size_(str.size()) {
}

ByteBuffer::ByteBuffer(std::string_view str)
    : data_(std::make_shared<std::vector<char>>(str.begin(), str.end())),
      offset_(0),
      size_(str.size()) {
}

ByteBuffer::ByteBuffer(const std::vector<uint8_t>& vec)
    : data_(std::make_shared<std::vector<char>>(
          reinterpret_cast<const char*>(vec.data()),
          reinterpret_cast<const char*>(vec.data() + vec.size()))),
      offset_(0),
      size_(vec.size()) {
}

ByteBuffer::ByteBuffer(const ByteBuffer& other)
    : data_(other.data_),
      offset_(other.offset_),
      size_(other.size_) {
}

ByteBuffer::ByteBuffer(ByteBuffer&& other) noexcept
    : data_(std::move(other.data_)),
      offset_(other.offset_),
      size_(other.size_) {
    other.offset_ = 0;
    other.size_ = 0;
}

ByteBuffer& ByteBuffer::operator=(const ByteBuffer& other) {
    if (this != &other) {
        data_ = other.data_;
        offset_ = other.offset_;
        size_ = other.size_;
    }
    return *this;
}

ByteBuffer& ByteBuffer::operator=(ByteBuffer&& other) noexcept {
    if (this != &other) {
        data_ = std::move(other.data_);
        offset_ = other.offset_;
        size_ = other.size_;
        
        other.offset_ = 0;
        other.size_ = 0;
    }
    return *this;
}

ByteBuffer::~ByteBuffer() = default;

const char* ByteBuffer::Data() const {
    if (!data_ || data_->empty()) {
        return nullptr;
    }
    return data_->data() + offset_;
}

size_t ByteBuffer::Size() const {
    return size_;
}

ByteBuffer ByteBuffer::Slice(size_t offset, size_t length) const {
    if (offset >= size_) {
        // Return empty buffer if offset is out of range
        return ByteBuffer();
    }
    
    // Adjust length if it would exceed the buffer's bounds
    length = std::min(length, size_ - offset);
    
    // Create a new buffer that shares the same underlying data
    return ByteBuffer(data_, offset_ + offset, length);
}

ByteBuffer ByteBuffer::Clone() const {
    // Create a deep copy of the buffer
    if (size_ == 0) {
        return ByteBuffer();
    }
    
    return ByteBuffer(Data(), size_);
}

std::string ByteBuffer::ToString() const {
    if (size_ == 0) {
        return std::string();
    }
    
    return std::string(Data(), size_);
}

bool ByteBuffer::operator==(const ByteBuffer& other) const {
    if (size_ != other.size_) {
        return false;
    }
    
    if (size_ == 0) {
        return true;
    }
    
    return std::memcmp(Data(), other.Data(), size_) == 0;
}

bool ByteBuffer::operator!=(const ByteBuffer& other) const {
    return !(*this == other);
}

bool ByteBuffer::operator<(const ByteBuffer& other) const {
    const size_t min_size = std::min(size_, other.size_);
    
    if (min_size == 0) {
        return size_ < other.size_;
    }
    
    int cmp = std::memcmp(Data(), other.Data(), min_size);
    
    if (cmp != 0) {
        return cmp < 0;
    }
    
    return size_ < other.size_;
}

bool ByteBuffer::operator>(const ByteBuffer& other) const {
    const size_t min_size = std::min(size_, other.size_);
    
    if (min_size == 0) {
        return size_ > other.size_;
    }
    
    int cmp = std::memcmp(Data(), other.Data(), min_size);
    
    if (cmp != 0) {
        return cmp > 0;
    }
    
    return size_ > other.size_;
}

ByteBuffer::ByteBuffer(std::shared_ptr<const std::vector<char>> data, size_t offset, size_t size)
    : data_(std::move(data)),
      offset_(offset),
      size_(size) {
}

}  // namespace util
