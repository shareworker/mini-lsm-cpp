#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <vector>


/**
 * @brief A reference-counted byte buffer with zero-copy operations
 * 
 * ByteBuffer provides a thread-safe, memory-efficient container for binary data
 * with copy-on-write semantics. It allows efficient slicing and cloning operations
 * that share the same underlying data via reference counting.
 */
class ByteBuffer {
public:
    /**
     * @brief Creates an empty buffer
     */
    ByteBuffer();

    /**
     * @brief Creates a buffer from raw bytes
     * 
     * @param data Pointer to the data
     * @param size Size of the data in bytes
     */
    ByteBuffer(const char* data, size_t size);

    /**
     * @brief Creates a buffer from a null-terminated C-string
     *
     * This convenience constructor allows direct construction from string
     * literals without requiring an explicit size argument and avoids the
     * previous overload ambiguity involving std::string and std::string_view.
     *
     * @param cstr Null-terminated C string
     */
    explicit ByteBuffer(const char* cstr);

    /**
     * @brief Creates a buffer from raw unsigned bytes
     * 
     * @param data Pointer to the unsigned byte data
     * @param size Size of the data in bytes
     */
    ByteBuffer(const uint8_t* data, size_t size);

    /**
     * @brief Creates a buffer from a string
     * 
     * @param str String to copy data from
     */
    explicit ByteBuffer(const std::string& str);

    /**
     * @brief Creates a buffer from a string view
     * 
     * @param str String view to copy data from
     */
    explicit ByteBuffer(std::string_view str);

    /**
     * @brief Creates a buffer from a vector of bytes
     * 
     * @param vec Vector to copy data from
     */
    explicit ByteBuffer(const std::vector<uint8_t>& vec);

    /**
     * @brief Copy constructor
     * 
     * Creates a new ByteBuffer that shares the same underlying data
     * 
     * @param other Buffer to copy from
     */
    ByteBuffer(const ByteBuffer& other);

    /**
     * @brief Move constructor
     * 
     * @param other Buffer to move from
     */
    ByteBuffer(ByteBuffer&& other) noexcept;

    /**
     * @brief Copy assignment operator
     * 
     * @param other Buffer to copy from
     * @return Reference to this buffer
     */
    ByteBuffer& operator=(const ByteBuffer& other);

    /**
     * @brief Move assignment operator
     * 
     * @param other Buffer to move from
     * @return Reference to this buffer
     */
    ByteBuffer& operator=(ByteBuffer&& other) noexcept;

    /**
     * @brief Destructor
     */
    ~ByteBuffer();

    /**
     * @brief Returns a pointer to the underlying data
     * 
     * @return Const pointer to the data
     */
    const char* Data() const;

    /**
     * @brief Returns the size of the buffer in bytes
     * 
     * @return Size in bytes
     */
    size_t Size() const;

    /**
     * @brief Creates a slice of the buffer
     * 
     * Creates a new ByteBuffer that shares the same underlying data
     * but only exposes a slice of it.
     * 
     * @param offset Offset from the start of the buffer
     * @param length Length of the slice
     * @return New ByteBuffer representing the slice
     */
    ByteBuffer Slice(size_t offset, size_t length) const;

    /**
     * @brief Creates a copy of the buffer
     * 
     * Unlike the copy constructor, this creates a deep copy with
     * its own underlying data.
     * 
     * @return New ByteBuffer with copied data
     */
    ByteBuffer Clone() const;

    /**
     * @brief Converts the buffer to a string
     * 
     * @return String containing a copy of the buffer data
     */
    std::string ToString() const;

    /**
     * @brief Compares two buffers for equality
     * 
     * @param other Buffer to compare with
     * @return true if buffers have identical content
     */
    bool operator==(const ByteBuffer& other) const;

    /**
     * @brief Compares two buffers for inequality
     * 
     * @param other Buffer to compare with
     * @return true if buffers have different content
     */
    bool operator!=(const ByteBuffer& other) const;

    /**
     * @brief Lexicographically compares this buffer with another
     * 
     * @param other Buffer to compare with
     * @return true if this buffer is less than the other
     */
    bool operator<(const ByteBuffer& other) const;

    /**
     * @brief Lexicographically compares this buffer with another
     * 
     * @param other Buffer to compare with
     * @return true if this buffer is greater than the other
     */
    bool operator>(const ByteBuffer& other) const;

    bool operator<=(const ByteBuffer& other) const { return !(*this > other); }
    bool operator>=(const ByteBuffer& other) const { return !(*this < other); }

    // -------- Added utility helpers --------
    /**
     * @brief Checks whether the buffer is empty.
     */
    bool Empty() const noexcept { return Size() == 0; }
    
    /**
     * @brief Compatibility method that delegates to Empty().
     * @return true if the buffer is empty (has size 0)
     */
    bool IsEmpty() const noexcept { return Empty(); }

    /**
     * @brief Reset the buffer to an empty state.
     *        After calling this, Empty() == true.
     */
    void Clear() noexcept {
        data_.reset();
        offset_ = 0;
        size_ = 0;
    }

    /**
     * @brief Copy content to a new std::vector<uint8_t>.
     *        This is equivalent to Rust's `to_vec()` for Bytes.
     */
    std::vector<uint8_t> CopyToBytes() const {
        const char* ptr = Data();
        return std::vector<uint8_t>(ptr, ptr + Size());
    }

private:
    // Private constructor for creating slices
    ByteBuffer(std::shared_ptr<const std::vector<char>> data, size_t offset, size_t size);

    // Shared data storage with reference counting
    std::shared_ptr<const std::vector<char>> data_;
    
    // Offset and size for slicing support
    size_t offset_;
    size_t size_;
};


namespace std {

/**
 * @brief Hash function for ByteBuffer
 * 
 * This allows ByteBuffer to be used as a key in unordered containers
 * like std::unordered_map and std::unordered_set.
 */
template <>
struct hash<ByteBuffer> {
    size_t operator()(const ByteBuffer& buf) const {
        return std::hash<std::string_view>{}(
            std::string_view(buf.Data(), buf.Size()));
    }
};

} // namespace std
