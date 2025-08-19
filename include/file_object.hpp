#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>


/**
 * @brief Simple RAII wrapper over a POSIX file used by SsTable.
 *
 * It supports random read (`Read`), size query (`Size`) and two factory helpers
 * `Create` (create-and-write) and `Open` (read-only).
 */
class FileObject {
public:
    // Default ctor creates an invalid handle.
    FileObject() noexcept = default;

    // Move-only (copy disabled)
    FileObject(const FileObject&) = delete;
    FileObject& operator=(const FileObject&) = delete;
    FileObject(FileObject&& other) noexcept = default;
    FileObject& operator=(FileObject&& other) noexcept = default;

    ~FileObject() = default;

    /**
     * @brief Create a brand-new file at `path` with content `data`.
     *        If the file exists it will be truncated.
     */
    static FileObject Create(const std::string& path, const std::vector<uint8_t>& data);

    /**
     * @brief Open an existing file as read-only.
     */
    static FileObject Open(const std::string& path);

    /**
     * @brief Read `len` bytes starting from `offset`.
     */
    std::vector<uint8_t> Read(uint64_t offset, uint64_t len) const;

    /**
     * @brief Total file size in bytes.
     */
    uint64_t Size() const noexcept;

    /**
     * @brief Whether the object is valid (file successfully opened/created).
     */
    bool Valid() const noexcept;

private:
    explicit FileObject(std::shared_ptr<struct FileObjectImpl> impl) noexcept : impl_(std::move(impl)) {}

    // PImpl pointer holding file descriptor and size.
    std::shared_ptr<struct FileObjectImpl> impl_;
};

