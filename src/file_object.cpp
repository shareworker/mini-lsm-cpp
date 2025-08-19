#include "file_object.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <stdexcept>
#include <system_error>


namespace {
inline int OpenRO(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        throw std::system_error(errno, std::generic_category(), "open read-only failed");
    }
    return fd;
}

inline int CreateFile(const std::string& path, const std::vector<uint8_t>& data) {
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        throw std::system_error(errno, std::generic_category(), "create file failed");
    }
    ssize_t written = ::write(fd, data.data(), data.size());
    if (written < 0 || static_cast<size_t>(written) != data.size()) {
        ::close(fd);
        throw std::system_error(errno, std::generic_category(), "write file failed");
    }
    // reopen read-only for subsequent access
    ::close(fd);
    return OpenRO(path);
}

inline uint64_t FileSize(int fd) {
    struct stat st {};
    if (::fstat(fd, &st) != 0) {
        throw std::system_error(errno, std::generic_category(), "fstat failed");
    }
    return static_cast<uint64_t>(st.st_size);
}
}  // namespace

class FileHandle {
public:
    explicit FileHandle(int fd) : fd_(fd) {}
    ~FileHandle() {
        if (fd_ >= 0) ::close(fd_);
    }
    FileHandle(const FileHandle&) = delete;
    FileHandle& operator=(const FileHandle&) = delete;
    FileHandle(FileHandle&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }
    FileHandle& operator=(FileHandle&& other) noexcept {
        if (this != &other) {
            if (fd_ >= 0) ::close(fd_);
            fd_ = other.fd_;
            other.fd_ = -1;
        }
        return *this;
    }
    int Fd() const noexcept { return fd_; }
private:
    int fd_{-1};
};

struct FileObjectImpl {
    FileHandle handle;
    uint64_t size;
    
    FileObjectImpl(FileHandle h, uint64_t s) : handle(std::move(h)), size(s) {}
};


FileObject FileObject::Create(const std::string& path, const std::vector<uint8_t>& data) {
    int fd = CreateFile(path, data);
    uint64_t size = data.size();
    FileObject obj;
    obj.impl_ = std::make_shared<FileObjectImpl>(FileHandle(fd), size);
    return obj;
}

FileObject FileObject::Open(const std::string& path) {
    int fd = OpenRO(path);
    uint64_t size = FileSize(fd);
    FileObject obj;
    obj.impl_ = std::make_shared<FileObjectImpl>(FileHandle(fd), size);
    return obj;
}

std::vector<uint8_t> FileObject::Read(uint64_t offset, uint64_t len) const {
    if (!Valid()) {
        throw std::invalid_argument("invalid file object");
    }
    std::vector<uint8_t> buf(len);
    ssize_t n = ::pread(impl_->handle.Fd(), buf.data(), len, static_cast<off_t>(offset));
    if (n < 0 || static_cast<uint64_t>(n) != len) {
        throw std::system_error(errno, std::generic_category(), "pread failed");
    }
    return buf;
}

bool FileObject::Valid() const noexcept { return impl_ != nullptr; }
uint64_t FileObject::Size() const noexcept { return Valid() ? impl_->size : 0; }

