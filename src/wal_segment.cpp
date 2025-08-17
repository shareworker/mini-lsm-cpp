#include <fcntl.h>       // 添加，用于O_RDWR等文件操作标志

#include "../include/wal_segment.hpp"  // 修正路径
#include "../include/crc32c.hpp"      // 修正路径

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <ios>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <filesystem>
#include <memory>
#include <utility>
#include <vector>
#include <fstream>
#include <regex>

#ifdef __linux__
#include <unistd.h>  // for fsync
#include <fcntl.h>   // for open/close
#endif

namespace util {

namespace {

// WAL file format constants
constexpr const char* kWalPrefix = "wal-";
constexpr const char* kWalSuffix = ".wal";
constexpr size_t kWalHeaderSize = 16;  // 4 magic + 8 seq + 4 version

// Format: MAGIC(4) + SEQ(8) + VERSION(4)
struct WalHeader {
    uint32_t magic;
    uint64_t sequence;
    uint32_t version;

    static constexpr uint32_t kMagic = 0x4C41574D;  // "MWAL" in little-endian
    static constexpr uint32_t kVersion = 1;

    bool IsValid() const {
        return magic == kMagic && version == kVersion;
    }

    static WalHeader Create(uint64_t seq) {
        return {kMagic, seq, kVersion};
    }
};

// CRC32 checksum calculation for data integrity
uint32_t CalculateChecksum(const std::vector<uint8_t>& data) {
    // Make sure tables are initialized
    static bool initialized = false;
    if (!initialized) {
        util::Crc32c::Initialize();
        initialized = true;
    }
    return util::Crc32c::Compute(data);
}

// Utility functions for byte manipulation
void AppendUint16(std::vector<uint8_t>& buffer, uint16_t value) {
    buffer.push_back(static_cast<uint8_t>(value & 0xFF));
    buffer.push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
}

void AppendUint32(std::vector<uint8_t>& buffer, uint32_t value) {
    buffer.push_back(static_cast<uint8_t>(value & 0xFF));
    buffer.push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
    buffer.push_back(static_cast<uint8_t>((value >> 16) & 0xFF));
    buffer.push_back(static_cast<uint8_t>((value >> 24) & 0xFF));
}

void AppendBuffer(std::vector<uint8_t>& buffer, const ByteBuffer& data) {
    const uint8_t* data_ptr = reinterpret_cast<const uint8_t*>(data.Data());
    buffer.insert(buffer.end(), data_ptr, data_ptr + data.Size());
}

uint16_t ReadUint16(const uint8_t* data) {
    return static_cast<uint16_t>(data[0]) | 
           (static_cast<uint16_t>(data[1]) << 8);
}

uint32_t ReadUint32(const uint8_t* data) {
    return static_cast<uint32_t>(data[0]) | 
           (static_cast<uint32_t>(data[1]) << 8) |
           (static_cast<uint32_t>(data[2]) << 16) |
           (static_cast<uint32_t>(data[3]) << 24);
}

}  // anonymous namespace

WalSegment::WalSegment(
    const std::filesystem::path& dir,
    const std::string& name,
    const Options& options)
    : options_(options),
      dir_(dir),
      name_(name) {
}

WalSegment::~WalSegment() {
    // Ensure file is properly closed
    std::lock_guard<std::mutex> lock(file_mutex_);
    if (file_ && file_->is_open()) {
        file_->close();
    }
}

std::unique_ptr<WalSegment> WalSegment::Create(
    const std::filesystem::path& dir,
    const std::string& name,
    const Options& options) {
    
    try {
        // Create directory if it doesn't exist
        std::filesystem::create_directories(dir);
        
        // Create WalSegment instance
        auto wal = std::unique_ptr<WalSegment>(new WalSegment(dir, name, options));
        
        // Initialize first segment
        if (!wal->InitNewSegment()) {
            return nullptr;
        }
        
        return wal;
    } catch (const std::exception& e) {
        // Log error and return nullptr on failure
        return nullptr;
    }
}

std::unique_ptr<WalSegment> WalSegment::Recover(
    const std::filesystem::path& dir,
    const std::string& name,
    std::shared_ptr<SkipList<ByteBuffer, ByteBuffer>> skiplist,
    const Options& options) {
    
    try {
        // Create directory if it doesn't exist
        std::filesystem::create_directories(dir);
        
        // Create WalSegment instance
        auto wal = std::unique_ptr<WalSegment>(new WalSegment(dir, name, options));
        
        // Find all WAL segments
        std::cout << "[WAL Recovery] Looking for WAL segments in: " << dir << std::endl;
        auto segments = wal->ListSegments();
        std::cout << "[WAL Recovery] Found " << segments.size() << " WAL segments" << std::endl;
        
        // Sort segments by ID (oldest first)
        std::sort(segments.begin(), segments.end(),
            [](const auto& a, const auto& b) { return a.id < b.id; });
        
        // Store segments in the instance
        wal->segments_ = segments;
        
        // Set current segment ID to the highest found or 0
        wal->current_segment_id_ = segments.empty() ? 0 : segments.back().id;
        std::cout << "[WAL Recovery] Current segment ID: " << wal->current_segment_id_ << std::endl;
        
        // Recover data from each segment
        for (const auto& segment : segments) {
            std::cout << "[WAL Recovery] Processing segment: " << segment.path << std::endl;
            std::ifstream file(segment.path, std::ios::binary);
            if (!file.is_open()) {
                std::cout << "[WAL Recovery] Failed to open segment file" << std::endl;
                continue;
            }
            
            // Read and validate header
            WalHeader header;
            if (!file.read(reinterpret_cast<char*>(&header), sizeof(header)) ||
                !header.IsValid()) {
                std::cout << "[WAL Recovery] Invalid header in segment" << std::endl;
                continue;
            }
            
            // Get file size for buffer allocation
            file.seekg(0, std::ios::end);
            size_t file_size = file.tellg();
            file.seekg(sizeof(header), std::ios::beg);
            std::cout << "[WAL Recovery] File size: " << file_size << ", header size: " << sizeof(header) << std::endl;
            
            // Process records
            int record_count = 0;
            while (file.tellg() < file_size) {
                record_count++;
                auto current_pos = file.tellg();
                std::cout << "[WAL Recovery] Processing record #" << record_count << " at position " << current_pos << std::endl;
                
                // Read key length (4 bytes)
                uint32_t key_len;
                if (!file.read(reinterpret_cast<char*>(&key_len), sizeof(key_len))) {
                    std::cout << "[WAL Recovery] Failed to read key_len for record #" << record_count << std::endl;
                    break;
                }
                std::cout << "[WAL Recovery] Record #" << record_count << ": key_len=" << key_len << std::endl;
                
                // Read value length (4 bytes)
                uint32_t val_len;
                if (!file.read(reinterpret_cast<char*>(&val_len), sizeof(val_len))) {
                    std::cout << "[WAL Recovery] Failed to read val_len for record #" << record_count << std::endl;
                    break;
                }
                std::cout << "[WAL Recovery] Record #" << record_count << ": val_len=" << val_len << std::endl;
                
                // Allocate buffer for key and value
                std::vector<uint8_t> buffer(key_len + val_len);
                
                // Read key and value
                if (!file.read(reinterpret_cast<char*>(buffer.data()), buffer.size())) {
                    std::cout << "[WAL Recovery] Failed to read key+value data for record #" << record_count << std::endl;
                    break;
                }
                
                // Read checksum (4 bytes)
                uint32_t stored_checksum;
                if (!file.read(reinterpret_cast<char*>(&stored_checksum), sizeof(stored_checksum))) {
                    std::cout << "[WAL Recovery] Failed to read checksum for record #" << record_count << std::endl;
                    break;
                }
                std::cout << "[WAL Recovery] Record #" << record_count << ": stored_checksum=" << stored_checksum << std::endl;
                
                // Calculate checksum of the record
                std::vector<uint8_t> record_data;
                record_data.reserve(8 + key_len + val_len);
                AppendUint32(record_data, key_len);
                AppendUint32(record_data, val_len);
                record_data.insert(record_data.end(), buffer.begin(), buffer.end());
                uint32_t calculated_checksum = CalculateChecksum(record_data);
                std::cout << "[WAL Recovery] Record #" << record_count << ": calculated_checksum=" << calculated_checksum << std::endl;
                
                // Verify checksum
                if (calculated_checksum != stored_checksum) {
                    std::cout << "[WAL Recovery] CHECKSUM MISMATCH for record #" << record_count 
                              << ": stored=" << stored_checksum << ", calculated=" << calculated_checksum << std::endl;
                    // Log the raw data for debugging
                    std::cout << "[WAL Recovery] Raw record data (first 32 bytes): ";
                    for (size_t i = 0; i < std::min(record_data.size(), size_t(32)); i++) {
                        printf("%02x ", record_data[i]);
                    }
                    std::cout << std::endl;
                    // Checksum mismatch, stop recovery for this segment
                    break;
                }
                
                // Apply to skiplist
                util::Crc32c::Initialize();
                ByteBuffer key(buffer.data(), key_len);
                ByteBuffer value(buffer.data() + key_len, val_len);
                
                // IMPORTANT: Preserve the key format exactly as it was stored
                // This ensures MVCC versioned keys (user_key + timestamp) are preserved during recovery
                skiplist->Insert(key, value);
                
                // Log recovery of potentially versioned key for debugging
                if (key_len >= 8) {
                    std::cout << "[WAL Recovery] Recovered key of size " << key_len 
                              << ", potentially a versioned key" << std::endl;
                }
            }
        }
        
        // Initialize a new segment for writing
        if (!wal->InitNewSegment()) {
            return nullptr;
        }
        
        return wal;
    } catch (const std::exception& e) {
        // Log error and return nullptr on failure
        std::cerr << "WAL recovery error: " << e.what() << std::endl;
        return nullptr;
    }
}

bool WalSegment::InitNewSegment() {
    std::lock_guard<std::mutex> lock(file_mutex_);
    
    // Close current file if open
    if (file_ && file_->is_open()) {
        file_->close();
    }
    
    // Create new segment path
    auto path = MakeSegmentPath(current_segment_id_);
    
    try {
        // Open file for writing
        file_ = std::make_unique<std::fstream>();
        file_->exceptions(std::ios::failbit | std::ios::badbit);
        file_->open(path, std::ios::out | std::ios::binary | std::ios::trunc);
        
        if (!file_->is_open()) {
            return false;
        }
        
        // Write header
        WalHeader header = WalHeader::Create(current_segment_id_);
        file_->write(reinterpret_cast<const char*>(&header), sizeof(header));
        file_->flush();
        
        // Reset current size
        current_size_ = sizeof(header);
        
        // Add to segments list
        segments_.push_back({current_segment_id_, path, current_size_});
        
        return true;
    } catch (const std::exception& e) {
        // Log error and return false on failure
        return false;
    }
}

bool WalSegment::RotateSegment() {
    std::lock_guard<std::mutex> lock(file_mutex_);
    
    // Close current segment
    if (file_ && file_->is_open()) {
        file_->close();
    }
    
    // Increment segment ID
    current_segment_id_++;
    
    // Initialize new segment
    if (!InitNewSegment()) {
        return false;
    }
    
    // Clean up old segments
    CleanupSegments();
    
    return true;
}

size_t WalSegment::CleanupSegments() {
    std::lock_guard<std::mutex> lock(file_mutex_);
    
    if (segments_.size() <= options_.max_segments) {
        return 0;
    }
    
    // Sort segments by ID (oldest first)
    std::sort(segments_.begin(), segments_.end(), 
        [](const auto& a, const auto& b) { return a.id < b.id; });
    
    // Calculate how many segments to remove
    size_t segments_to_remove = segments_.size() - options_.max_segments;
    size_t removed = 0;
    
    // Remove oldest segments
    while (removed < segments_to_remove) {
        const auto& segment = segments_.front();
        
        // Don't remove current segment
        if (segment.id == current_segment_id_) {
            break;
        }
        
        // Remove file
        std::error_code ec;
        if (std::filesystem::remove(segment.path, ec)) {
            removed++;
        }
        
        // Remove from list
        segments_.erase(segments_.begin());
    }
    
    return removed;
}

std::filesystem::path WalSegment::CurrentSegmentPath() const {
    return MakeSegmentPath(current_segment_id_);
}

size_t WalSegment::SegmentCount() const {
    return segments_.size();
}

std::filesystem::path WalSegment::MakeSegmentPath(size_t segment_id) const {
    std::ostringstream ss;
    ss << kWalPrefix << name_ << "-" 
       << std::setw(16) << std::setfill('0') << segment_id
       << kWalSuffix;
    return dir_ / ss.str();
}

std::vector<WalSegment::SegmentInfo> WalSegment::ListSegments() const {
    std::vector<SegmentInfo> segments;
    
    // Create regex pattern for WAL files
    std::string pattern = std::string(kWalPrefix) + name_ + "-([0-9]+)" + kWalSuffix;
    std::regex wal_regex(pattern);
    
    try {
        // Iterate through directory
        for (const auto& entry : std::filesystem::directory_iterator(dir_)) {
            const auto& path = entry.path();
            std::string filename = path.filename().string();
            
            // Check if file matches pattern
            std::smatch match;
            if (std::regex_match(filename, match, wal_regex) && match.size() > 1) {
                try {
                    // Extract segment ID
                    size_t id = std::stoull(match[1]);
                    
                    // Get file size
                    size_t size = std::filesystem::file_size(path);
                    
                    // Add to segments
                    segments.push_back({id, path, size});
                } catch (...) {
                    // Skip invalid filenames
                    continue;
                }
            }
        }
    } catch (const std::exception& e) {
        // Log error and return empty list on failure
    }
    
    return segments;
}

bool WalSegment::Put(const ByteBuffer& key, const ByteBuffer& value) {
    // Check if we need to rotate
    if (current_size_ + key.Size() + value.Size() + 16 > options_.max_segment_size) {
        if (!RotateSegment()) {
            return false;
        }
    }
    
    // Format: [key_len(4)][value_len(4)][key][value][checksum(4)]
    std::vector<uint8_t> record;
    uint32_t key_len = key.Size();
    uint32_t val_len = value.Size();
    
    // Calculate record size
    size_t record_size = 4 + 4 + key_len + val_len + 4;
    record.reserve(record_size);
    
    // Append key length
    AppendUint32(record, key_len);
    
    // Append value length
    AppendUint32(record, val_len);
    
    // Append key
    AppendBuffer(record, key);
    
    // Append value
    AppendBuffer(record, value);
    
    // Calculate checksum (excluding the checksum field itself)
    uint32_t checksum = CalculateChecksum(record);
    
    // Append checksum
    AppendUint32(record, checksum);
    
    // Write record to file
    {
        std::lock_guard<std::mutex> lock(file_mutex_);
        
        if (!file_ || !file_->is_open()) {
            return false;
        }
        
        file_->write(reinterpret_cast<const char*>(record.data()), record.size());
        
        // Update current size
        current_size_ += record.size();
        
        // Update segment size
        for (auto& segment : segments_) {
            if (segment.id == current_segment_id_) {
                segment.size = current_size_;
                break;
            }
        }
        
        // Sync if configured
        if (options_.sync_on_write) {
            file_->flush();
            
            #ifdef __linux__
            if (file_->rdbuf() && file_->rdbuf()->is_open()) {
                // 调用Sync()方法进行完整的同步
                if (!Sync()) {
                    return false;
                }
            }
            #endif
        }
        
        return !file_->fail();
    }
}

bool WalSegment::PutBatch(const std::vector<std::pair<ByteBuffer, ByteBuffer>>& data) {
    // Calculate total size
    size_t total_size = 0;
    for (const auto& [key, value] : data) {
        total_size += key.Size() + value.Size() + 12;  // key_len + value_len + key + value + checksum
    }
    
    // Check if we need to rotate
    if (current_size_ + total_size > options_.max_segment_size) {
        if (!RotateSegment()) {
            return false;
        }
    }
    
    // Write all records
    for (const auto& [key, value] : data) {
        if (!Put(key, value)) {
            return false;
        }
    }
    
    return true;
}

bool WalSegment::Sync() {
    std::lock_guard<std::mutex> lock(file_mutex_);
    
    if (!file_ || !file_->is_open()) {
        return false;
    }
    
    file_->flush();
    
    #ifdef __linux__
    // Use platform-specific sync for durability
    if (file_->rdbuf() && file_->rdbuf()->is_open()) {
        // 刷新缓冲区
        file_->flush();
        
        // 在Linux上使用fsync持久化数据到磁盘
        // 我们需要获取文件描述符
        int fd = -1;
        
// 我们无法直接访问 std::filebuf 的文件描述符，因为它是受保护的
        // 所以我们始终使用备用方案
        
        // 如果无法获取描述符，我们需要重新打开文件
        if (fd < 0) {
            // 关闭现有文件
            file_->close();
            
            // 记住当前位置
            std::filesystem::path current_path = MakeSegmentPath(current_segment_id_);
            
            // 重新打开文件
            int open_fd = ::open(current_path.c_str(), O_RDWR);
            if (open_fd < 0) {
                return false;
            }
            fd = open_fd;
            
            // 确保关闭文件描述符
            struct FdCloser {
                int fd;
                ~FdCloser() { if (fd >= 0) ::close(fd); }
            } fd_closer{fd};
            
            // fsync文件
            if (::fsync(fd) != 0) {
                return false;
            }
            
            // 重新打开原来的文件流
            file_ = std::make_unique<std::fstream>(current_path, std::ios::in | std::ios::out | std::ios::app);
            if (!file_->is_open()) {
                return false;
            }
            
            return true;
        }
        if (fsync(fd) != 0) {
            return false;
        }
    }
    #endif
    
    return !file_->fail();
}

}  // namespace util
