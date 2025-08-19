#pragma once

#include <condition_variable>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

#include "byte_buffer.hpp"
#include "lsm_storage.hpp"


/**
 * @brief High-level wrapper of `LsmStorageInner`, mirroring Rust `MiniLsm`.
 *
 * Provides a simplified user-facing API and manages background flush/
 * compaction worker threads. Many advanced features (week-2 tasks in the
 * original Rust course) are stubbed out for now but the structure allows
 * plug-in later.
 */
class MiniLsm : public std::enable_shared_from_this<MiniLsm> {
public:
    using Ptr = std::shared_ptr<MiniLsm>;

    /**
     * @brief Open or create an LSM instance at path.
     */
    static Ptr Open(const std::filesystem::path& path,
                                  const LsmStorageOptions& opts = {});

    // Key-value operations (delegate to inner).
    bool Put(const ByteBuffer& key, const ByteBuffer& value);
    bool Delete(const ByteBuffer& key);
    std::optional<ByteBuffer> Get(const ByteBuffer& key) const;

    // Flush background workers and close.
    bool Close();

    // For testing purposes only: access the inner storage
    LsmStorageInner* GetInner() const { return inner_.get(); }

    // Shared-pointer alias to inner storage (does not increase ownership)
    std::shared_ptr<LsmStorageInner> GetStorage() {
        return std::shared_ptr<LsmStorageInner>(shared_from_this(), inner_.get());
    }

    ~MiniLsm();

private:
    explicit MiniLsm(std::unique_ptr<LsmStorageInner> inner,
                     const LsmStorageOptions& opts);

    // Worker helpers
    void CompactionWorker();
    void FlushWorker();
    void NotifyCompaction();
    void NotifyFlush();

    std::unique_ptr<LsmStorageInner> inner_;

    // Flush / compaction signalling primitives.
    std::mutex compaction_mtx_;
    std::condition_variable compaction_cv_;
    bool compaction_notified_ = false;
    std::thread compaction_thread_;

    std::mutex flush_mtx_;
    std::condition_variable flush_cv_;
    bool flush_notified_ = false;
    std::thread flush_thread_;

    // Options copy for workers.
    LsmStorageOptions opts_;

    // Control flag to stop threads.
    std::atomic<bool> shutting_down_{false};
};

