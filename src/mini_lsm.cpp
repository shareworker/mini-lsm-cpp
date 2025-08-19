#include "mini_lsm.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>


/* static */ MiniLsm::Ptr MiniLsm::Open(const std::filesystem::path& path,
                                       const LsmStorageOptions& opts) {
    auto inner = std::filesystem::exists(path) ?
                 LsmStorageInner::Open(path, opts) :
                 LsmStorageInner::Create(path, opts);
    return Ptr(new MiniLsm(std::move(inner), opts));
}

MiniLsm::MiniLsm(std::unique_ptr<LsmStorageInner> inner,
                 const LsmStorageOptions& opts)
    : inner_(std::move(inner)), opts_(opts) {
    // Launch background compaction thread
    compaction_thread_ = std::thread([this]() { CompactionWorker(); });
    // Launch background flush thread
    flush_thread_ = std::thread([this]() { FlushWorker(); });
}

MiniLsm::~MiniLsm() {
    Close();
}

bool MiniLsm::Put(const ByteBuffer& key, const ByteBuffer& value) {
    bool ok = inner_->Put(key, value);
    NotifyFlush();
    return ok;
}

bool MiniLsm::Delete(const ByteBuffer& key) {
    bool ok = inner_->Delete(key);
    NotifyFlush();
    return ok;
}

std::optional<ByteBuffer> MiniLsm::Get(const ByteBuffer& key) const {
    return inner_->Get(key);
}

void MiniLsm::NotifyCompaction() {
    {
        std::lock_guard<std::mutex> lk(compaction_mtx_);
        compaction_notified_ = true;
    }
    compaction_cv_.notify_one();
}

void MiniLsm::NotifyFlush() {
    {
        std::lock_guard<std::mutex> lk(flush_mtx_);
        flush_notified_ = true;
    }
    flush_cv_.notify_one();
}

void MiniLsm::CompactionWorker() {
    while (!shutting_down_.load()) {
        std::unique_lock<std::mutex> lk(compaction_mtx_);
        compaction_cv_.wait(lk, [this]() { return compaction_notified_ || shutting_down_.load(); });
        if (shutting_down_.load()) break;
        compaction_notified_ = false;
        lk.unlock();
        inner_->Compact();
    }
}

void MiniLsm::FlushWorker() {
    while (!shutting_down_.load()) {
        std::unique_lock<std::mutex> lk(flush_mtx_);
        // Wait for either explicit notify (Put/Delete) or 1 second timeout so
        // that we periodically check memtable size even under low write load.
        flush_cv_.wait_for(lk, std::chrono::seconds(1), [this]() {
            return flush_notified_ || shutting_down_.load();
        });
        if (shutting_down_.load()) break;
        // reset flag
        flush_notified_ = false;
        lk.unlock();
        // Flush will internally freeze active memtable if non-empty and then
        // flush/compact imm memtables; this matches Rust behaviour.
        inner_->Flush();
    }
}

bool MiniLsm::Close() {
    bool already = shutting_down_.exchange(true);
    if (already) return true; // already closed

    // CRITICAL: Flush all pending memtable data BEFORE stopping threads
    // This ensures data persistence during shutdown/restart cycles
    if (!inner_->Flush()) {
        // Continue with close process even if flush fails to avoid hangs
        // In production, consider logging this error appropriately
    }
    
    // Sync directory after flush
    inner_->SyncDir();

    // Wake up background threads so they can exit
    compaction_cv_.notify_one();
    flush_cv_.notify_one();

    if (compaction_thread_.joinable()) compaction_thread_.join();
    if (flush_thread_.joinable()) flush_thread_.join();

    if (inner_->GetOptions()->enable_wal) {
        inner_->Sync();
        inner_->SyncDir();  
    }
    
    return true;
}

// SyncDir implementation moved to lsm_storage.cpp to avoid duplicate definition

