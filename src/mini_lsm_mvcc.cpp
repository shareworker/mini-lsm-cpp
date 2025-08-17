#include "../include/mini_lsm_mvcc.hpp"

#include <memory>
#include <utility>
#include "../include/mvcc_transaction.hpp"
#include "../include/mvcc_lsm_storage.hpp"

namespace util {

std::unique_ptr<MiniLsmMvcc> MiniLsmMvcc::Create(const Options& options, const std::filesystem::path& path) {
    return std::make_unique<MiniLsmMvcc>(path, options);
}

MiniLsmMvcc::MiniLsmMvcc(const std::filesystem::path& path, const Options& options)
    : lsm_(MiniLsm::Open(path, options)),
      mvcc_(std::make_shared<LsmMvccInner>(1)) {
    // Set the MVCC instance on the storage so transactions can access MVCC functionality
    lsm_->GetStorage()->SetMvcc(mvcc_);
}

MiniLsmMvcc::~MiniLsmMvcc() {
    Close();
}

std::shared_ptr<MvccTransaction> MiniLsmMvcc::NewTransaction() {
    printf("[MVCC] MiniLsmMvcc::NewTransaction() called\n");
    fflush(stdout);
    
    auto txn = mvcc_->NewTxn(lsm_->GetStorage(), false);
    
    printf("[MVCC] MiniLsmMvcc::NewTransaction() - transaction created, type: %s\n", 
           typeid(*txn).name());
    fflush(stdout);
    
    return txn;
}

std::shared_ptr<MvccTransaction> MiniLsmMvcc::NewSerializableTransaction() {
    return mvcc_->NewTxn(lsm_->GetStorage(), true);
}

std::shared_ptr<MiniLsm> MiniLsmMvcc::GetLsm() {
    return lsm_;
}

std::shared_ptr<MvccLsmStorage> MiniLsmMvcc::GetMvccStorage() {
    // Create a MvccLsmStorage wrapper around the underlying storage
    auto storage_inner = lsm_->GetStorage();
    return MvccLsmStorage::CreateShared(storage_inner);
}

void MiniLsmMvcc::Close() {
    if (lsm_) {
        lsm_->Close();
    }
}

} // namespace util
