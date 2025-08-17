#pragma once

// Mini-LSM MVCC Integration
// Copyright (c) 2025
// Licensed under Apache 2.0.
//
// This header integrates MVCC functionality with the MiniLsm class.

#include <memory>
#include <filesystem>

#include "lsm_storage.hpp"
#include "mini_lsm.hpp"
#include "mvcc.hpp"
#include "mvcc_txn.hpp"
#include "mvcc_transaction.hpp"

namespace util {

/**
 * @brief MVCC-enabled MiniLsm wrapper
 * 
 * This class extends MiniLsm with MVCC transaction support.
 */
class MiniLsmMvcc {
public:
    using Options = LsmStorageOptions;
    
    /**
     * @brief Create a new MiniLsmMvcc instance
     * 
     * @param options LSM options
     * @param path Database path
     * @return std::unique_ptr<MiniLsmMvcc> New MiniLsmMvcc instance
     */
    static std::unique_ptr<MiniLsmMvcc> Create(const Options& options, const std::filesystem::path& path);
    
    /**
     * @brief Construct a new MiniLsmMvcc object
     * 
     * @param path Database path
     * @param options LSM options
     */
    explicit MiniLsmMvcc(const std::filesystem::path& path, const Options& options);

    /**
     * @brief Destroy the MiniLsmMvcc object
     */
    ~MiniLsmMvcc();

    /**
     * @brief Create a new transaction with snapshot isolation
     * 
     * @return std::shared_ptr<MvccTransaction> New MVCC transaction
     */
    std::shared_ptr<MvccTransaction> NewTransaction();

    /**
     * @brief Create a new serializable transaction
     * 
     * @return std::shared_ptr<MvccTransaction> New MVCC transaction
     */
    std::shared_ptr<MvccTransaction> NewSerializableTransaction();

    /**
     * @brief Get the underlying MiniLsm object
     * 
     * @return std::shared_ptr<MiniLsm> MiniLsm instance
     */
    std::shared_ptr<MiniLsm> GetLsm();
    
    /**
     * @brief Get the MVCC storage for garbage collection
     * 
     * @return std::shared_ptr<MvccLsmStorage> MVCC storage instance
     */
    std::shared_ptr<MvccLsmStorage> GetMvccStorage();

    /**
     * @brief Close the database
     */
    void Close();

private:
    // MiniLsm instance
    std::shared_ptr<MiniLsm> lsm_;
    
    // MVCC inner
    std::shared_ptr<LsmMvccInner> mvcc_;
};

} // namespace util
