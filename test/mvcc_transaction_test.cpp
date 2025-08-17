#include <gtest/gtest.h>
#include "../include/mini_lsm_mvcc.hpp"
#include "../include/mvcc_transaction.hpp"
#include "../include/byte_buffer.hpp"
#include "../include/lsm_storage.hpp"
#include <filesystem>
#include <memory>
#include <string>
#include <chrono>

namespace util {

class MvccTransactionTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Use a unique directory for each test
        test_dir_ = std::filesystem::temp_directory_path() / 
                   ("mvcc_txn_test_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        
        // Clean up if exists
        if (std::filesystem::exists(test_dir_)) {
            std::filesystem::remove_all(test_dir_);
        }
        
        // Create test directory
        std::filesystem::create_directories(test_dir_);
        
        // Create MVCC storage with minimal options
        LsmStorageOptions options;
        options.enable_wal = false;  // Disable WAL for simplicity
        
        mvcc_storage_ = std::make_unique<MiniLsmMvcc>(test_dir_, options);
        ASSERT_NE(mvcc_storage_, nullptr);
    }
    
    void TearDown() override {
        mvcc_storage_.reset();
        
        // Clean up test directory
        if (std::filesystem::exists(test_dir_)) {
            std::filesystem::remove_all(test_dir_);
        }
    }
    
    std::filesystem::path test_dir_;
    std::unique_ptr<MiniLsmMvcc> mvcc_storage_;
};

// Test 1: Basic transaction lifecycle (Begin -> Put -> Commit)
TEST_F(MvccTransactionTest, BasicTransactionLifecycle) {
    // Begin a transaction
    auto txn = mvcc_storage_->NewTransaction();
    ASSERT_NE(txn, nullptr);
    
    // Put some data
    ByteBuffer key("test_key");
    ByteBuffer value("test_value");
    EXPECT_TRUE(txn->Put(key, value));
    
    // Commit the transaction
    EXPECT_TRUE(txn->Commit());
    
    // Verify data is visible outside transaction
    auto result = mvcc_storage_->GetLsm()->Get(key);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->ToString(), "test_value");
}

// Test 2: Transaction rollback (Begin -> Put -> Abort)
TEST_F(MvccTransactionTest, TransactionRollback) {
    // Begin a transaction
    auto txn = mvcc_storage_->NewTransaction();
    ASSERT_NE(txn, nullptr);
    
    // Put some data
    ByteBuffer key("rollback_key");
    ByteBuffer value("rollback_value");
    EXPECT_TRUE(txn->Put(key, value));
    
    // Abort the transaction
    txn->Abort();
    
    // Verify data is NOT visible outside transaction
    auto result = mvcc_storage_->GetLsm()->Get(key);
    EXPECT_FALSE(result.has_value());
}

// Test 3: Transaction isolation - reads don't see uncommitted writes
TEST_F(MvccTransactionTest, TransactionIsolation) {
    ByteBuffer key("isolation_key");
    ByteBuffer value1("value1");
    ByteBuffer value2("value2");
    
    // First, put initial value using a transaction to ensure MVCC consistency
    auto setup_txn = mvcc_storage_->NewTransaction();
    ASSERT_NE(setup_txn, nullptr);
    EXPECT_TRUE(setup_txn->Put(key, value1));
    EXPECT_TRUE(setup_txn->Commit());
    
    // Begin a transaction and modify the key
    auto txn = mvcc_storage_->NewTransaction();
    ASSERT_NE(txn, nullptr);
    EXPECT_TRUE(txn->Put(key, value2));
    
    // Outside the transaction, should still see old value
    auto result = mvcc_storage_->GetLsm()->Get(key);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->ToString(), "value1");
    
    // Inside the transaction, should see new value
    auto txn_result = txn->Get(key);
    EXPECT_EQ(txn_result.ToString(), "value2");
    
    // After commit, outside should see new value
    EXPECT_TRUE(txn->Commit());
    result = mvcc_storage_->GetLsm()->Get(key);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->ToString(), "value2");
}

// Test 4: Multiple operations in one transaction
TEST_F(MvccTransactionTest, MultipleOperationsInTransaction) {
    auto txn = mvcc_storage_->NewTransaction();
    ASSERT_NE(txn, nullptr);
    
    // Put multiple keys
    for (int i = 0; i < 5; ++i) {
        std::string key_str = "multi_key_" + std::to_string(i);
        std::string value_str = "multi_value_" + std::to_string(i);
        
        ByteBuffer key(key_str);
        ByteBuffer value(value_str);
        EXPECT_TRUE(txn->Put(key, value));
    }
    
    // Read back within transaction
    for (int i = 0; i < 5; ++i) {
        std::string key_str = "multi_key_" + std::to_string(i);
        std::string expected_value = "multi_value_" + std::to_string(i);
        
        ByteBuffer key(key_str);
        auto result = txn->Get(key);
        EXPECT_EQ(result.ToString(), expected_value);
    }
    
    // Commit
    EXPECT_TRUE(txn->Commit());
    
    // Verify all data is visible outside transaction
    for (int i = 0; i < 5; ++i) {
        std::string key_str = "multi_key_" + std::to_string(i);
        std::string expected_value = "multi_value_" + std::to_string(i);
        
        ByteBuffer key(key_str);
        auto result = mvcc_storage_->GetLsm()->Get(key);
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result->ToString(), expected_value);
    }
}

// Test 5: Transaction delete operations
TEST_F(MvccTransactionTest, TransactionDelete) {
    ByteBuffer key("delete_key");
    ByteBuffer value("delete_value");
    
    // First put some data using a transaction to ensure MVCC consistency
    auto setup_txn = mvcc_storage_->NewTransaction();
    ASSERT_NE(setup_txn, nullptr);
    EXPECT_TRUE(setup_txn->Put(key, value));
    EXPECT_TRUE(setup_txn->Commit());
    
    // Verify it exists
    auto result = mvcc_storage_->GetLsm()->Get(key);
    ASSERT_TRUE(result.has_value());
    
    // Delete in transaction
    auto txn = mvcc_storage_->NewTransaction();
    ASSERT_NE(txn, nullptr);
    EXPECT_TRUE(txn->Delete(key));
    
    // Outside transaction should still see the value
    result = mvcc_storage_->GetLsm()->Get(key);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->ToString(), "delete_value");
    
    // Inside transaction should not see the value
    auto txn_result = txn->Get(key);
    EXPECT_TRUE(txn_result.Empty());
    
    // Commit and verify deletion
    EXPECT_TRUE(txn->Commit());
    result = mvcc_storage_->GetLsm()->Get(key);
    EXPECT_FALSE(result.has_value());
}

// Test 6: Cannot operate on committed/aborted transactions
TEST_F(MvccTransactionTest, OperateOnInactiveTransaction) {
    auto txn = mvcc_storage_->NewTransaction();
    ASSERT_NE(txn, nullptr);
    
    ByteBuffer key("inactive_key");
    ByteBuffer value("inactive_value");
    
    // Commit the transaction
    EXPECT_TRUE(txn->Commit());
    
    // Should not be able to put after commit
    EXPECT_FALSE(txn->Put(key, value));
    
    // Should not be able to commit again
    EXPECT_FALSE(txn->Commit());
    
    // Test same with abort
    auto txn2 = mvcc_storage_->NewTransaction();
    ASSERT_NE(txn2, nullptr);
    txn2->Abort();
    
    // Should not be able to put after abort
    EXPECT_FALSE(txn2->Put(key, value));
}

} // namespace util

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
