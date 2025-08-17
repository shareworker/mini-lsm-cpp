#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include <string>

#include "fused_iterator.hpp"
#include "safe_merge_iterator.hpp"
#include "safe_two_merge_iterator.hpp"
#include "safe_block_iterator.hpp"
#include "storage_iterator.hpp"
#include "byte_buffer.hpp"
#include "block.hpp"

namespace util {
namespace test {

/**
 * @brief Mock iterator for testing safety mechanisms
 */
class MockIterator : public StorageIterator {
private:
    std::vector<std::pair<std::string, std::string>> data_;
    size_t pos_ = 0;
    bool force_invalid_ = false;

public:
    explicit MockIterator(std::vector<std::pair<std::string, std::string>> data)
        : data_(std::move(data)) {}

    void ForceInvalid() { force_invalid_ = true; }

    bool IsValid() const noexcept override {
        return !force_invalid_ && pos_ < data_.size();
    }

    ByteBuffer Key() const noexcept override {
        if (!IsValid()) return ByteBuffer{};
        return ByteBuffer(data_[pos_].first);
    }

    const ByteBuffer& Value() const noexcept override {
        static ByteBuffer buffer;
        if (!IsValid()) {
            buffer = ByteBuffer{};
        } else {
            buffer = ByteBuffer(data_[pos_].second);
        }
        return buffer;
    }

    void Next() noexcept override {
        if (IsValid()) {
            ++pos_;
        }
    }
};

class IteratorSafetyTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Setup test data
        test_data_a_ = {
            {"key1", "value1"},
            {"key3", "value3"},
            {"key5", "value5"}
        };
        
        test_data_b_ = {
            {"key2", "value2"},
            {"key4", "value4"},
            {"key6", "value6"}
        };
        
        test_data_overlapping_ = {
            {"key1", "value1_b"},
            {"key3", "value3_b"},
            {"key7", "value7"}
        };
    }

    std::unique_ptr<MockIterator> CreateMockIterator(
        const std::vector<std::pair<std::string, std::string>>& data) {
        return std::make_unique<MockIterator>(data);
    }

    std::vector<std::pair<std::string, std::string>> test_data_a_;
    std::vector<std::pair<std::string, std::string>> test_data_b_;
    std::vector<std::pair<std::string, std::string>> test_data_overlapping_;
};

// Test FusedIterator base safety guarantees
TEST_F(IteratorSafetyTest, FusedIteratorBasicSafety) {
    auto mock_iter = CreateMockIterator(test_data_a_);
    auto safe_iter = MakeSafe(std::move(mock_iter));

    // Initially valid
    EXPECT_TRUE(safe_iter->IsValid());
    EXPECT_FALSE(safe_iter->IsFused());
    
    // Advance through all elements
    std::vector<std::string> keys;
    while (safe_iter->IsValid()) {
        keys.push_back(safe_iter->Key().ToString());
        safe_iter->Next();
    }
    
    EXPECT_EQ(keys.size(), 3);
    EXPECT_EQ(keys[0], "key1");
    EXPECT_EQ(keys[1], "key3");
    EXPECT_EQ(keys[2], "key5");
    
    // Now exhausted - should be fused
    EXPECT_FALSE(safe_iter->IsValid());
    EXPECT_TRUE(safe_iter->IsFused());
    
    // Safe to call operations on fused iterator
    EXPECT_TRUE(safe_iter->Key().IsEmpty());
    EXPECT_TRUE(safe_iter->Value().IsEmpty());
    
    // Next() should be no-op on fused iterator
    safe_iter->Next();
    EXPECT_FALSE(safe_iter->IsValid());
    EXPECT_TRUE(safe_iter->IsFused());
}

// Test SafeMergeIterator with multiple iterators
TEST_F(IteratorSafetyTest, SafeMergeIteratorBasic) {
    std::vector<std::unique_ptr<StorageIterator>> iters;
    iters.push_back(CreateMockIterator(test_data_a_));
    iters.push_back(CreateMockIterator(test_data_b_));
    
    auto merge_iter = SafeMergeIterator::Create(std::move(iters));
    
    // Verify merged sequence
    std::vector<std::string> merged_keys;
    while (merge_iter->IsValid()) {
        merged_keys.push_back(merge_iter->Key().ToString());
        merge_iter->Next();
    }
    
    EXPECT_EQ(merged_keys.size(), 6);
    EXPECT_EQ(merged_keys[0], "key1");
    EXPECT_EQ(merged_keys[1], "key2");
    EXPECT_EQ(merged_keys[2], "key3");
    EXPECT_EQ(merged_keys[3], "key4");
    EXPECT_EQ(merged_keys[4], "key5");
    EXPECT_EQ(merged_keys[5], "key6");
    
    // Should be fused after exhaustion
    EXPECT_TRUE(merge_iter->IsFused());
    EXPECT_FALSE(merge_iter->IsValid());
}

// Test SafeMergeIterator with duplicates
TEST_F(IteratorSafetyTest, SafeMergeIteratorDuplicates) {
    std::vector<std::unique_ptr<StorageIterator>> iters;
    iters.push_back(CreateMockIterator(test_data_a_));
    iters.push_back(CreateMockIterator(test_data_overlapping_));
    
    auto merge_iter = SafeMergeIterator::Create(std::move(iters));
    
    std::vector<std::pair<std::string, std::string>> results;
    while (merge_iter->IsValid()) {
        results.emplace_back(
            merge_iter->Key().ToString(),
            merge_iter->Value().ToString()
        );
        merge_iter->Next();
    }
    
    // Should prefer first iterator on duplicates
    EXPECT_EQ(results.size(), 4);
    EXPECT_EQ(results[0], std::make_pair(std::string("key1"), std::string("value1")));
    EXPECT_EQ(results[1], std::make_pair(std::string("key3"), std::string("value3")));
    EXPECT_EQ(results[2], std::make_pair(std::string("key5"), std::string("value5")));
    EXPECT_EQ(results[3], std::make_pair(std::string("key7"), std::string("value7")));
}

// Test SafeTwoMergeIterator basic functionality
TEST_F(IteratorSafetyTest, SafeTwoMergeIteratorBasic) {
    auto iter_a = CreateMockIterator(test_data_a_);
    auto iter_b = CreateMockIterator(test_data_b_);
    
    auto two_merge = SafeTwoMergeIterator::Create(std::move(iter_a), std::move(iter_b));
    
    std::vector<std::string> merged_keys;
    while (two_merge->IsValid()) {
        merged_keys.push_back(two_merge->Key().ToString());
        two_merge->Next();
    }
    
    EXPECT_EQ(merged_keys.size(), 6);
    EXPECT_EQ(merged_keys[0], "key1");
    EXPECT_EQ(merged_keys[5], "key6");
    
    EXPECT_TRUE(two_merge->IsFused());
}

// Test SafeTwoMergeIterator with duplicates
TEST_F(IteratorSafetyTest, SafeTwoMergeIteratorDuplicates) {
    auto iter_a = CreateMockIterator(test_data_a_);
    auto iter_b = CreateMockIterator(test_data_overlapping_);
    
    auto two_merge = SafeTwoMergeIterator::Create(std::move(iter_a), std::move(iter_b));
    
    std::vector<std::pair<std::string, std::string>> results;
    while (two_merge->IsValid()) {
        results.emplace_back(
            two_merge->Key().ToString(),
            two_merge->Value().ToString()
        );
        two_merge->Next();
    }
    
    // Should prefer iterator A on duplicates
    EXPECT_EQ(results.size(), 4);
    EXPECT_EQ(results[0].second, "value1"); // From A, not B
    EXPECT_EQ(results[1].second, "value3"); // From A, not B
}

// Test exhaustion safety
TEST_F(IteratorSafetyTest, ExhaustionSafety) {
    auto mock_iter = CreateMockIterator({{"key1", "value1"}});
    auto safe_iter = MakeSafe(std::move(mock_iter));
    
    // Advance past the single element
    EXPECT_TRUE(safe_iter->IsValid());
    safe_iter->Next();
    EXPECT_FALSE(safe_iter->IsValid());
    EXPECT_TRUE(safe_iter->IsFused());
    
    // Multiple calls to Next() should be safe
    safe_iter->Next();
    safe_iter->Next();
    safe_iter->Next();
    
    EXPECT_FALSE(safe_iter->IsValid());
    EXPECT_TRUE(safe_iter->IsFused());
    
    // Key() and Value() should return empty
    EXPECT_TRUE(safe_iter->Key().IsEmpty());
    EXPECT_TRUE(safe_iter->Value().IsEmpty());
}

// Test corruption detection
TEST_F(IteratorSafetyTest, CorruptionDetection) {
    std::vector<std::unique_ptr<StorageIterator>> iters;
    iters.push_back(CreateMockIterator(test_data_a_));
    iters.push_back(nullptr); // Null iterator to trigger corruption
    
    auto merge_iter = SafeMergeIterator::Create(std::move(iters));
    
    // Should detect corruption and become invalid
    EXPECT_TRUE(merge_iter->IsStateCorrupted());
    EXPECT_FALSE(merge_iter->IsValid());
}

// Test empty iterator handling
TEST_F(IteratorSafetyTest, EmptyIteratorHandling) {
    std::vector<std::unique_ptr<StorageIterator>> iters;
    iters.push_back(CreateMockIterator({})); // Empty iterator
    iters.push_back(CreateMockIterator(test_data_a_));
    
    auto merge_iter = SafeMergeIterator::Create(std::move(iters));
    
    // Should handle empty iterator gracefully
    std::vector<std::string> keys;
    while (merge_iter->IsValid()) {
        keys.push_back(merge_iter->Key().ToString());
        merge_iter->Next();
    }
    
    EXPECT_EQ(keys.size(), 3); // Only from non-empty iterator
    EXPECT_EQ(keys[0], "key1");
}

// Test statistics and debugging features
TEST_F(IteratorSafetyTest, DebugStatistics) {
    auto iter_a = CreateMockIterator(test_data_a_);
    auto iter_b = CreateMockIterator(test_data_b_);
    
    auto two_merge = SafeTwoMergeIterator::Create(std::move(iter_a), std::move(iter_b));
    
    size_t initial_comparisons = two_merge->GetComparisonCount();
    
    // Advance a few times
    two_merge->Next();
    two_merge->Next();
    
    size_t final_comparisons = two_merge->GetComparisonCount();
    EXPECT_GT(final_comparisons, initial_comparisons);
    
    // Check source info
    [[maybe_unused]] auto [from_a, both_valid] = two_merge->GetSourceInfo();
    // At this point, should still have both iterators valid
}

// Test performance under stress
TEST_F(IteratorSafetyTest, StressTest) {
    // Create larger dataset
    std::vector<std::pair<std::string, std::string>> large_data_a;
    std::vector<std::pair<std::string, std::string>> large_data_b;
    
    for (int i = 0; i < 1000; i += 2) {
        large_data_a.emplace_back("key" + std::to_string(i), "value" + std::to_string(i));
    }
    
    for (int i = 1; i < 1000; i += 2) {
        large_data_b.emplace_back("key" + std::to_string(i), "value" + std::to_string(i));
    }
    
    auto iter_a = CreateMockIterator(large_data_a);
    auto iter_b = CreateMockIterator(large_data_b);
    
    auto two_merge = SafeTwoMergeIterator::Create(std::move(iter_a), std::move(iter_b));
    
    size_t count = 0;
    while (two_merge->IsValid()) {
        two_merge->Next();
        ++count;
    }
    
    EXPECT_EQ(count, 1000);
    EXPECT_TRUE(two_merge->IsFused());
    EXPECT_FALSE(two_merge->IsStateCorrupted());
}

// Test empty merge iterator handling
TEST_F(IteratorSafetyTest, EmptyMergeIteratorSafety) {
    std::vector<std::unique_ptr<StorageIterator>> empty_iters;
    auto empty_merge = SafeMergeIterator::Create(std::move(empty_iters));
    
    // Should handle empty iterator list gracefully
    EXPECT_FALSE(empty_merge->IsValid());
    EXPECT_TRUE(empty_merge->IsFused());
    
    // Safe operations on empty merge iterator
    EXPECT_TRUE(empty_merge->Key().IsEmpty());
    empty_merge->Next(); // Should be no-op
}

} // namespace test
} // namespace util

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
