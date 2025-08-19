#pragma once

#include <memory>
#include <random>
#include <utility>
#include <vector>
#include <optional>
#include <functional>
#include <shared_mutex>
#include <mutex>
#include <atomic>
#include <cassert>


/**
 * @brief A lock-free concurrent skiplist implementation inspired by Crossbeam's skiplist in Rust.
 * 
 * This skiplist provides thread-safe operations with lock-free reads and fine-grained locking
 * for writes. It supports concurrent access from multiple threads with minimal contention.
 * 
 * @tparam Key The key type for the skiplist, must be comparable with operator<
 * @tparam Value The value type stored in the skiplist
 * @tparam Compare The comparison function type, defaults to std::less<Key>
 */
template <typename Key, typename Value, typename Compare = std::less<Key>>
class SkipList {
public:
    using KeyType = Key;
    using ValueType = Value;
    using KeyValuePair = std::pair<Key, Value>;
    using OptionalValue = std::optional<Value>;

private:
    // Forward declaration of Node
    struct Node;
    using NodePtr = std::shared_ptr<Node>;

    // Maximum level of the skiplist
    static constexpr int kMaxLevel = 32;
    
    // Probability factor for level generation (p = 1/4)
    static constexpr float kProbability = 0.25f;

    /**
     * @brief Internal node structure for the skiplist
     */
    struct Node {
        KeyType key;
        ValueType value;
        int level;
        std::vector<NodePtr> next;

        // Constructor for regular nodes
        Node(KeyType k, ValueType v, int lvl)
            : key(std::move(k)),
              value(std::move(v)),
              level(std::clamp(lvl, 0, kMaxLevel - 1)),
              next(std::clamp(lvl, 0, kMaxLevel - 1) + 1, nullptr) {}
    };

    // Head node of the skiplist
    NodePtr head_;
    
    // Current maximum level of the skiplist
    std::atomic<int> current_level_{0};
    
    // Comparison function object
    Compare compare_;
    
    // Random number generator for level generation
    inline static thread_local std::random_device random_device_;
    inline static thread_local std::mt19937 generator_{random_device_()};
    inline static thread_local std::uniform_real_distribution<float> distribution_{0.0f, 1.0f};
    
    // Mutex for write operations
    mutable std::shared_mutex mutex_;

    /**
     * @brief Generates a random level for a new node
     * 
     * @return int The generated level
     */
    int RandomLevel() const {
        int level = 0;
        while (distribution_(generator_) < kProbability && level < kMaxLevel - 1) {
            ++level;
        }
        return level;
    }

    /**
     * @brief Finds the node with the given key and collects predecessors and successors
     * 
     * @param key The key to search for
     * @param predecessors Array to store predecessor nodes
     * @param successors Array to store successor nodes
     * @return bool True if the key was found, false otherwise
     */
    bool FindNode(const KeyType& key, std::vector<NodePtr>& predecessors, std::vector<NodePtr>& successors) const {
        // Ensure vectors are large enough to hold all levels
        if (predecessors.size() < kMaxLevel || successors.size() < kMaxLevel) {
            // Resize vectors if they're not large enough
            try {
                if (predecessors.size() < kMaxLevel) {
                    predecessors.resize(kMaxLevel, nullptr);
                }
                if (successors.size() < kMaxLevel) {
                    successors.resize(kMaxLevel, nullptr);
                }
            } catch (const std::exception& e) {
                // Failed to resize, can't proceed safely
                return false;
            }
        }
        
        bool found = false;
        NodePtr current = head_;
        
        // Ensure current_level_ is valid
        int current_lvl = std::clamp(current_level_.load(), 0, kMaxLevel - 1);

        for (int i = current_lvl; i >= 0; --i) {
            // Ensure that we have a valid 'next' array and index is within bounds
            if (!current || i >= static_cast<int>(current->next.size())) {
                // Something is wrong with the structure, abort safely
                return false;
            }
            
            NodePtr next_node = std::atomic_load(&current->next[i]);
            while (next_node && compare_(next_node->key, key)) {
                current = next_node;
                // Check again after updating current
                if (!current || i >= static_cast<int>(current->next.size())) {
                    return false;
                }
                next_node = std::atomic_load(&current->next[i]);
            }
            
            // Store the predecessor and successor nodes
            predecessors[i] = current;
            successors[i] = next_node;
            
            // Check if we found the key at this level
            if (!found && next_node && !compare_(key, next_node->key) && 
                !compare_(next_node->key, key)) {
                found = true;
            }
        }
        
        return found;
    }

public:
    /**
     * @brief Construct a new SkipList object
     * 
     * @param compare Custom comparison function (optional)
     */
    SkipList(Compare compare = Compare()) 
        : compare_(compare) {
        // Create a head node with a dummy key/value at maximum level
        head_ = std::make_shared<Node>(KeyType{}, ValueType{}, kMaxLevel - 1);
        
        // Ensure the head node's next pointers are all properly initialized to nullptr
        for (size_t i = 0; i < head_->next.size(); ++i) {
            head_->next[i] = nullptr;
        }
        
        // Initialize current level to 0
        current_level_ = 0;
    }
    
    /**
     * @brief Clear all elements from the skiplist
     */
    void Clear() {
        std::unique_lock lock(mutex_);
        
        for (int i = 0; i <= current_level_; ++i) {
            std::atomic_store(&head_->next[i], NodePtr());
        }
        
        current_level_ = 0;
    }

    /**
     * @brief Destructor that properly cleans up all nodes
     */
    ~SkipList() {
        Clear();
    }

    // SkipList is not copyable
    SkipList(const SkipList&) = delete;
    SkipList& operator=(const SkipList&) = delete;

    // SkipList is movable
    SkipList(SkipList&& other) noexcept 
        : head_(std::move(other.head_)),
          current_level_(other.current_level_.load()),
          compare_(std::move(other.compare_)) {
        other.current_level_ = 0;
    }

    SkipList& operator=(SkipList&& other) noexcept {
        if (this != &other) {
            Clear();
            head_ = std::move(other.head_);
            current_level_ = other.current_level_.load();
            compare_ = std::move(other.compare_);
            other.current_level_ = 0;
        }
        return *this;
    }
    std::unique_lock<std::shared_mutex> lock1() { return std::unique_lock(mutex_, std::defer_lock); }


    /**
     * @brief Insert a key-value pair into the skiplist
     * 
     * @param key The key to insert
     * @param value The value to associate with the key
     * @return true if insertion was successful, false if the key already exists
     */
    bool Insert(const KeyType& key, const ValueType& value) {
        std::unique_lock lock(mutex_);
        
        try {
            std::vector<NodePtr> predecessors(kMaxLevel, nullptr);
            std::vector<NodePtr> successors(kMaxLevel, nullptr);
            
            if (FindNode(key, predecessors, successors)) {
                // Key already exists - update the value (upsert behavior)
                NodePtr existing_node = successors[0];
                if (existing_node) {
                    existing_node->value = value;
                    return true;
                }
                return false;
            }
            
            // Ensure we have a valid RandomLevel (0 <= new_level < kMaxLevel)
            int new_level = std::clamp(RandomLevel(), 0, kMaxLevel - 1);
            
            // Safely update the current_level_
            int current_lvl = std::clamp(current_level_.load(), 0, kMaxLevel - 1);
            
            if (new_level > current_lvl) {
                for (int i = current_lvl + 1; i <= new_level && i < kMaxLevel; ++i) {
                    // Ensure head_ and its next array are valid
                    if (!head_ || i >= static_cast<int>(head_->next.size())) {
                        return false;
                    }
                    
                    predecessors[i] = head_;
                    successors[i] = std::atomic_load(&head_->next[i]);
                }
                
                // Safely update current_level_
                current_level_.store(new_level);
            }
            
            // Create a new node with safe level value
            NodePtr new_node = std::make_shared<Node>(key, value, new_level);
            
            // Ensure the node was created successfully and has a valid next array
            if (!new_node || new_node->next.size() <= static_cast<size_t>(new_level)) {
                return false;
            }
            
            // Connect node at each level
            for (int i = 0; i <= new_level && i < static_cast<int>(new_node->next.size()); ++i) {
                if (i < static_cast<int>(predecessors.size()) && predecessors[i]) {
                    // Only store if index is valid
                    std::atomic_store(&new_node->next[i], successors[i]);
                    if (i < static_cast<int>(predecessors[i]->next.size())) {
                        std::atomic_store(&predecessors[i]->next[i], new_node);
                    }
                }
            }
            
            return true;
        } catch (const std::exception& e) {
            // Handle any exceptions during insertion
            return false;
        }
    }

    /**
     * @brief Remove a key from the skiplist
     * 
     * @param key The key to remove
     * @return true if removal was successful, false if the key was not found
     */
    bool Remove(const KeyType& key) {
        std::unique_lock lock(mutex_);
        
        try {
            std::vector<NodePtr> predecessors(kMaxLevel, nullptr);
            std::vector<NodePtr> successors(kMaxLevel, nullptr);
            
            if (!FindNode(key, predecessors, successors)) {
                // Key not found
                return false;
            }
            
            // Get the node to remove
            NodePtr node_to_remove = successors[0];
            if (!node_to_remove) {
                return false;
            }
            
            // Similar to crossbeam-skiplist's approach, adjust references at each level
            // Get a safe current level value
            int current_lvl = std::clamp(current_level_.load(), 0, kMaxLevel - 1);
            
            for (int i = 0; i <= current_lvl; ++i) {
                // Safety checks
                if (i >= static_cast<int>(predecessors.size()) || !predecessors[i] || 
                    i >= static_cast<int>(predecessors[i]->next.size())) {
                    continue;
                }
                
                if (std::atomic_load(&predecessors[i]->next[i]) != node_to_remove) {
                    // Node not found at this level
                    continue;
                }
                
                // Safety check for node_to_remove
                if (i >= static_cast<int>(node_to_remove->next.size())) {
                    continue;
                }
                
                // Update references - bypass the node being removed
                std::atomic_store(
                    &predecessors[i]->next[i], 
                    std::atomic_load(&node_to_remove->next[i])
                );
            }
            
            // Update the current level if needed
            while (current_level_.load() > 0 && 
                  head_ && 
                  current_level_.load() < static_cast<int>(head_->next.size()) && 
                  std::atomic_load(&head_->next[current_level_.load()]) == nullptr) {
                current_level_.fetch_sub(1);
            }
            
            return true;
        } catch (const std::exception& e) {
            // Handle any exceptions during removal
            return false;
        }
    }

    /**
     * @brief Find a key in the skiplist
     * 
     * @param key The key to find
     * @return OptionalValue The value if found, std::nullopt otherwise
     */
    OptionalValue Find(const KeyType& key) const {
        std::shared_lock lock(mutex_);
        
        std::vector<NodePtr> predecessors(kMaxLevel);
        std::vector<NodePtr> successors(kMaxLevel);
        
        if (FindNode(key, predecessors, successors)) {
            NodePtr node = successors[0];
            return node->value;
        }
        
        return std::nullopt;
    }

    /**
     * @brief Find a key in the skiplist
     * 
     * @param key The key to find
     * @return OptionalValue The value if found, std::nullopt otherwise
     */

    /**
     * @brief Check if a key exists in the skiplist
     * 
     * @param key The key to check
     * @return true if the key exists, false otherwise
     */
    bool Contains(const KeyType& key) const {
        std::shared_lock lock(mutex_);
        
        std::vector<NodePtr> predecessors(kMaxLevel);
        std::vector<NodePtr> successors(kMaxLevel);
        
        return FindNode(key, predecessors, successors);
    }

    /**
     * @brief Update the value associated with a key
     * 
     * @param key The key to update
     * @param value The new value
     * @return true if update was successful, false if the key was not found
     */
    bool Update(const KeyType& key, const ValueType& value) {
        std::unique_lock lock(mutex_);
        
        NodePtr current = head_;
        
        for (int i = current_level_; i >= 0; --i) {
            NodePtr next_node = std::atomic_load(&current->next[i]);
            while (next_node && compare_(next_node->key, key)) {
                current = next_node;
                next_node = std::atomic_load(&current->next[i]);
            }
        }
        
        current = std::atomic_load(&current->next[0]);
        
        if (current && !compare_(key, current->key) && !compare_(current->key, key)) {
            current->value = value;
            return true;
        }
        
        return false;
    }

    /**
     * @brief Clear all elements from the skiplist
     */
    /**
     * @brief Check if the skiplist is empty
     * 
     * @return true if empty, false otherwise
     */
    bool IsEmpty() const {
        std::shared_lock lock(mutex_);
        return std::atomic_load(&head_->next[0]) == nullptr;
    }

    /**
     * @brief Get the first key-value pair in the skiplist
     * 
     * @return std::optional<KeyValuePair> The first pair if exists, std::nullopt otherwise
     */
    std::optional<KeyValuePair> First() const {
        std::shared_lock lock(mutex_);
        
        NodePtr first_node = std::atomic_load(&head_->next[0]);
        if (first_node == nullptr) {
            return std::nullopt;
        }
        
        return std::make_optional<KeyValuePair>(
            first_node->key, 
            first_node->value
        );
    }

    /**
     * @brief Get the last key-value pair in the skiplist
     * 
     * @return std::optional<KeyValuePair> The last pair if exists, std::nullopt otherwise
     */
    std::optional<KeyValuePair> Last() const {
        std::shared_lock lock(mutex_);
        
        NodePtr current = head_;
        
        for (int i = current_level_; i >= 0; --i) {
            NodePtr next_node = std::atomic_load(&current->next[i]);
            while (next_node != nullptr) {
                current = next_node;
                next_node = std::atomic_load(&current->next[i]);
            }
        }
        
        if (current == head_) {
            return std::nullopt;
        }

        return std::make_optional<KeyValuePair>(current->key, current->value);
    }

    /**
     * @brief Apply a function to each key-value pair in the skiplist
     * 
     * @param func The function to apply, should accept (const Key&, const Value&)
     */
    template <typename Func>
    void ForEach(Func func) const {
        std::shared_lock lock(mutex_);
        
        NodePtr current = std::atomic_load(&head_->next[0]);
        
        while (current) {
            func(current->key, current->value);
            current = std::atomic_load(&current->next[0]);
        }
    }
};

// (static thread_local members are defined inline, no out-of-class definitions needed)




