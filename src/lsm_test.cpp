#include "lsm_storage.hpp"
#include "byte_buffer.hpp"

#include <iostream>

// Simple test program to verify LsmStorageInner class compilation
int main() {
    std::filesystem::path test_path = "/tmp/lsm_test";
    
    // Clean up any existing test directory
    std::filesystem::remove_all(test_path);
    std::filesystem::create_directory(test_path);
    
    // Create simple options
    util::LsmStorageOptions options;
    
    // Try to create an LSM storage instance
    auto storage = util::LsmStorageInner::Create(test_path, options);
    
    if (storage) {
        std::cout << "Successfully created LsmStorageInner instance" << std::endl;
        
        // Test a simple Put operation
        util::ByteBuffer key("test_key");
        util::ByteBuffer value("test_value");
        
        bool result = storage->Put(key, value);
        
        if (result) {
            std::cout << "Successfully put key-value pair" << std::endl;
            
            // Test Get operation
            auto retrieved = storage->Get(key);
            
            if (retrieved && retrieved->ToString() == "test_value") {
                std::cout << "Successfully retrieved value: " << retrieved->ToString() << std::endl;
            } else {
                std::cout << "Failed to retrieve value correctly" << std::endl;
            }
        } else {
            std::cout << "Failed to put key-value pair" << std::endl;
        }
    } else {
        std::cout << "Failed to create LsmStorageInner instance" << std::endl;
    }
    
    return 0;
}
