#include "crc32c.hpp"

#include <cstddef>
#include <cstdint>


// Static variable initialization
std::array<uint32_t, 256> Crc32c::table0_ = {};
std::array<uint32_t, 256> Crc32c::table1_ = {};
std::array<uint32_t, 256> Crc32c::table2_ = {};
std::array<uint32_t, 256> Crc32c::table3_ = {};
bool Crc32c::initialized_ = false;

void Crc32c::Initialize() {
    if (initialized_) {
        return;
    }
    
    // Generate CRC32C lookup tables
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t crc = i;
        for (uint32_t j = 0; j < 8; j++) {
            crc = (crc >> 1) ^ (kPolynomial & -(crc & 1));
        }
        table0_[i] = crc;
    }
    
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t crc = table0_[i];
        for (uint32_t j = 0; j < 3; j++) {
            crc = table0_[crc & 0xff] ^ (crc >> 8);
        }
        table1_[i] = crc;
        
        crc = table0_[i];
        for (uint32_t j = 0; j < 2; j++) {
            crc = table0_[crc & 0xff] ^ (crc >> 8);
        }
        table2_[i] = crc;
        
        crc = table0_[i];
        crc = table0_[crc & 0xff] ^ (crc >> 8);
        table3_[i] = crc;
    }
    
    initialized_ = true;
}

uint32_t Crc32c::Compute(const void* data, size_t length) {
    if (!initialized_) {
        Initialize();
    }
    
    const uint8_t* ptr = static_cast<const uint8_t*>(data);
    uint32_t crc = ~0U;
    
    // Process data in blocks of 4 bytes using the lookup tables
    while (length >= 4) {
        crc ^= ptr[0] | (static_cast<uint32_t>(ptr[1]) << 8) |
               (static_cast<uint32_t>(ptr[2]) << 16) | (static_cast<uint32_t>(ptr[3]) << 24);
        crc = table3_[crc & 0xff] ^
              table2_[(crc >> 8) & 0xff] ^
              table1_[(crc >> 16) & 0xff] ^
              table0_[(crc >> 24) & 0xff];
        ptr += 4;
        length -= 4;
    }
    
    // Process remaining bytes
    while (length-- > 0) {
        crc = table0_[(crc ^ *ptr++) & 0xff] ^ (crc >> 8);
    }
    
    return ~crc;  // Final inversion
}

uint32_t Crc32c::Compute(const std::vector<uint8_t>& data) {
    return Compute(data.data(), data.size());
}

