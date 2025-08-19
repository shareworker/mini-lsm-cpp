#pragma once

#include <cstdint>
#include <cstddef>  // For size_t
#include <array>
#include <vector>


/**
 * @brief Implementation of CRC32C (Castagnoli polynomial 0x1EDC6F41)
 * 
 * This is a software-based implementation of the CRC32C checksum algorithm,
 * which uses the Castagnoli polynomial for better error detection than the
 * standard CRC32 algorithm.
 */
class Crc32c {
public:
    /**
     * @brief Initialize the CRC32C lookup tables
     * 
     * Must be called before using any other methods.
     */
    static void Initialize();

    /**
     * @brief Calculate CRC32C checksum for a byte array
     * 
     * @param data Pointer to the data
     * @param length Length of the data in bytes
     * @return uint32_t CRC32C checksum
     */
    static uint32_t Compute(const void* data, size_t length);

    /**
     * @brief Calculate CRC32C checksum for a vector of bytes
     * 
     * @param data Vector containing the data
     * @return uint32_t CRC32C checksum
     */
    static uint32_t Compute(const std::vector<uint8_t>& data);

private:
    // CRC32C polynomial in reversed bit order
    static constexpr uint32_t kPolynomial = 0x82F63B78;
    
    // Lookup tables for CRC32C calculation
    static std::array<uint32_t, 256> table0_;
    static std::array<uint32_t, 256> table1_;
    static std::array<uint32_t, 256> table2_;
    static std::array<uint32_t, 256> table3_;
    
    // Flag to check if tables are initialized
    static bool initialized_;
};

