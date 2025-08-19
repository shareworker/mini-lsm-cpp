#pragma once

// Bloom filter implementation ported from Rust mini-lsm `bloom.rs`.
// Header-only for simplicity. Uses zlib's crc32 for checksum.

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <stdexcept>
#include <string_view>
#include <vector>
#include <zlib.h>  // crc32


class BloomFilter {
public:
    BloomFilter() = default;

    BloomFilter(std::vector<uint8_t> filter, uint8_t k)
        : filter_(std::move(filter)), k_(k) {}

    // Deserialize from buffer (filter bytes + k byte + 4-byte checksum)
    static BloomFilter Decode(const std::vector<uint8_t>& buf) {
        if (buf.size() < 5) {
            throw std::invalid_argument("bloom buffer too small");
        }
        const size_t payload_len = buf.size() - 5;  // filter + k
        uint32_t checksum_read = 0;
        for (size_t i = 0; i < 4; ++i) {
            checksum_read = (checksum_read << 8u) | buf[payload_len + 1 + i];
        }
        uint32_t checksum_calc =
            ::crc32(0U, buf.data(), static_cast<uInt>(payload_len + 1));
        if (checksum_calc != checksum_read) {
            throw std::invalid_argument("bloom checksum mismatch");
        }
        uint8_t k = buf[payload_len];
        return BloomFilter(std::vector<uint8_t>(buf.begin(), buf.begin() + payload_len), k);
    }

    // Serialize into out vector
    void Encode(std::vector<uint8_t>& out) const {
        size_t offset = out.size();
        out.insert(out.end(), filter_.begin(), filter_.end());
        out.push_back(k_);
        uint32_t checksum =
            ::crc32(0U, out.data() + offset, static_cast<uInt>(filter_.size() + 1));
        for (int i = 3; i >= 0; --i) {
            out.push_back(static_cast<uint8_t>((checksum >> (i * 8)) & 0xFF));
        }
    }

    // bits per key suggestion
    static size_t BitsPerKey(size_t entries, double fpr) {
        double size = -1.0 * static_cast<double>(entries) * std::log(fpr) /
                       std::pow(std::log(2.0), 2);
        return static_cast<size_t>(std::ceil(size / static_cast<double>(entries)));
    }

    static BloomFilter BuildFromKeyHashes(const std::vector<uint32_t>& hashes,
                                          size_t bits_per_key) {
        uint32_t k = static_cast<uint32_t>(bits_per_key * 0.69);
        k = std::clamp<uint32_t>(k, 1, 30);
        size_t nbits = std::max<size_t>(hashes.size() * bits_per_key, 64);
        size_t nbytes = (nbits + 7) / 8;
        nbits = nbytes * 8;
        std::vector<uint8_t> filter(nbytes, 0);
        for (uint32_t h0 : hashes) {
            uint32_t h = h0;
            uint32_t delta = (h << 15) | (h >> 17);  // rotate_left(15)
            for (uint32_t i = 0; i < k; ++i) {
                size_t bit_pos = static_cast<size_t>(h) % nbits;
                filter[bit_pos / 8] |= static_cast<uint8_t>(1u << (bit_pos % 8));
                h += delta;
            }
        }
        return BloomFilter(std::move(filter), static_cast<uint8_t>(k));
    }

    bool MayContain(uint32_t hash) const {
        if (k_ > 30) return true;  // future encoding reserved
        size_t nbits = filter_.size() * 8;
        uint32_t delta = (hash << 15) | (hash >> 17);
        for (uint8_t i = 0; i < k_; ++i) {
            size_t bit_pos = static_cast<size_t>(hash) % nbits;
            if ((filter_[bit_pos / 8] & (1u << (bit_pos % 8))) == 0) {
                return false;
            }
            hash += delta;
        }
        return true;
    }

    // Getters
    const std::vector<uint8_t>& Data() const { return filter_; }
    uint8_t K() const { return k_; }

private:
    std::vector<uint8_t> filter_;
    uint8_t k_{};
};

