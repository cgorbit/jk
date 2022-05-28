#pragma once

#include <cstddef>

namespace NJK {
    // TODO Do I need my own page allocator?
    // TODO What fragmentation and overhead issues will be in C++ allocator?
    class TBlockBitSet {
    public:
        TBlockBitSet() {
        }

        ~TBlockBitSet() {
            delete[] Page_;
        }

        bool IsAllocated() const {
            return Page_ != nullptr;
        }

        void Allocate() {
            Page_ = new std::byte[ByteSize];
        }

        bool test(size_t pos) const {
            return static_cast<bool>(Page_[pos / 8] & static_cast<std::byte>(1 << (pos % 8)));
        }

        void set(size_t pos, bool value = true) {
            auto& b = Page_[pos / 8];
            if (value) {
                b |= static_cast<std::byte>(1 << (pos % 8));
            } else {
                b &= static_cast<std::byte>(0b11111111 ^ (1 << (pos % 8)));
            }
        }

        const std::byte* data() const {
            return Page_;
        }

        std::byte* data() {
            return Page_;
        }

    public:
        static const constexpr size_t ByteSize = 4096;

    private:
        std::byte* Page_{};
    };

}