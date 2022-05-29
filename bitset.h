#pragma once

#include "fixed_buffer.h"
#include "common.h"

#include <cstddef>

namespace NJK {
    // TODO Do I need my own page allocator?
    // TODO What fragmentation and overhead issues will be in C++ allocator?
    class TBlockBitSet {
    public:
        TBlockBitSet(TFixedBuffer&& buf)
            : Buf_(std::move(buf))
        {
        }

        ~TBlockBitSet() {
            //delete[] Page_;
        }

        i32 FindUnset() const;

        //bool IsAllocated() const {
        //    //return Page_ != nullptr;
        //}

        //void Allocate() {
        //    Page_ = new std::byte[ByteSize];
        //}

        bool Test(size_t pos) const {
            const auto& b = reinterpret_cast<const std::byte&>(Buf_.Data()[pos / 8]);
            return static_cast<bool>(b & static_cast<std::byte>(1 << (pos % 8)));
        }

        void Set(size_t pos, bool value = true) {
            auto& b = reinterpret_cast<std::byte&>(Buf_.Data()[pos / 8]);
            if (value) {
                b |= static_cast<std::byte>(1 << (pos % 8));
            } else {
                b &= static_cast<std::byte>(0b11111111 ^ (1 << (pos % 8)));
            }
        }

        const TFixedBuffer& Buf() const {
            return Buf_;
        }

        TFixedBuffer& Buf() {
            return Buf_;
        }

        //const char* data() const {
        //    return Buf_.Data();
        //}

        //char* data() {
        //    return Buf_.Data();
        //}

    public:
        //static const constexpr size_t ByteSize = 4096;

    private:
        TFixedBuffer Buf_;
    };

}