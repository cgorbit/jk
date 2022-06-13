#pragma once

#include "fixed_buffer.h"
#include "common.h"

#include <cstddef>
#include <atomic>

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
        //    Page_ = new uint8_t[ByteSize];
        //}

        bool Test(size_t pos) const {
            const auto& b = reinterpret_cast<const uint8_t&>(Buf_.Data()[pos / 8]);
            return static_cast<bool>(b & static_cast<uint8_t>(1 << (pos % 8))) != 0;
        }

        void Set(size_t pos, bool value = true) {
            //Y_TODO("atomic");
            auto& b = reinterpret_cast<uint8_t&>(Buf_.MutableData()[pos / 8]);
            if (value) {
                b |= static_cast<uint8_t>(1 << (pos % 8));
            } else {
                b &= static_cast<uint8_t>(0b11111111 ^ (1 << (pos % 8)));
            }
        }

        //ui32 TrySetBit(size_t hint = 0) {
        //    for (size_t i = hint; i < Buf_.Size() / sizeof(size_t); ++i) {
        //        size_t* wordPtr = reinterpret_cast<size_t*>(Buf_.Data()) + i;
        //        auto& word = reinterpret_cast<std::atomic<size_t>&>(*wordPtr);
        //        size_t state = word.load();
        //        while (true)
        //            if (state == ~(size_t)0) {
        //                break;
        //            }
        //            size_t inv = ~state;
        //            size_t newState = ~((inv - 1) & inv);
        //            Y_TODO("calc bit positiion convert to idx to return");
        //            if (word.compare_exchange_strong(state, newState)) {
        //                return idx;
        //            }
        //        }
        //    }
        //    return -1;
        //}

        void Unset(size_t pos) {
            Set(pos, false);
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