#pragma once

#include <cstddef>
#include <cstdint>
#include <exception>
#include <stdexcept>
#include <memory>
#include <cstdlib>
#include <system_error>
#include <string.h>

namespace NJK {

    inline size_t operator "" _GiB(unsigned long long v) {
        return v << 30;
    }

    inline size_t operator "" _MiB(unsigned long long v) {
        return v << 20;
    }

    using ui32 = uint32_t;
    using ui16 = uint16_t;
    using ui8 = uint8_t;

    #define Y_ENSURE(cond) \
        if (!(cond)) { throw std::runtime_error(#cond); }

    #define Y_SYSCALL(expr) \
        if ((expr) == -1) { throw std::system_error(errno, std::generic_category(), std::string(#expr)); }

    // TODO Use mmap?
    // TODO Allocate from cache
    // TODO FIXME Non-owning version?
    struct TFixedBuffer {
        TFixedBuffer(size_t size)
            : Size_(size)
            //, Data_(new char[size])
            , Data_(std::aligned_alloc(size, size))
        {
        }

        TFixedBuffer(const TFixedBuffer&) = delete;

        TFixedBuffer(TFixedBuffer&& other) {
            Swap(other);
        }

        ~TFixedBuffer() {
            //delete[] Data_;
            if (Data_) {
                std::free(Data_);
            }
        }

        TFixedBuffer& operator= (const TFixedBuffer&) = delete;

        TFixedBuffer& operator= (TFixedBuffer&& other) {
            TFixedBuffer tmp(std::move(other));
            Swap(tmp);
            return *this;
        }

        size_t Size() const {
            return Size_;
        }

        char* Data() {
            //return Data_.get();
            return (char*)Data_;
        }

        const char* Data() const {
            //return Data_.get();
            return (const char*)Data_;
        }

        void Swap(TFixedBuffer& other) {
            std::swap(Size_, other.Size_);
            std::swap(Data_, other.Data_);
        }

    private:
        size_t Size_{};
        //std::unique_ptr<char[]> Data_;
        void* Data_{};
    };


}