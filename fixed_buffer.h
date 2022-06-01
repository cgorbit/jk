#pragma once

#include "common.h"

#include <cstddef>
#include <memory>
#include <cstring>
#include <iostream>

namespace NJK {

    // TODO Use mmap?
    // TODO Allocate from cache
    // TODO FIXME Non-owning version?

    class TFixedBuffer {
    public:
        //TFixedBuffer(size_t size)
        //    : Data_(std::aligned_alloc(size, size)) // TODO
        //    , Size_(size)
        //{
        //}

        static TFixedBuffer Aligned(size_t size) {
            TFixedBuffer ret;
            ret.Data_ = std::aligned_alloc(size, size);
            ret.Size_ = size;
            return ret;
        }

        static TFixedBuffer Empty() {
            return {};
        }

        TFixedBuffer(const TFixedBuffer&) = delete;

        TFixedBuffer(TFixedBuffer&& other) {
            //std::cerr << "MOVE CTOR\n";
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
            //std::cerr << "MOVE ASSIGN\n";
            TFixedBuffer tmp(std::move(other));
            Swap(tmp);
            return *this;
        }

        // FIXME Dirty or not???
        void FillZeroes() {
            //std::cerr << "FILL ZEROES\n";
            std::memset(MutableData(), 0, Size());
        }

        size_t Size() const {
            return Size_;
        }

        char* MutableData() {
            //return Data_.get();
            //Dirty_ = true;
            return (char*)Data_;
        }

        void CopyTo(TFixedBuffer& dst) const {
            Y_ENSURE(Size_ == dst.Size_);
            std::memcpy(dst.MutableData(), Data(), Size());
        }

        //void ResetDirtiness() {
            //Dirty_ = false;
        //}

        //bool Dirty() const {
            //return Dirty_;
        //}

        const char* Data() const {
            //return Data_.get();
            return (const char*)Data_;
        }

        void Swap(TFixedBuffer& other) {
            std::swap(Size_, other.Size_);
            std::swap(Data_, other.Data_);
        }
        
    private:
        TFixedBuffer() {
        }

    private:
        //std::unique_ptr<char[]> Data_;
        void* Data_{};
        size_t Size_{};
        //bool Dirty_ = false;
    };

}