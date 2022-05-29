#pragma once

#include "common.h"
#include "fixed_buffer.h"
#include <cstddef>
#include <cstring>

namespace NJK {

    class IOutputStream {
    public:
        virtual size_t Write(const char* buf, size_t count) = 0;

        void Save(const char* buf, size_t count) {
            Y_ENSURE(Write(buf, count) == count);
        }

        virtual void SkipWrite(size_t count) {
            char buf[count];
            Y_ENSURE(Write(buf, count) == count);
        }
    };

    class IInputStream {
    public:
        virtual size_t Read(char* buf, size_t count) = 0;

        void Load(char* buf, size_t count) {
            Y_ENSURE(Read(buf, count) == count);
        }

        virtual void SkipRead(size_t count) {
            char buf[count];
            Y_ENSURE(Read(buf, count) == count);
        }
    };

    class TBufOutput: public IOutputStream {
    public:
        TBufOutput(char* buf, size_t size)
            : Buf_(buf)
            , Size_(size)
            , Pos_(0)
        {
        }

        TBufOutput(TFixedBuffer& buf)
            : TBufOutput(buf.Data(), buf.Size())
        {
        }

        size_t Write(const char* src, size_t count) override {
            Y_ENSURE(Pos_ + count <= Size_);
            std::memcpy(Buf_ + Pos_, src, count);
            Pos_ += count;
            return count;
        }

        void SkipWrite(size_t count) override {
            Pos_ += count;
        }

    private:
        char* Buf_{};
        size_t Size_;
        size_t Pos_;
    };

    class TBufInput: public IInputStream {
    public:
        TBufInput(const char* buf, size_t size)
            : Buf_(buf)
            , Size_(size)
            , Pos_(0)
        {
        }

        TBufInput(const TFixedBuffer& buf)
            : TBufInput(buf.Data(), buf.Size())
        {
        }

        size_t Read(char* dst, size_t count) override {
            Y_ENSURE(Pos_ + count <= Size_);
            std::memcpy(dst, Buf_ + Pos_, count);
            Pos_ += count;
            return count;
        }

        void SkipRead(size_t count) override {
            Pos_ += count;
        }

    private:
        const char* Buf_{};
        size_t Size_;
        size_t Pos_;
    };

}