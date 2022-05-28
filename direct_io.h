#pragma once

#include "common.h"

#include <string>

namespace NJK {

    class TDirectIoFile {
    public:
        TDirectIoFile(const std::string& path);
        ~TDirectIoFile();

        size_t Read(char* dst, size_t count, off_t offset) const;
        size_t Write(const char* dst, size_t count, off_t offset);
        size_t GetSize() const;
        void Truncate(size_t size);

    private:
        int Fd_ = -1;
    };

}