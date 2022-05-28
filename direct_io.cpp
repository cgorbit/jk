#include "direct_io.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>

namespace NJK {

    TDirectIoFile::TDirectIoFile(const std::string& path)
        : Fd_(open(path.c_str(), O_DIRECT | O_RDWR | O_CREAT, 0666))
    {
        Y_ENSURE(Fd_ != -1);
    }

    TDirectIoFile::~TDirectIoFile() {
        close(Fd_);
    }

    size_t TDirectIoFile::Read(char* buf, size_t count, off_t offset) const {
        ssize_t ret = pread(Fd_, buf, count, offset);
        Y_ENSURE(ret != -1);
        return ret;
    }

    size_t TDirectIoFile::Write(const char* buf, size_t count, off_t offset) {
        ssize_t ret = pwrite(Fd_, buf, count, offset);
        Y_ENSURE(ret != -1);
        return ret;
    }

    size_t TDirectIoFile::GetSize() const {
        struct stat stat{};
        Y_SYSCALL(fstat(Fd_, &stat));
        return stat.st_size;
    }

    void TDirectIoFile::Truncate(size_t size) {
        Y_SYSCALL(ftruncate(Fd_, size));
    }
}