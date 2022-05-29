#pragma once

#include "direct_io.h"
#include "fixed_buffer.h"

namespace NJK {

    // TODO Use some safe-int type for TBlockCount

    // TODO TBlockDirectIoFileRegion with constraints
    class TBlockDirectIoFileView {
    public:
        TBlockDirectIoFileView(TDirectIoFile& file, size_t blockSize, size_t offset = 0)
            : File_(file)
            , BlockSize_(blockSize)
            , Offset_(offset)
        {
        }

        void ReadBlock(TFixedBuffer& buf, size_t blockIdx) const {
            Y_ENSURE(buf.Size() == BlockSize_); // FIXME
            Y_ENSURE(File_.Read(buf.Data(), BlockSize_, Offset_ + BlockSize_ * blockIdx) == BlockSize_);
        }

        void WriteBlock(const TFixedBuffer& buf, size_t blockIdx) {
            Y_ENSURE(buf.Size() == BlockSize_); // FIXME
            Y_ENSURE(File_.Write(buf.Data(), BlockSize_, Offset_ + BlockSize_ * blockIdx) == BlockSize_);
        }

        size_t GetSizeInBlocks() const {
            size_t byteSize = File_.GetSize();
            Y_ENSURE(byteSize % BlockSize_ == 0);
            return byteSize / BlockSize_;
        }

        //void Truncate(size_t blockCount) {
            //File_.Truncate(blockCount * BlockSize_);
        //}

    private:
        TDirectIoFile& File_;
        size_t BlockSize_{};
        size_t Offset_ = 0;
    };

}