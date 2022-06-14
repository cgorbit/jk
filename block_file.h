#pragma once

#include "direct_io.h"
#include "fixed_buffer.h"
#include "hash_map.h"
//#include <unordered_map>

#include <atomic>
#include <mutex>
#include <condition_variable>

#include <iostream>

namespace NJK {

    // TODO Use some safe-int type for TBlockCount

    // TODO TBlockDirectIoFileRegion with constraints
    class TBlockDirectIoFile {
    public:
        TBlockDirectIoFile(const std::string& path, size_t blockSize)
            : File_(path)
            , BlockSize_(blockSize)
        {
        }

        void ReadBlock(TFixedBuffer& buf, size_t blockIdx) const {
            Y_ENSURE(buf.Size() == BlockSize_); // FIXME
            Y_ENSURE(File_.Read(buf.MutableData(), BlockSize_, BlockSize_ * blockIdx) == BlockSize_);

            // TODO Better (maybe wrapper class)
            //buf.ResetDirtiness();
        }

        void WriteBlock(const TFixedBuffer& buf, size_t blockIdx) {
            Y_ENSURE(buf.Size() == BlockSize_); // FIXME
            Y_ENSURE(File_.Write(buf.Data(), BlockSize_, BlockSize_ * blockIdx) == BlockSize_);
        }

        size_t GetSizeInBlocks() const {
            size_t byteSize = File_.GetSize();
            Y_ENSURE(byteSize % BlockSize_ == 0);
            return byteSize / BlockSize_;
        }

        size_t GetSizeInBytes() const {
            size_t byteSize = File_.GetSize();
            Y_ENSURE(byteSize % BlockSize_ == 0);
            return byteSize;
        }

        size_t GetBlockSize() const {
            return BlockSize_;
        }

        void TruncateInBlocks(size_t blockCount) {
            File_.Truncate(blockCount * BlockSize_);
        }

    private:
        TDirectIoFile File_;
        size_t BlockSize_{};
    };

    class TCachedBlockFile {
    public:
        TCachedBlockFile(TBlockDirectIoFile& file)
            : File_(file)
        {
        }

        ~TCachedBlockFile() {
            Flush();
        }

    private:
        struct TRawBlock {
            TNaiveSpinLock Lock;
            TCondVar CondVar;
            TFixedBuffer Buf = TFixedBuffer::Empty();
            bool DataLoaded = false;
            bool Dirty = false;
            ui32 InModify = 0;
            bool Flushing = false;
        };

        using TCache = THashMap<ui32, TRawBlock>;
        using TRawBlockPtr = TCache::TValuePtr;

    public:

        template <bool Mutable>
        class TPage {
        public:
            TPage(TRawBlockPtr page)
                : Page_(std::move(page))
            {
            }

            ~TPage() {
                auto g = MakeGuard(Page_->Lock);
                if (Mutable) {
                    if (--Page_->InModify == 0) {
                        Page_->CondVar.NotifyAll();
                    }
                }
            }

            TPage(TPage&& other) noexcept {
                Page_.Swap(other.Page_);
            }

            std::conditional_t<Mutable, TFixedBuffer&, const TFixedBuffer&> Buf() const {
                return Page_->Buf;
            }

        private:
            TRawBlockPtr Page_{};
        };

        TPage<false> GetBlock(size_t blockIdx) {
            TPage<false> ret{GetBlockImpl(blockIdx, false)};
            return ret;
        }

        TPage<true> GetMutableBlock(size_t blockIdx) {
            TPage<true> ret{GetBlockImpl(blockIdx, true)};
            return ret;
        }

    private:
        TRawBlockPtr GetBlockImpl(size_t blockIdx, bool modify) {
            TRawBlockPtr page{};
            {
                page = Cache_[blockIdx];
            }

            auto guard = MakeGuard(page->Lock);
            if (page->Buf.Size() == 0) {
                page->Buf = TFixedBuffer::Aligned(File_.GetBlockSize()); // TODO BETTER
            }
            if (!page->DataLoaded) {
                File_.ReadBlock(page->Buf, blockIdx);
                page->DataLoaded = true;
            }
            if (modify) {
                while (page->Flushing) {
                    page->CondVar.Wait(page->Lock);
                }
                page->Dirty = true;
                ++page->InModify; // we want simultaneously modify different inodes in same block
            }
            return page;
        }

        void Flush() {
            Cache_.Iterate([this](ui32 blockIdx, TRawBlock& block) {
                if (block.Dirty) {
                    File_.WriteBlock(block.Buf, blockIdx);
                    block.Dirty = false;
                }
            });
        }

    private:
        TBlockDirectIoFile& File_;
        THashMap<ui32, TRawBlock> Cache_;
    };

    class TCachedBlockFileRegion {
    public:
        TCachedBlockFileRegion(TCachedBlockFile& file, size_t offsetInBlocks = 0)
            : File_(file)
            , Offset_(offsetInBlocks)
        {
        }

        auto GetBlock(size_t blockIdx) {
            return File_.GetBlock(blockIdx + Offset_);
        }
        auto GetMutableBlock(size_t blockIdx) {
            return File_.GetMutableBlock(blockIdx + Offset_);
        }

    private:
        TCachedBlockFile& File_;
        size_t Offset_ = 0;
    };

}
