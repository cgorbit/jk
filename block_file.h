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
            std::mutex Lock;
            std::condition_variable CondVar;
            TFixedBuffer Buf = TFixedBuffer::Empty();
            size_t RefCount = 0;
            bool DataLoaded = false;
            bool Dirty = false;
            ui32 InModify = 0;
            bool Flushing = false;
        };

    public:

        template <bool Mutable>
        class TPage {
        public:
            TPage(TRawBlock* page)
                : Page_(page)
            {
            }

            ~TPage() {
                std::lock_guard g(Page_->Lock);
                if (Mutable) {
                    if (--Page_->InModify == 0) {
                        Page_->CondVar.notify_all();
                    }
                }
                --Page_->RefCount;
            }

            std::conditional_t<Mutable, TFixedBuffer&, const TFixedBuffer&> Buf() const {
                return Page_->Buf;
            }

        private:
            TRawBlock* Page_{};
        };

        TPage<false> GetBlock(size_t blockIdx) {
            TPage<false> ret{GetBlockImpl(blockIdx, false)};
            //std::cerr << "+ GetBlock(" << blockIdx << ") => " << (void*)ret.Buf().Data() << '\n';
            return ret;
        }

        TPage<true> GetMutableBlock(size_t blockIdx) {
            TPage<true> ret{GetBlockImpl(blockIdx, true)};
            //std::cerr << "+ GetMutableBlock(" << blockIdx << ") => " << (void*)ret.Buf().Data() << '\n';
            return ret;
        }

    private:
        TRawBlock* GetBlockImpl(size_t blockIdx, bool modify) {
            TRawBlock* page{};
            {
                std::lock_guard g(Lock_);
                page = &Cache_[blockIdx];
            }

            std::unique_lock guard(page->Lock);
            if (page->Buf.Size() == 0) {
                page->Buf = TFixedBuffer::Aligned(File_.GetBlockSize()); // TODO BETTER
                //std::cerr << "++ Allocated Page: " << (void*)page->Buf.Data() << "\n";
            }
            ++page->RefCount;
            if (!page->DataLoaded) {
                File_.ReadBlock(page->Buf, blockIdx);
                page->DataLoaded = true;
            }
            if (modify) {
                while (page->Flushing) {
                    page->CondVar.wait(guard);
                }
                page->Dirty = true;
                ++page->InModify; // we want simultaneously modify different inodes in same block
            }
            return page;
        }

        void Flush() {
            for (auto& [blockIdx, block] : Cache_) {
                if (block.Dirty) {
                    //std::cerr << "+ write dirty block " << blockIdx << '\n';
                    File_.WriteBlock(block.Buf, blockIdx);
                    block.Dirty = false;
                } else {
                    //std::cerr << "+ skip clean block " << blockIdx << '\n';
                }
            }
        }

    private:
        TBlockDirectIoFile& File_;

        std::mutex Lock_;
        std::unordered_map<ui32, TRawBlock> Cache_;
        // TODO
        //THashMap<ui32, TRawBlock> Cache_;
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