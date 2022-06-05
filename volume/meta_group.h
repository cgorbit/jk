#pragma once

#include "block_group.h"

#include "../datetime.h"
#include <vector>
#include <memory>
#include <mutex>

namespace NJK::NVolume {

    // One data file up to 2 GiB by default
    class TMetaGroup {
    public:
        TMetaGroup(const std::string& file, const TSuperBlock& sb);
        ~TMetaGroup();

        std::optional<TInode> TryAllocateInode();
        void DeallocateInode(const TInode& inode);

        i32 TryAllocateDataBlock(const TInode& owner);
        i32 TryAllocateDataBlock();
        void DeallocateDataBlock(ui32);

        TInode ReadInode(ui32 id);
        void WriteInode(const TInode& inode);

        TCachedBlockFile::TPage<false> GetDataBlock(ui32 id);
        TCachedBlockFile::TPage<true> GetMutableDataBlock(ui32 id);

    private:
        void AllocateNewBlockGroup();
        std::unique_ptr<TBlockGroup> CreateBlockGroup(ui32 blockGroupIdx);
        void LoadBlockGroupDescriptors();
        void VerifyFile();
        void UpdateBlockGroupDescriptors();
        void SaveBlockGroupDescriptors();
        i32 DoTryAllocateDataBlock(const TInode* inode = nullptr);
        TBlockGroup& GetInodeBlockGroup(const TInode& inode);
        TBlockGroup& GetInodeBlockGroup(ui32 id);
        TBlockGroup& GetDataBlockGroup(ui32 id);

        ui32 CalcBlockGroupOffset(ui32 blockGroupIdx) const {
            return SuperBlock->ZeroBlockGroupOffset + blockGroupIdx * SuperBlock->BlockGroupSize;
        }

        ui32 CalcExpectedFileSize(size_t bgCount) const {
            return CalcBlockGroupOffset(bgCount);
        }

    private:
        const TSuperBlock* SuperBlock{};
        std::string FileName;
        TBlockDirectIoFile RawFile;
        TCachedBlockFile File;

        std::atomic<size_t> TotalFreeInodeCount_ = 0;
        std::atomic<size_t> ExistingFreeInodeCount_ = 0;

        std::atomic<size_t> TotalFreeDataBlockCount_ = 0;
        std::atomic<size_t> ExistingFreeDataBlockCount_ = 0;

        //ui32 DirectoryCount = 0; // debug

        TODO_BETTER_CONCURRENCY

        std::mutex Lock_; // guard BlockGroups allocation
        std::atomic<size_t> AliveBlockGroupCount_ = 0;
        std::vector<TBlockGroupDescr> BlockGroupDescrs_;
        std::vector<std::unique_ptr<TBlockGroup>> BlockGroups_;
    };

}