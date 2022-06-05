#include "meta_group.h"

#include "../atomic.h"

namespace NJK::NVolume {

    TMetaGroup::TMetaGroup(const std::string& file, const TSuperBlock& sb)
        : SuperBlock(&sb)
        , FileName(file)
        , RawFile(FileName, SuperBlock->BlockSize)
        , File(RawFile)
    {
        TotalFreeInodeCount_ = SuperBlock->MetaGroupInodeCount;
        TotalFreeDataBlockCount_ = SuperBlock->MetaGroupDataBlockCount;

        BlockGroupDescrs_.resize(SuperBlock->MaxBlockGroupCount); // FIXME Better
        BlockGroups_.resize(SuperBlock->MaxBlockGroupCount); // FIXME Better

        if (RawFile.GetSizeInBlocks() == 0) {
            RawFile.TruncateInBlocks(CalcExpectedFileSize(0) / SuperBlock->BlockSize); // TODO Better
            SaveBlockGroupDescriptors();
        }
        LoadBlockGroupDescriptors();
        VerifyFile();
    }

    TBlockGroup& TMetaGroup::GetInodeBlockGroup(ui32 id) {
        const size_t bgIndex = (id % SuperBlock->MetaGroupInodeCount) / SuperBlock->BlockGroupInodeCount;
        return *BlockGroups_[bgIndex];
    }

    TBlockGroup& TMetaGroup::GetInodeBlockGroup(const TInode& inode) {
        return GetInodeBlockGroup(inode.Id);
    }

    TBlockGroup& TMetaGroup::GetDataBlockGroup(ui32 id) {
        const size_t bgIndex = (id % SuperBlock->MetaGroupDataBlockCount) / SuperBlock->BlockGroupDataBlockCount;
        return *BlockGroups_[bgIndex];
    }

    TMetaGroup::~TMetaGroup() {
        UpdateBlockGroupDescriptors();
        SaveBlockGroupDescriptors();
    }

    void TMetaGroup::AllocateNewBlockGroup() {
        Y_VERIFY(AliveBlockGroupCount_ < SuperBlock->MaxBlockGroupCount);

        const auto blockGroupIdx = AliveBlockGroupCount_.load();

        auto& descr = BlockGroupDescrs_[blockGroupIdx];
        descr.D.CreationTime = NowSeconds();
        descr.D.FreeInodeCount = SuperBlock->BlockGroupInodeCount;
        descr.D.FreeDataBlockCount = SuperBlock->BlockGroupDataBlockCount;

        RawFile.TruncateInBlocks(CalcExpectedFileSize(blockGroupIdx + 1) / SuperBlock->BlockSize); // TODO Better

        BlockGroups_[blockGroupIdx] = CreateBlockGroup(blockGroupIdx);

        ++AliveBlockGroupCount_;

        ExistingFreeInodeCount_ += SuperBlock->BlockGroupInodeCount;
        ExistingFreeDataBlockCount_ += SuperBlock->BlockGroupDataBlockCount;
    }

    void TMetaGroup::LoadBlockGroupDescriptors() {
        // TODO Not only one block (for general case)
        auto block = File.GetBlock(0);
        TBufInput in(block.Buf());
        DeserializeFixedVector(in, BlockGroupDescrs_);

        for (const auto& bg : BlockGroupDescrs_) {
            if (!bg.D.CreationTime) {
                break;
            }
            // TODO Better, lazy, this code ugly
            BlockGroups_[AliveBlockGroupCount_] = CreateBlockGroup(AliveBlockGroupCount_);

            //Y_TODO("TEST THESE counters");
            ++AliveBlockGroupCount_;

            ExistingFreeInodeCount_ += bg.D.FreeInodeCount;
            ExistingFreeDataBlockCount_ += bg.D.FreeDataBlockCount;

            TotalFreeInodeCount_ -= SuperBlock->BlockGroupInodeCount - bg.D.FreeInodeCount;
            TotalFreeDataBlockCount_ -= SuperBlock->BlockGroupDataBlockCount - bg.D.FreeDataBlockCount;

            // TODO Check for holes
        }
    }

    std::optional<TInode> TMetaGroup::TryAllocateInode() {
        if (!TrySub(TotalFreeInodeCount_)) {
            return {};
        }

        while (!TrySub(ExistingFreeInodeCount_)) {
            std::unique_lock g(Lock_);
            if (TrySub(ExistingFreeInodeCount_)) {
                break;
            }
            AllocateNewBlockGroup();
        }

        // BlockGroups_ vector can't be resized
        const size_t alive = AliveBlockGroupCount_.load();
        while (true) {
            auto inode = BlockGroups_[0]->TryAllocateInode();
            if (inode) {
                return inode;
            }
            // Make some probabalistic approach for index here
            // for example for some probability start form the
            // last BlockGroup and then use TLS shuffled array
            // with indexes to traverse through all BlockGroups_
            for (size_t i = 0; i < alive; ++i) {
                auto& bg = *BlockGroups_[i];
                auto inode = bg.TryAllocateInode();
                if (inode) {
                    return inode;
                }
            }
        }
    }

    void TMetaGroup::DeallocateInode(const TInode& inode) {
        const size_t bgIndex = (inode.Id % SuperBlock->MetaGroupInodeCount) / SuperBlock->BlockGroupInodeCount;
        BlockGroups_[bgIndex]->DeallocateInode(inode);
        ++ExistingFreeInodeCount_;
        ++TotalFreeInodeCount_;
    }

    i32 TMetaGroup::TryAllocateDataBlock(const TInode& owner) {
        return DoTryAllocateDataBlock(&owner);
    }

    i32 TMetaGroup::TryAllocateDataBlock() {
        return DoTryAllocateDataBlock(nullptr);
    }

    i32 TMetaGroup::DoTryAllocateDataBlock(const TInode* owner) {
        if (!TrySub(TotalFreeDataBlockCount_)) {
            return {};
        }

        while (!TrySub(ExistingFreeDataBlockCount_)) {
            std::unique_lock g(Lock_);
            if (TrySub(ExistingFreeDataBlockCount_)) {
                break;
            }
            AllocateNewBlockGroup();
        }

        if (owner) {
            i32 id = GetInodeBlockGroup(*owner).TryAllocateDataBlock();
            if (id != -1) {
                return id;
            }
        }

        // BlockGroups_ vector can't be resized
        const size_t alive = AliveBlockGroupCount_.load();
        while (true) {
            const i32 id = BlockGroups_[alive - 1]->TryAllocateDataBlock();
            if (id != -1) {
                return id;
            }
            // Make some probabalistic approach for index here
            // for example for some probability start form the
            // last BlockGroup and then use TLS shuffled array
            // with indexes to traverse through all BlockGroups_
            for (size_t i = 0; i < alive; ++i) {
                auto& bg = *BlockGroups_[i];
                i32 id = bg.TryAllocateDataBlock();
                if (id != -1) {
                    return id;
                }
            }
        }
    }

    void TMetaGroup::DeallocateDataBlock(ui32 id) {
        GetDataBlockGroup(id).DeallocateDataBlock(id);
    }

    TInode TMetaGroup::ReadInode(ui32 id) {
        return GetInodeBlockGroup(id).ReadInode(id);
    }

    void TMetaGroup::WriteInode(const TInode& inode) {
        GetInodeBlockGroup(inode).WriteInode(inode);
    }

    TCachedBlockFile::TPage<false> TMetaGroup::GetDataBlock(ui32 id) {
        return GetDataBlockGroup(id).GetDataBlock(id);
    }

    TCachedBlockFile::TPage<true> TMetaGroup::GetMutableDataBlock(ui32 id) {
        return GetDataBlockGroup(id).GetMutableDataBlock(id);
    }

    void TMetaGroup::UpdateBlockGroupDescriptors() {
        size_t alive = AliveBlockGroupCount_.load();
        for (size_t i = 0; i < alive; ++i) {
            auto& descr = BlockGroupDescrs_[i];
            descr.D.FreeInodeCount = BlockGroups_[i]->GetFreeInodeCount();
            descr.D.FreeDataBlockCount = BlockGroups_[i]->GetFreeDataBlockCount();
        }
    }

    void TMetaGroup::SaveBlockGroupDescriptors() {
        // TODO Not only one block (for general case)
        auto block = File.GetMutableBlock(0);
        TBufOutput out(block.Buf());
        SerializeFixedVector(out, BlockGroupDescrs_);
    }

    std::unique_ptr<TBlockGroup> TMetaGroup::CreateBlockGroup(ui32 blockGroupIdx) {
        return std::make_unique<TBlockGroup>(
            CalcBlockGroupOffset(blockGroupIdx) / SuperBlock->BlockSize,
            blockGroupIdx * SuperBlock->BlockGroupInodeCount, // TODO Add MetaGroup Inode offset
            File,
            *SuperBlock,
            BlockGroupDescrs_[blockGroupIdx]
        );
    }

    void TMetaGroup::VerifyFile() {
        //auto expectedSize = CalcExpectedFileSize();
        //std::cerr << "+ file size: " << FileName << ": " << RawFile.GetSizeInBytes() << '\n';
        //std::cerr << "+ expected size: " << expectedSize << '\n';
        Y_VERIFY(RawFile.GetSizeInBytes() == SuperBlock->ZeroBlockGroupOffset + AliveBlockGroupCount_ * SuperBlock->BlockGroupSize);
    }

}