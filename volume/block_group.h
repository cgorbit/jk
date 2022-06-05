#pragma once

#include "super_block.h"
#include "inode.h"

#include "../common.h"
#include "../bitset.h"
#include "../saveload.h"
#include "../block_file.h"

namespace NJK::NVolume {

    struct TBlockGroupDescr {
        // in-memory only
        //ui32 Location = 0;

        // On Disk
        struct {
            ui32 CreationTime = 0;
            ui32 FreeInodeCount = 0; // FIXME
            ui32 FreeDataBlockCount = 0; // FIXME
            ui32 DirectoryCount = 0; // FIXME
        } D;

        static constexpr ui32 OnDiskSize = 16; // TODO
        static_assert(512 % OnDiskSize == 0);

        Y_DECLARE_SERIALIZATION
    };

    class TBlockGroup {
    public:
        TBlockGroup(size_t inFileOffset, ui32 inodeOffset, TCachedBlockFile& file, const TSuperBlock& sb);
        ~TBlockGroup();

        ui32 GetFreeInodeCount() {
            return Inodes.GetFreeCount();
        }
        std::optional<TInode> TryAllocateInode();
        void DeallocateInode(const TInode& inode);

        TInode ReadInode(ui32 id);
        void WriteInode(const TInode& inode);

        ui32 GetFreeDataBlockCount() {
            return DataBlocks.GetFreeCount();
        }
        i32 TryAllocateDataBlock();
        void DeallocateDataBlock(ui32);

        TCachedBlockFile::TPage<false> GetDataBlock(ui32 id);
        TCachedBlockFile::TPage<true> GetMutableDataBlock(ui32 id);
        //void WriteDataBlock(const TDataBlock&);

    private:
        TFixedBuffer NewBuffer() {
            return SuperBlock->NewBuffer();
        }

        ui32 CalcInodeInBlockOffset(ui32 inodeId) const {
            return (inodeId - InodeIndexOffset) * TInode::OnDiskSize % SuperBlock->BlockSize;
        }
        ui32 CalcInodeBlockIndex(ui32 inodeId) const {
            return 2 + (inodeId - InodeIndexOffset) * TInode::OnDiskSize / SuperBlock->BlockSize;
        }

        ui32 CalcDataBlockIndex(ui32 blockId) const {
            const ui32 inodeBlocks = SuperBlock->BlockGroupInodeCount * TInode::OnDiskSize / SuperBlock->BlockSize;
            const ui32 ret = 2 + inodeBlocks + (blockId - DataBlockIndexOffset);
            //std::cerr << "+++ CalcDataBlockIndex for " << blockId << ", indeBlocks = " << inodeBlocks
                //<< ", result = " << ret << "\n";
            return ret;
        }

        void Flush() {
            Inodes.Bitmap.Buf().CopyTo(File_.GetMutableBlock(InodesBitmapBlockIndex).Buf());
            DataBlocks.Bitmap.Buf().CopyTo(File_.GetMutableBlock(DataBlocksBitmapBlockIndex).Buf());
        }

        static constexpr size_t InodesBitmapBlockIndex = 0;
        static constexpr size_t DataBlocksBitmapBlockIndex = 1;

        const TSuperBlock* SuperBlock{};
        TCachedBlockFileRegion File_;

        // in-memory only
        //ui32 InFileOffset = 0;
        const ui32 InodeIndexOffset = 0;
        const ui32 DataBlockIndexOffset = 0;

        struct TAllocatableItems {
            std::mutex Lock_;
            size_t FreeCount = 0;
            TBlockBitSet Bitmap; // 4096 bytes

            ui32 GetFreeCount();
            i32 TryAllocate();
            void Deallocate(ui32);
        };
        
        TAllocatableItems Inodes;
        TAllocatableItems DataBlocks;
    };

}