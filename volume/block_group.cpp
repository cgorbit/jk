#include "block_group.h"

#include "../saveload.h"
#include "../block_file.h"

namespace NJK::NVolume {

    Y_DEFINE_SERIALIZATION(TBlockGroupDescr,
        D.CreationTime,
        D.FreeInodeCount,
        D.FreeDataBlockCount,
        D.DirectoryCount
    )

    /*
        TBlockGroup
    */

    TBlockGroup::TBlockGroup(size_t inFileOffset, ui32 inodeOffset, TCachedBlockFile& file, const TSuperBlock& sb, const TBlockGroupDescr& descr)
        : SuperBlock(&sb)
        , File_(file, inFileOffset)
        , InodeIndexOffset(inodeOffset)
        , DataBlockIndexOffset(inodeOffset)
        , Inodes{
            .FreeCount = descr.D.FreeInodeCount, // FIXME Locking
            .Bitmap{NewBuffer()}
        }
        , DataBlocks{
            .FreeCount = descr.D.FreeDataBlockCount,
            .Bitmap{NewBuffer()}
        }
    {
        Y_ENSURE(SuperBlock->BlockGroupInodeCount == SuperBlock->BlockGroupDataBlockCount);

        Inodes.CopyFrom(File_.GetBlock(InodesBitmapBlockIndex).Buf());
        DataBlocks.CopyFrom(File_.GetBlock(DataBlocksBitmapBlockIndex).Buf());
    }

    TBlockGroup::~TBlockGroup() {
        Flush();
    }

    /*
        Inode management
    */
    ui32 TBlockGroup::TAllocatableItems::GetFreeCount() {
        std::unique_lock g(Lock_);
        return FreeCount;
    }

    i32 TBlockGroup::TAllocatableItems::TryAllocate() {
        std::unique_lock g(Lock_);

        if (!FreeCount) {
            return -1;
        }
        --FreeCount;

        const i32 idx = Bitmap.FindUnset();
        Y_VERIFY(idx != -1); // For now (we use mutual exclusion) this is impossible
        if (idx == -1) {
            return -1;
        }
        Y_ASSERT(!Bitmap.Test(idx));
        Bitmap.Set(idx);
        Y_ASSERT(Bitmap.Test(idx));

        return idx;
    }

    void TBlockGroup::TAllocatableItems::Deallocate(ui32 idx) {
        std::unique_lock g(Lock_);
        ++FreeCount;
        Y_ASSERT(Bitmap.Test(idx));
        Bitmap.Unset(idx);
        Y_ASSERT(!Bitmap.Test(idx));
    }

    std::optional<TInode> TBlockGroup::TryAllocateInode() {
        //if (!TryAllocate(FreeInodeCount)) {
        //    throw std::runtime_error("Not implemented, failed to allocate");
        //}

        i32 idx = Inodes.TryAllocate();
        if (idx == -1) {
            return {};
        }

        TInode inode;
        inode.Id = idx + InodeIndexOffset;
        WriteInode(inode); // FIXME Don't write?

        return inode;
    }

    void TBlockGroup::DeallocateInode(const TInode& inode) {
        auto idx = inode.Id - InodeIndexOffset;
        Inodes.Deallocate(idx);
    }

    TInode TBlockGroup::ReadInode(ui32 id) {
        auto block = File_.GetBlock(CalcInodeBlockIndex(id));
        TBufInput in(block.Buf());
        //std::cerr << "+ ReadInode: ID=" << id << ", page=" << (void*)block.Buf().Data()
            //<< ", offset=" << CalcInodeInBlockOffset(id) << '\n';
        in.SkipRead(CalcInodeInBlockOffset(id));
        TInode inode;
        inode.Deserialize(in);
        inode.Id = id;
        return inode;
    }

    void TBlockGroup::WriteInode(const TInode& inode) {
        auto block = File_.GetMutableBlock(CalcInodeBlockIndex(inode.Id));
        TBufOutput out(block.Buf());
        out.SkipWrite(CalcInodeInBlockOffset(inode.Id));
        //std::cerr << "+ WriteInode: ID=" << inode.Id << ", page=" << (void*)block.Buf().Data()
            //<< ", offset=" << CalcInodeInBlockOffset(inode.Id) << '\n';
        inode.Serialize(out);
    }

    /*
        Data Block management

        TODO Generalize management of data and inodes into separate class
    */

    i32 TBlockGroup::TryAllocateDataBlock() {
        //if (!TryAllocate(FreeDataBlockCount)) {
        //    throw std::runtime_error("Not implemented, failed to allocate");
        //}


        i32 idx = DataBlocks.TryAllocate();
        if (idx == -1) {
            return {};
        }

        const ui32 id = idx + DataBlockIndexOffset;
        return id;
    }

    void TBlockGroup::DeallocateDataBlock(ui32 id) {
        auto idx = id - DataBlockIndexOffset;
        DataBlocks.Deallocate(idx);
        // FIXME No block on disk modification here
    }

    TCachedBlockFile::TPage<false> TBlockGroup::GetDataBlock(ui32 id) {
        return File_.GetBlock(CalcDataBlockIndex(id));
    }
    TCachedBlockFile::TPage<true> TBlockGroup::GetMutableDataBlock(ui32 id) {
        return File_.GetMutableBlock(CalcDataBlockIndex(id));
    }

    //void TBlockGroup::WriteDataBlock(const TDataBlock& block) {
    //    Y_FAIL("");
    //    // TODO Block Cache
    //    //File_.WriteBlock(block.Buf, CalcDataBlockIndex(block.Id));
    //}

}