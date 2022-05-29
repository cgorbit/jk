#include "volume.h"
#include "saveload.h"

namespace NJK {

    void TVolume::TSuperBlock::Serialize(IOutputStream& out) const {
        ::NJK::Serialize(out, BlockSize);
        ::NJK::Serialize(out, BlockGroupCount);
        ::NJK::Serialize(out, MaxBlockGroupCount);
        ::NJK::Serialize(out, BlockGroupSize);
        ::NJK::Serialize(out, BlockGroupDescriptorsBlockCount);
        ::NJK::Serialize(out, MetaGroupCount); // File count
        ::NJK::Serialize(out, MaxFileSize);
        ::NJK::Serialize(out, ZeroBlockGroupOffset);
        ::NJK::Serialize(out, BlockGroupInodeCount);
        ::NJK::Serialize(out, BlockGroupDataBlockCount);
        ::NJK::Serialize(out, MetaGroupInodeCount);
    }

    void TVolume::TSuperBlock::Deserialize(IInputStream& in) {
        DeserializeMany(in,
            BlockSize,
            BlockGroupCount,
            MaxBlockGroupCount,
            BlockGroupSize,
            BlockGroupDescriptorsBlockCount,
            MetaGroupCount, // File count
            MaxFileSize,
            ZeroBlockGroupOffset,
            BlockGroupInodeCount,
            BlockGroupDataBlockCount,
            MetaGroupInodeCount
        );
    }

    void TVolume::TBlockGroupDescr::Serialize(IOutputStream& out) const {
        ::NJK::Serialize(out, D.CreationTime);
        ::NJK::Serialize(out, D.FreeInodeCount);
        ::NJK::Serialize(out, D.FreeDataBlockCount);
        ::NJK::Serialize(out, D.DirectoryCount);
    }
    void TVolume::TBlockGroupDescr::Deserialize(IInputStream& in) {
        ::NJK::Deserialize(in, D.CreationTime);
        ::NJK::Deserialize(in, D.FreeInodeCount);
        ::NJK::Deserialize(in, D.FreeDataBlockCount);
        ::NJK::Deserialize(in, D.DirectoryCount);
    }


    /*
        TBlockGroup
    */

    TVolume::TBlockGroup::TBlockGroup(size_t inFileOffset, ui32 inodeOffset, TDirectIoFile& file, const TSuperBlock& sb)
        : SuperBlock(&sb)
        , File_(file, sb.BlockSize, inFileOffset)
        , InodeIndexOffset(inodeOffset)
        , InodesBitmap(SuperBlock->NewBuffer())
        , DataBlocksBitmap(SuperBlock->NewBuffer())
    {
        File_.ReadBlock(InodesBitmap.Buf(), InodesBitmapBlockIndex);
        File_.ReadBlock(DataBlocksBitmap.Buf(), DataBlocksBitmapBlockIndex);
    }

    TVolume::TBlockGroup::~TBlockGroup() {
        Flush();
    }

    TVolume::TInode TVolume::TBlockGroup::AllocateInode() {
        i32 idx = InodesBitmap.FindUnset();
        Y_ENSURE(idx != -1);
        InodesBitmap.Set(idx);

        TInode inode;
        inode.Id = idx + InodeIndexOffset;
        return inode;
    }

    void TVolume::TBlockGroup::DeallocateInode(const TInode& inode) {
        // TODO Y_ASSERT
        auto idx = inode.Id - InodeIndexOffset;
        Y_ENSURE(InodesBitmap.Test(idx));
        InodesBitmap.Unset(idx);

        // FIXME No inode on disk modification here
    }

    TVolume::TInode TVolume::TBlockGroup::ReadInode(ui32 id) {
        // TODO Block Cache
        auto buf = SuperBlock->NewBuffer();
        File_.ReadBlock(buf, CalcInodeBlockIndex(id));

        TBufInput in(buf);
        in.SkipRead(CalcInodeInBlockOffset(id));
        TInode inode;
        inode.Deserialize(in);
        inode.Id = id;
        return inode;
    }

    void TVolume::TBlockGroup::WriteInode(const TInode& inode) {
        // TODO Block Cache
        auto buf = SuperBlock->NewBuffer();
        File_.ReadBlock(buf, CalcInodeBlockIndex(inode.Id));

        TBufOutput out(buf);
        out.SkipWrite(CalcInodeInBlockOffset(inode.Id));
        inode.Serialize(out);

        File_.WriteBlock(buf, CalcInodeBlockIndex(inode.Id));
    }
}