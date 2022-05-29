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
        , DataBlockIndexOffset(inodeOffset)
        , InodesBitmap(NewBuffer())
        , DataBlocksBitmap(NewBuffer())
    {
        InodesBitmap.Buf().FillZeroes();
        DataBlocksBitmap.Buf().FillZeroes();

        Y_ENSURE(SuperBlock->BlockGroupInodeCount == SuperBlock->BlockGroupDataBlockCount);
        File_.ReadBlock(InodesBitmap.Buf(), InodesBitmapBlockIndex);
        File_.ReadBlock(DataBlocksBitmap.Buf(), DataBlocksBitmapBlockIndex);
    }

    TVolume::TBlockGroup::~TBlockGroup() {
        Flush();
    }

    /*
        Inode management
    */
    TVolume::TInode TVolume::TBlockGroup::AllocateInode() {
        const i32 idx = InodesBitmap.FindUnset();
        Y_ENSURE(idx != -1);
        InodesBitmap.Set(idx);

        TInode inode;
        inode.Id = idx + InodeIndexOffset;

        WriteInode(inode); // TODO Until we have cache

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
        auto buf = NewBuffer();
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
        auto buf = NewBuffer();
        File_.ReadBlock(buf, CalcInodeBlockIndex(inode.Id));

        TBufOutput out(buf);
        out.SkipWrite(CalcInodeInBlockOffset(inode.Id));
        inode.Serialize(out);

        File_.WriteBlock(buf, CalcInodeBlockIndex(inode.Id));
    }

    /*
        Data Block management

        TODO Generalize management of data and inodes into separate class
    */

    TVolume::TDataBlock TVolume::TBlockGroup::AllocateDataBlock() {
        const i32 idx = DataBlocksBitmap.FindUnset();
        Y_ENSURE(idx != -1);
        DataBlocksBitmap.Set(idx);

        TDataBlock block{.Buf = NewBuffer()};
        block.Id = idx + DataBlockIndexOffset;
        block.Buf.FillZeroes();

        WriteDataBlock(block); // TODO Until we have cache

        return block;
    }

    void TVolume::TBlockGroup::DeallocateDataBlock(const TDataBlock& block) {
        // TODO Y_ASSERT
        auto idx = block.Id - DataBlockIndexOffset;
        Y_ENSURE(DataBlocksBitmap.Test(idx));
        DataBlocksBitmap.Unset(idx);

        // FIXME No block on disk modification here
    }

    TVolume::TDataBlock TVolume::TBlockGroup::ReadDataBlock(ui32 id) {
        // TODO Block Cache
        auto buf = NewBuffer();
        File_.ReadBlock(buf, CalcDataBlockIndex(id));
        TDataBlock block{.Buf = std::move(buf)};
        block.Id = id;
        return block;
    }

    void TVolume::TBlockGroup::WriteDataBlock(const TDataBlock& block) {
        // TODO Block Cache
        File_.WriteBlock(block.Buf, CalcDataBlockIndex(block.Id));
    }


    /*
        TInodeDataOps
    */

    TVolume::TInodeDataOps::TInodeDataOps(TBlockGroup& group)
        : Group_(group)
    {
    }

    TVolume::TInode TVolume::TInodeDataOps::AddChild(TInode& parent, const std::string& name) {
        // TODO FIXME Don't use Zero Id for Inodes and for Data Blocks -- start from One

        if (parent.Dir.HasChildren) {
            Y_ENSURE(parent.Dir.BlockCount != 0);
            auto data = Group_.ReadDataBlock(parent.Dir.FirstBlockId);
            auto children = DeserializeDirectoryEntries(data.Buf);

            if (std::any_of(children.begin(), children.end(), [&](const TDirEntry& c) { return c.Name == name; })) {
                throw std::runtime_error("Already has child");
            }

            auto child = Group_.AllocateInode();
            children.push_back({child.Id, name});

            SerializeDirectoryEntries(data.Buf, children);
            Group_.WriteDataBlock(data);
            return child;
        } else {
            auto child = Group_.AllocateInode();
            auto data = Group_.AllocateDataBlock();
            SerializeDirectoryEntries(data.Buf, {{child.Id, name}});
            Group_.WriteDataBlock(data);

            parent.Dir.HasChildren = true;
            parent.Dir.BlockCount = 1;
            parent.Dir.FirstBlockId = data.Id;
            Group_.WriteInode(parent);
            return child;
        }
    }

    void TVolume::TInodeDataOps::RemoveChild(TInode& parent, const std::string& name) {
        Y_ENSURE(parent.Dir.HasChildren);
        Y_ENSURE(parent.Dir.BlockCount != 0);

        auto data = Group_.ReadDataBlock(parent.Dir.FirstBlockId);
        auto children = DeserializeDirectoryEntries(data.Buf);

        // TODO Optimize
        auto it = std::find_if(children.begin(), children.end(), [&](const TDirEntry& c) { return c.Name == name; });
        if (it == children.end()) {
            throw std::runtime_error("Has no such child");
        }
        auto child = *it;
        children.erase(it); // TODO Optimize
        Group_.DeallocateInode(Group_.ReadInode(child.Id)); // FIXME

        if (children.empty()) {
            parent.Dir.HasChildren = false;
            parent.Dir.BlockCount = 0;
            parent.Dir.FirstBlockId = 0;
            Group_.WriteInode(parent);
            Group_.DeallocateDataBlock(data);
        } else {
            SerializeDirectoryEntries(data.Buf, children);
            Group_.WriteDataBlock(data);
        }
    }

    std::vector<TVolume::TInodeDataOps::TDirEntry> TVolume::TInodeDataOps::ListChildren(TInode& parent) {
        if (!parent.Dir.HasChildren) {
            return {};
        }

        Y_ENSURE(parent.Dir.BlockCount != 0);

        auto data = Group_.ReadDataBlock(parent.Dir.FirstBlockId);

        return DeserializeDirectoryEntries(data.Buf);
    }

    TVolume::TInode TVolume::TInodeDataOps::LookupChild(TInode& parent, const std::string& name) {
        Y_FAIL("todo");
        return {};
    }

    void TVolume::TInodeDataOps::SetValue(TInode& parent, const TValue& value, const ui32 deadline) {
        Y_FAIL("todo");
    }

    void TVolume::TInodeDataOps::UnsetValue(TInode& inode) {
        Y_FAIL("todo");
    }

    std::vector<TVolume::TInodeDataOps::TDirEntry> TVolume::TInodeDataOps::DeserializeDirectoryEntries(const TFixedBuffer& buf) {
        TBufInput in(buf);
        ui16 count = 0;
        Deserialize(in, count);
        Y_ENSURE(count > 0);
        std::vector<TDirEntry> ret;
        ret.resize(count);
        for (size_t i = 0; i < count; ++i) {
            auto& entry = ret[i];
            Deserialize(in, entry.Id);
            ui8 len = 0;
            Deserialize(in, len);
            entry.Name.resize(len);
            in.Load(entry.Name.data(), len);
        }
        return ret;
    }

    void TVolume::TInodeDataOps::SerializeDirectoryEntries(TFixedBuffer& buf, const std::vector<TDirEntry>& entries) {
        TBufOutput out(buf);
        ui16 count = entries.size();
        Y_ENSURE(count > 0);
        Serialize(out, count);
        std::vector<TDirEntry> ret;
        for (const auto& entry : entries) {
            Serialize(out, entry.Id);
            ui8 len = entry.Name.size();
            Serialize(out, len);
            out.Save(entry.Name.data(), len);
        }
    }
}