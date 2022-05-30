#include "volume.h"
#include "saveload.h"

namespace NJK {

    Y_DEFINE_SERIALIZATION(TVolume::TSuperBlock,
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

    Y_DEFINE_SERIALIZATION(TVolume::TBlockGroupDescr,
        D.CreationTime,
        D.FreeInodeCount,
        D.FreeDataBlockCount,
        D.DirectoryCount
    )

    Y_DEFINE_SERIALIZATION(TVolume::TInode,
        CreationTime, ModTime,
        Val.Type, Val.BlockCount, Val.FirstBlockId, Val.Deadline,
        Dir.HasChildren, Dir.BlockCount, Dir.FirstBlockId,
        Data,
        TSkipMe{ToSkip}
    )


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

    std::optional<TVolume::TInode> TVolume::TInodeDataOps::LookupChild(TInode& parent, const std::string& name) {
        if (!parent.Dir.HasChildren) {
            return {};
        }
        Y_ENSURE(parent.Dir.BlockCount != 0);

        auto data = Group_.ReadDataBlock(parent.Dir.FirstBlockId);
        auto children = DeserializeDirectoryEntries(data.Buf); // TODO We can lookup without full deserialization

        // TODO Optimize
        auto it = std::find_if(children.begin(), children.end(), [&](const TDirEntry& c) { return c.Name == name; });
        if (it == children.end()) {
            return {};
        }

        auto child = *it;
        return Group_.ReadInode(child.Id);
    }

    // TODO OPTIMIZE, Do it in less movements
    TVolume::TInode TVolume::TInodeDataOps::EnsureChild(TInode& parent, const std::string& name) {
        auto child = LookupChild(parent, name);
        if (child) {
            return *child;
        }
        return AddChild(parent, name);
    }

    void TVolume::TInodeDataOps::SetValue(TInode& inode, const TValue& value, const ui32 deadline) {
        if (const auto* v = std::get_if<std::monostate>(&value)) {
            UnsetValue(inode);
            return;
        }

        auto data = inode.Val.BlockCount
            ?  Group_.ReadDataBlock(inode.Val.FirstBlockId)
            :  Group_.AllocateDataBlock();

        const auto type = static_cast<TInode::EType>(value.index());
        inode.Val.Type = type;

        if (!inode.Val.BlockCount) {
            inode.Val.BlockCount = 1;
            inode.Val.FirstBlockId = data.Id;
        }
        Group_.WriteInode(inode); // TODO Until we have cache

        TBufOutput out(data.Buf);
        if (const auto* v = std::get_if<ui32>(&value)) {
            Serialize(out, *v);
        } else if (const auto* v = std::get_if<float>(&value)) {
            Serialize(out, *v);
        } else if (const auto* v = std::get_if<std::string>(&value)) {
            ui16 len = v->size();
            Serialize(out, len);
            out.Save(v->data(), len);
        } else {
            Y_FAIL("todo");
        }
        Group_.WriteDataBlock(data);
    }

    TVolume::TInodeDataOps::TValue TVolume::TInodeDataOps::GetValue(const TInode& inode) {
        if (inode.Val.Type == TInode::EType::Undefined) {
            Y_ENSURE(!inode.Val.BlockCount);
            return {};
        }
        Y_ENSURE(inode.Val.BlockCount);

        auto data = Group_.ReadDataBlock(inode.Val.FirstBlockId);

        using EType = TInode::EType;

        TValue ret;
        TBufInput in(data.Buf);
        switch (inode.Val.Type) {
        case EType::Ui32: {
            ui32 val = 0;
            Deserialize(in, val);
            ret = val;
        }
            break;
        case EType::Float: {
            float val = 0;
            Deserialize(in, val);
            ret = val;
        }
            break;
        case EType::String: {
            std::string val;
            ui16 len = 0;
            Deserialize(in, len);
            val.resize(len);
            in.Load(val.data(), len);
            ret = val;
        }
            break;
        default:
            Y_FAIL("TODO");
        };

        return ret;
    }

    void TVolume::TInodeDataOps::UnsetValue(TInode& inode) {
        if (inode.Val.Type == TInode::EType::Undefined) {
            Y_ENSURE(!inode.Val.BlockCount);
            Y_ENSURE(inode.Val.FirstBlockId == 0);
            return;
        }
        Y_ENSURE(inode.Val.BlockCount);

        // TODO Why to read for deallocate?
        auto data = Group_.ReadDataBlock(inode.Val.FirstBlockId);
        Group_.DeallocateDataBlock(data);

        inode.Val.Type = TInode::EType::Undefined;
        inode.Val.BlockCount = 0;
        inode.Val.FirstBlockId = 0;
        Group_.WriteInode(inode); // TODO Until we have cache
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