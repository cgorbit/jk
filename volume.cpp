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

    TVolume::TBlockGroup::TBlockGroup(size_t inFileOffset, ui32 inodeOffset, TCachedBlockFile& file, const TSuperBlock& sb)
        : SuperBlock(&sb)
        , File_(file, inFileOffset)
        , InodeIndexOffset(inodeOffset)
        , DataBlockIndexOffset(inodeOffset)
        , InodesBitmap(NewBuffer())
        , DataBlocksBitmap(NewBuffer())
    {
        InodesBitmap.Buf().FillZeroes();
        DataBlocksBitmap.Buf().FillZeroes();

        Y_ENSURE(SuperBlock->BlockGroupInodeCount == SuperBlock->BlockGroupDataBlockCount);

        File_.GetBlock(InodesBitmapBlockIndex).Buf().CopyTo(InodesBitmap.Buf());
        File_.GetBlock(DataBlocksBitmapBlockIndex).Buf().CopyTo(DataBlocksBitmap.Buf());
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

        WriteInode(inode); // FIXME Don't write?

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

    void TVolume::TBlockGroup::WriteInode(const TInode& inode) {
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

    ui32 TVolume::TBlockGroup::AllocateDataBlock() {
        const i32 idx = DataBlocksBitmap.FindUnset();
        Y_ENSURE(idx != -1);
        DataBlocksBitmap.Set(idx);

        //TDataBlock block{.Buf = NewBuffer()};
        //block.Id = idx + DataBlockIndexOffset;
        //block.Buf.FillZeroes();

        //WriteDataBlock(block); // TODO Until we have cache

        return idx;
    }

    void TVolume::TBlockGroup::DeallocateDataBlock(ui32 id) {
        // TODO Y_ASSERT
        auto idx = id - DataBlockIndexOffset;
        Y_ENSURE(DataBlocksBitmap.Test(idx));
        DataBlocksBitmap.Unset(idx);

        // FIXME No block on disk modification here
    }

    TCachedBlockFile::TPage<false> TVolume::TBlockGroup::GetDataBlock(ui32 id) {
        return File_.GetBlock(CalcDataBlockIndex(id));
    }
    TCachedBlockFile::TPage<true> TVolume::TBlockGroup::GetMutableDataBlock(ui32 id) {
        return File_.GetMutableBlock(CalcDataBlockIndex(id));
    }

    //void TVolume::TBlockGroup::WriteDataBlock(const TDataBlock& block) {
    //    Y_FAIL("");
    //    // TODO Block Cache
    //    //File_.WriteBlock(block.Buf, CalcDataBlockIndex(block.Id));
    //}


    /*
        TInodeDataOps
    */

    TVolume::TInodeDataOps::TInodeDataOps(TVolume* volume)
        : Group_(*volume->MetaGroups_[0]->BlockGroups[0])
    {
    }
    TVolume::TInodeDataOps::TInodeDataOps(TBlockGroup& group)
        : Group_(group)
    {
    }

    TVolume::TInode TVolume::TInodeDataOps::AddChild(TInode& parent, const std::string& name) {
        // TODO FIXME Don't use Zero Id for Inodes and for Data Blocks -- start from One

        if (parent.Dir.HasChildren) {
            Y_ENSURE(parent.Dir.BlockCount != 0);
            auto block = Group_.GetMutableDataBlock(parent.Dir.FirstBlockId);
            auto children = DeserializeDirectoryEntries(block.Buf());

            if (std::any_of(children.begin(), children.end(), [&](const TDirEntry& c) { return c.Name == name; })) {
                throw std::runtime_error("Already has child");
            }

            auto child = Group_.AllocateInode();
            children.push_back({child.Id, name});

            SerializeDirectoryEntries(block.Buf(), children);
            return child;
        } else {
            auto child = Group_.AllocateInode();
            auto blockId = Group_.AllocateDataBlock();
            auto block = Group_.GetMutableDataBlock(blockId);
            SerializeDirectoryEntries(block.Buf(), {{child.Id, name}});

            parent.Dir.HasChildren = true;
            parent.Dir.BlockCount = 1;
            parent.Dir.FirstBlockId = blockId;
            Group_.WriteInode(parent);
            return child;
        }
    }

    void TVolume::TInodeDataOps::RemoveChild(TInode& parent, const std::string& name) {
        Y_ENSURE(parent.Dir.HasChildren);
        Y_ENSURE(parent.Dir.BlockCount != 0);

        auto block = Group_.GetMutableDataBlock(parent.Dir.FirstBlockId);
        auto children = DeserializeDirectoryEntries(block.Buf());

        // TODO Optimize
        auto it = std::find_if(children.begin(), children.end(), [&](const TDirEntry& c) { return c.Name == name; });
        if (it == children.end()) {
            throw std::runtime_error("Has no such child");
        }
        auto childDentry = *it;
        auto child = Group_.ReadInode(childDentry.Id);

        // XXX
        Y_ENSURE(!child.Dir.HasChildren);

        if (child.Val.Type != TInode::EType::Undefined) {
            Y_FAIL("TODO Remove value data blocks");
        }

        children.erase(it); // TODO Optimize
        Group_.DeallocateInode(child); // FIXME

        if (children.empty()) {
            Group_.DeallocateDataBlock(parent.Dir.FirstBlockId);
            parent.Dir.HasChildren = false;
            parent.Dir.BlockCount = 0;
            parent.Dir.FirstBlockId = 0;
            Group_.WriteInode(parent);
        } else {
            SerializeDirectoryEntries(block.Buf(), children);
        }
    }

    std::vector<TVolume::TInodeDataOps::TDirEntry> TVolume::TInodeDataOps::ListChildren(const TInode& parent) {
        if (!parent.Dir.HasChildren) {
            return {};
        }

        Y_ENSURE(parent.Dir.BlockCount != 0);

        auto block = Group_.GetDataBlock(parent.Dir.FirstBlockId);
        return DeserializeDirectoryEntries(block.Buf());
    }

    std::optional<TVolume::TInode> TVolume::TInodeDataOps::LookupChild(const TInode& parent, const std::string& name) {
        if (!parent.Dir.HasChildren) {
            return {};
        }
        Y_ENSURE(parent.Dir.BlockCount != 0);

        auto block = Group_.GetDataBlock(parent.Dir.FirstBlockId);
        auto children = DeserializeDirectoryEntries(block.Buf()); // TODO We can lookup without full deserialization

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

        const auto blockId = inode.Val.BlockCount
            ?  inode.Val.FirstBlockId
            :  Group_.AllocateDataBlock();

        auto block = Group_.GetMutableDataBlock(blockId);

        const auto type = static_cast<TInode::EType>(value.index());
        inode.Val.Type = type;

        if (!inode.Val.BlockCount) {
            inode.Val.BlockCount = 1;
            inode.Val.FirstBlockId = blockId;
        }
        Group_.WriteInode(inode); // TODO Until we have cache

        TBufOutput out(block.Buf());
        if (const auto* v = std::get_if<ui32>(&value)) {
            Serialize(out, *v);
        } else if (const auto* v = std::get_if<float>(&value)) {
            Serialize(out, *v);
        } else if (const auto* v = std::get_if<double>(&value)) {
            Serialize(out, *v);
        } else if (const auto* v = std::get_if<bool>(&value)) {
            Serialize(out, *v);
        } else if (const auto* v = std::get_if<std::string>(&value)) {
            ui16 len = v->size();
            Serialize(out, len);
            out.Save(v->data(), len);
        } else {
            Y_FAIL("TODO TInodeDataOps::SetValue");
        }
    }

    TVolume::TInodeDataOps::TValue TVolume::TInodeDataOps::GetValue(const TInode& inode) {
        if (inode.Val.Type == TInode::EType::Undefined) {
            Y_ENSURE(!inode.Val.BlockCount);
            return {};
        }
        Y_ENSURE(inode.Val.BlockCount);

        auto block = Group_.GetDataBlock(inode.Val.FirstBlockId);

        using EType = TInode::EType;

        TValue ret;
        TBufInput in(block.Buf());
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
        case EType::Bool: {
            bool val = false;
            Deserialize(in, val);
            ret = val;
        }
            break;
        case EType::Double: {
            double val = 0;
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
            Y_FAIL("TODO TInodeDataOps::GetValue");
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

        Group_.DeallocateDataBlock(inode.Val.FirstBlockId);

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

    void TVolume::TInodeDataOps::DumpTree(std::ostream& out, bool dumpInodeId) {
        const auto& root = Group_.ReadInode(0); // TODO
        DoDumpTree(out, root, 0, dumpInodeId);
    }

    void TVolume::TInodeDataOps::DoDumpTree(std::ostream& out, const TInode& root, size_t offset, bool dumpInodeId) {
        auto children = ListChildren(root);
        std::sort(children.begin(), children.end(), [] (const auto& lhs, const auto& rhs) {
            return lhs.Name < rhs.Name;
        });
        for (auto&& childEntry : children) {
            for (size_t i = 0; i < offset; ++i) {
                out.put(' ');
                out.put(' ');
                out.put(' ');
                out.put(' ');
            }
            const auto& child = Group_.ReadInode(childEntry.Id);
            const auto& value = GetValue(child);
            out << childEntry.Name;
            if (dumpInodeId) {
                out << ' ' << child.Id;
            }
            if (value.index()) {
                out << " = ";
                if (auto* p = std::get_if<bool>(&value)) {
                    out << "bool " << (*p ? "true" : "false");
                } else if (auto* p = std::get_if<i32>(&value)) {
                    out << "i32 " << *p;
                } else if (auto* p = std::get_if<ui32>(&value)) {
                    out << "ui32 " << *p;
                } else if (auto* p = std::get_if<int64_t>(&value)) {
                    out << "i64 " << *p;
                } else if (auto* p = std::get_if<uint64_t>(&value)) {
                    out << "ui64 " << *p;
                } else if (auto* p = std::get_if<float>(&value)) {
                    out << "float " << *p;
                } else if (auto* p = std::get_if<double>(&value)) {
                    out << "double " << *p;
                } else if (auto* p = std::get_if<std::string>(&value)) {
                    out << "string \"" << *p << "\"";
                } else if (auto* p = std::get_if<TBlobView>(&value)) {
                    out << "blob \"" << std::string(p->Ptr, p->Size) << "\"";
                }
            }
            out << '\n';
            DoDumpTree(out, child, offset + 1, dumpInodeId);
        }
    }
}