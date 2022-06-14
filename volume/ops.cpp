#include "ops.h"

#include "../saveload.h"

#include <variant>

namespace NJK::NVolume {
    /*
        TInodeDataOps
    */

    TInodeDataOps::TInodeDataOps(TVolume* volume)
        : Volume_(*volume)
    {
    }

    TVolume::TInode TInodeDataOps::AddChild(TInode& parent, const std::string& name) {
        // TODO FIXME Don't use Zero Id for Inodes and for Data Blocks -- start from One

        if (parent.Dir.HasChildren) {
            Y_VERIFY(parent.Dir.BlockCount != 0);
            auto block = Volume_.GetMutableDataBlock(parent.Dir.FirstBlockId);
            auto children = DeserializeDirectoryEntries(block.Buf());

            if (std::any_of(children.begin(), children.end(), [&](const TDirEntry& c) { return c.Name == name; })) {
                throw std::runtime_error("Already has child");
            }

            auto child = Volume_.AllocateInode();
            children.push_back({child.Id, name});

            SerializeDirectoryEntries(block.Buf(), children);
            return child;
        } else {
            auto child = Volume_.AllocateInode();
TODO("Allocate with owner inode argument")
            auto blockId = Volume_.AllocateDataBlock();
            auto block = Volume_.GetMutableDataBlock(blockId);
            SerializeDirectoryEntries(block.Buf(), {{child.Id, name}});

            parent.Dir.HasChildren = true;
            parent.Dir.BlockCount = 1;
            parent.Dir.FirstBlockId = blockId;
            Volume_.WriteInode(parent);
            return child;
        }
    }

    void TInodeDataOps::RemoveChild(TInode& parent, const std::string& name) {
        Y_VERIFY(parent.Dir.HasChildren);
        Y_VERIFY(parent.Dir.BlockCount != 0);

        auto block = Volume_.GetMutableDataBlock(parent.Dir.FirstBlockId);
        auto children = DeserializeDirectoryEntries(block.Buf());

        // TODO Optimize
        auto it = std::find_if(children.begin(), children.end(), [&](const TDirEntry& c) { return c.Name == name; });
        if (it == children.end()) {
            throw std::runtime_error("Has no such child");
        }
        auto childDentry = *it;
        auto child = Volume_.ReadInode(childDentry.Id);

        // XXX
        Y_VERIFY(!child.Dir.HasChildren);

        if (child.Val.Type != TInode::EType::Undefined) {
            Y_FAIL("TODO Remove value data blocks");
        }

        children.erase(it); // TODO Optimize
        Volume_.DeallocateInode(child); // FIXME

        if (children.empty()) {
            Volume_.DeallocateDataBlock(parent.Dir.FirstBlockId);
            parent.Dir.HasChildren = false;
            parent.Dir.BlockCount = 0;
            parent.Dir.FirstBlockId = 0;
            Volume_.WriteInode(parent);
        } else {
            SerializeDirectoryEntries(block.Buf(), children);
        }
    }

    std::vector<TInodeDataOps::TDirEntry> TInodeDataOps::ListChildren(const TInode& parent) {
        if (!parent.Dir.HasChildren) {
            return {};
        }

        Y_VERIFY(parent.Dir.BlockCount != 0);

        auto block = Volume_.GetDataBlock(parent.Dir.FirstBlockId);
        return DeserializeDirectoryEntries(block.Buf());
    }

    std::optional<TVolume::TInode> TInodeDataOps::LookupChild(const TInode& parent, const std::string& name) {
        if (!parent.Dir.HasChildren) {
            return {};
        }
        Y_VERIFY(parent.Dir.BlockCount != 0);

        auto block = Volume_.GetDataBlock(parent.Dir.FirstBlockId);
        auto children = DeserializeDirectoryEntries(block.Buf()); // TODO We can lookup without full deserialization

        // TODO Optimize
        auto it = std::find_if(children.begin(), children.end(), [&](const TDirEntry& c) { return c.Name == name; });
        if (it == children.end()) {
            return {};
        }

        auto child = *it;
        return Volume_.ReadInode(child.Id);
    }

    // TODO OPTIMIZE, Do it in less movements
    TVolume::TInode TInodeDataOps::EnsureChild(TInode& parent, const std::string& name) {
        auto child = LookupChild(parent, name);
        if (child) {
            return *child;
        }
        return AddChild(parent, name);
    }

    void TInodeDataOps::SetValue(TInode& inode, const TValue& value, const ui32 deadline) {
        if (const auto* v = std::get_if<std::monostate>(&value)) {
            UnsetValue(inode);
            return;
        }

        TODO("Allocate with owner inode argument")

        const auto blockId = inode.Val.BlockCount
            ?  inode.Val.FirstBlockId
            :  Volume_.AllocateDataBlock();

        auto block = Volume_.GetMutableDataBlock(blockId);

        const auto type = static_cast<TInode::EType>(value.index());
        inode.Val.Type = type;

        if (!inode.Val.BlockCount) {
            inode.Val.BlockCount = 1;
            inode.Val.FirstBlockId = blockId;
        }
        Volume_.WriteInode(inode);

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
            const ui16 len = v->size();
            Y_ENSURE(len <= Volume_.GetSuperBlock().BlockSize - sizeof(len));
            Serialize(out, len);
            out.Save(v->data(), len);
        } else if (const auto* v = std::get_if<TBlobView>(&value)) {
            const ui16 len = v->Size();
            Y_ENSURE(len <= Volume_.GetSuperBlock().BlockSize - sizeof(len));
            Serialize(out, len);
            out.Save(v->Data(), len);
        } else {
            Y_FAIL("TODO TInodeDataOps::SetValue");
        }
    }

    TInodeDataOps::TValue TInodeDataOps::GetValue(const TInode& inode) {
        if (inode.Val.Type == TInode::EType::Undefined) {
            Y_VERIFY(!inode.Val.BlockCount);
            return {};
        }
        Y_VERIFY(inode.Val.BlockCount);

        auto block = Volume_.GetDataBlock(inode.Val.FirstBlockId);

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

    void TInodeDataOps::UnsetValue(TInode& inode) {
        if (inode.Val.Type == TInode::EType::Undefined) {
            Y_VERIFY(!inode.Val.BlockCount);
            Y_VERIFY(inode.Val.FirstBlockId == 0);
            return;
        }
        Y_VERIFY(inode.Val.BlockCount);

        Volume_.DeallocateDataBlock(inode.Val.FirstBlockId);

        inode.Val.Type = TInode::EType::Undefined;
        inode.Val.BlockCount = 0;
        inode.Val.FirstBlockId = 0;

        Volume_.WriteInode(inode);
    }

    std::vector<TInodeDataOps::TDirEntry> TInodeDataOps::DeserializeDirectoryEntries(const TFixedBuffer& buf) {
        TBufInput in(buf);
        ui16 count = 0;
        Deserialize(in, count);
        Y_VERIFY(count > 0);
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

    void TInodeDataOps::SerializeDirectoryEntries(TFixedBuffer& buf, const std::vector<TDirEntry>& entries) {
        TBufOutput out(buf);
        ui16 count = entries.size();
        Y_VERIFY(count > 0);
        Serialize(out, count);
        std::vector<TDirEntry> ret;
        for (const auto& entry : entries) {
            Serialize(out, entry.Id);
            ui8 len = entry.Name.size();
            Serialize(out, len);
            out.Save(entry.Name.data(), len);
        }
    }

    void TInodeDataOps::DumpTree(std::ostream& out, bool dumpInodeId) {
        const auto& root = Volume_.ReadInode(0); // TODO
        DoDumpTree(out, root, 0, dumpInodeId);
    }

    std::string TInodeDataOps::DumpTree(bool dumpInodeId) {
        std::stringstream out;
        DumpTree(out, dumpInodeId);
        return out.str();
    }

    void TInodeDataOps::DoDumpTree(std::ostream& out, const TInode& root, size_t offset, bool dumpInodeId) {
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
            const auto& child = Volume_.ReadInode(childEntry.Id);
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
                    out << "blob \"" << std::string(p->Data(), p->Size()) << "\"";
                }
            }
            out << '\n';
            DoDumpTree(out, child, offset + 1, dumpInodeId);
        }
    }

}