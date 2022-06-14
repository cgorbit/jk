#pragma once

#include "volume.h"
#include "value.h"

#include <vector>
#include <sstream>

namespace NJK::NVolume {

    TODO("Return Inodes in smart pointers")

    class TInodeDataOps {
    public:
        using TValue = TInodeValue;

        struct TDirEntry {
            ui32 Id{};
            std::string Name;

            bool operator== (const TDirEntry& other) const {
                return Id == other.Id && Name == other.Name;
            }
        };

        TInodeDataOps(TVolume* group);

        TInode AddChild(TInode& parent, const std::string& name);
        void RemoveChild(TInode& parent, const std::string& name);
        std::optional<TInode> LookupChild(const TInode& parent, const std::string& name);
        TInode EnsureChild(TInode& parent, const std::string& name);
        std::vector<TDirEntry> ListChildren(const TInode& parent);

        void SetValue(TInode& inode, const TValue& value, const ui32 deadline = 0);
        TValue GetValue(const TInode& inode);
        void UnsetValue(TInode& inode);

        std::string DumpTree(bool dumpInodeId = false);
        void DumpTree(std::ostream& out, bool dumpInodeId = false);
        void DoDumpTree(std::ostream& out, const TInode& root, size_t offset, bool dumpInodeId);

    private:
        // TODO Write Deserialization of:
        // 1. std::string
        // 2. TDirEntry
        std::vector<TDirEntry> DeserializeDirectoryEntries(const TFixedBuffer& buf);
        void SerializeDirectoryEntries(TFixedBuffer& buf, const std::vector<TDirEntry>& entries);

    private:
        TVolume& Volume_;
    };

}