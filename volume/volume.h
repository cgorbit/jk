#pragma once

#include "../volume_fwd.h"
#include "../block_file.h"
#include "super_block.h"
#include "inode.h"

#include <memory>
#include <string>

namespace NJK {

    // Limits: up to several terrabytes (10 for example)
    //   => 5000 files (TMetaGroups)
    //   FIXME Need files hierarchy?
    //
    // Limits: up to several millions keys (10 million keys for example)
    //   => 10M inodes => 20 files only (1 file -- ~500k inodes)

    struct TVolumeSettings {
        ui32 BlockSize = 4096;
        ui32 NameMaxLen = 32; // or 64 TODO Not used
        ui32 MaxFileSize = 2_GiB;
    };

    class TVolume {
    public:
        using TSettings = TVolumeSettings;
        using TSuperBlock = NVolume::TSuperBlock;
        using TInode = NVolume::TInode;

        explicit TVolume(const std::string& dir, const TSettings& settings = {}, bool ensureRoot = true);
        ~TVolume();

        TInode GetRoot();

        TInode AllocateInode();
        void DeallocateInode(const TInode& inode);

        TInode ReadInode(ui32 id);
        void WriteInode(const TInode& inode);

        ui32 AllocateDataBlock();
        void DeallocateDataBlock(ui32);

        TCachedBlockFile::TPage<false> GetDataBlock(ui32 id);
        TCachedBlockFile::TPage<true> GetMutableDataBlock(ui32 id);

        const TSuperBlock& GetSuperBlock() const;
        static TSuperBlock CalcSuperBlock(const TSettings& settings);

    private:
        class TImpl;
        std::unique_ptr<TImpl> Impl_;
    };

    using TVolumePtr = std::unique_ptr<TVolume>;

}