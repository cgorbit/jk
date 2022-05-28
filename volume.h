#pragma once

#include "common.h"
#include "bitset.h"
#include "direct_io.h"
#include "stream.h"
#include "saveload.h"

#include <vector>
#include <iostream>
#include <fstream>
#include <filesystem>

namespace NJK {

    // Limits: up to several terrabytes (10 for example)
    //   => 5000 files (TMetaGroups)
    //   FIXME Need files hierarchy?
    //
    // Limits: up to several millions keys (10 million keys for example)
    //   => 10M inodes => 20 files only (1 file -- ~500k inodes)

    inline void ReadBlock(TDirectIoFile& file, TFixedBuffer& buf, off_t offset) {
        Y_ENSURE(file.Read(buf.Data(), buf.Size(), offset) == buf.Size());
    }

    inline void WriteBlock(TDirectIoFile& file, const TFixedBuffer& buf, off_t offset) {
        Y_ENSURE(file.Write(buf.Data(), buf.Size(), offset) == buf.Size());
    }

    class TVolume {
    public:
        struct TSettings {
            ui32 BlockSize = 4096;
            ui32 NameMaxLen = 32; // or 64
            ui32 MaxFileSize = 2_GiB;
            //static constexpr ui32 GroupDescrSize = 64; // TODO
            //ui32 MetaGroupGroupCount = 4096 / GroupDescrSize;
        };

        struct TSuperBlock {
            ui32 BlockSize = 0;
            ui32 BlockGroupCount = 0;
            ui32 MaxBlockGroupCount = 0;
            ui32 BlockGroupSize = 0;
            ui32 BlockGroupDescriptorsBlockCount = 0;
            ui32 MetaGroupCount = 0; // File count for check
            ui32 MaxFileSize = 0;
            ui32 ZeroBlockGroupOffset = 0;
            ui32 BlockGroupInodeCount = 0;
            ui32 MetaGroupInodeCount = 0;

            void Serialize(IOutputStream& out) const;
            void Deserialize(IInputStream& in);

            TFixedBuffer NewBuffer() const {
                return TFixedBuffer(BlockSize);
            }
        };

        struct TBlockGroupDescr {
            ui32 Location = 0;

            // On Disk
            ui32 FreeInodeCount = 0;
            ui32 FreeDataBlockCount = 0;
            ui32 DirectoryCount = 0; // debug

            static constexpr ui32 OnDiskSize = 16; // TODO

            void Serialize(IOutputStream& out) const;
            void Deserialize(IInputStream& in);
        };

        // 128 MiB Data Blocks
        // Layout
        // 1 block Data Block Bitmap
        // 1 block Inode Bitmap
        // 'inode_size' blocks (inode_size * 4096 / 4096) for Inodes
        // 4096 * 8 = 32k blocks Data Block
        //
        // TODO XXX Maybe I need inode count >> than data block count
        //          because I store small objects in general
        //          and I don't need separate data block for most of them.
        //          (in defaults ratio 64/32k = 0.1-0.2%)
        // TODO Choose Block Size other than 4 KiB
        struct TBlockGroup {
            // in-memory only
            ui32 InFileLocation = 0;
            ui32 InodeIndexOffset = 0;

            // in-memory only
            ui32 FreeInodeCount = 0;
            ui32 FreeDataBlockCount = 0;
            ui32 DirectoryCount = 0; // debug

            // on disk
            TBlockBitSet InodesBitmap; // 4096 bytes
            TBlockBitSet DataBlocksBitmap; // 4096 bytes

            // FIXME Inodes Cache?

            // Ref to file?
        };

        // One data file up to 2 GiB by default
        struct TMetaGroup {
            TMetaGroup(const std::string& file, size_t idx, const TSuperBlock& sb)
                : SuperBlock(&sb)
                , Index(idx)
                , FileName(file)
                , File(FileName)
            {
                if (File.GetSize() == 0) {
                    File.Truncate(SuperBlock->ZeroBlockGroupOffset);
                }
                // TODO Not only one block
                auto buf = ReadBlock(0);
                BlockGroupDescrs.resize(SuperBlock->MaxBlockGroupCount);
                TBufInput in(buf);
                DeserializeFixedVector(in, BlockGroupDescrs);
            }

            void Flush() {
                auto buf = SuperBlock->NewBuffer();
                TBufOutput out(buf);
                SerializeFixedVector(out, BlockGroupDescrs);
                WriteBlock(buf, 0);
            }

            TFixedBuffer ReadBlock(size_t idx) {
                auto buf = SuperBlock->NewBuffer();
                size_t offset = SuperBlock->BlockSize * idx;
                ::NJK::ReadBlock(File, buf, offset);
                return buf;
            }

            void WriteBlock(const TFixedBuffer& buf, size_t idx) {
                size_t offset = SuperBlock->BlockSize * idx;
                ::NJK::WriteBlock(File, buf, offset);
            }

            //TFixedBuffer ReadInodeBitMapBlock() {
            //    return ReadBlock(0);
            //}

            //TFixedBuffer ReadDataBitMapBlock() {
            //    return ReadBlock(1);
            //}

            // in-memory only
            const TSuperBlock* SuperBlock{};
            ui32 Index = 0;
            std::string FileName;
            TDirectIoFile File;

            // in-memory only
            ui32 BlockGroupCount = 0;
            ui32 FreeInodeCount = 0;
            ui32 FreeDataBlockCount = 0;
            ui32 DirectoryCount = 0; // debug

            // on disk
            std::vector<TBlockGroupDescr> BlockGroupDescrs;
            std::vector<TBlockGroup> BlockGroups;
        };

        TVolume(const std::string& dir, const TSettings& settings)
            : Directory_(dir)
        {
            InitSuperBlock(settings);
            InitMetaGroups();
        }

        void InitSuperBlock(const TSettings& settings) {
            if (std::filesystem::exists(Directory_)) {
                TFixedBuffer buf(settings.BlockSize); // FIXME
                TDirectIoFile f(MakeSuperBlockFilePath());
                ReadBlock(f, buf, 0);
                TBufInput in(buf);
                SuperBlock_.Deserialize(in);
            } else {
                SuperBlock_ = CalcSuperBlock(settings);
                std::filesystem::create_directories(Directory_);
                TFixedBuffer buf(SuperBlock_.BlockSize);
                TBufOutput out(buf);
                SuperBlock_.Serialize(out);
                TDirectIoFile f(MakeSuperBlockFilePath());
                WriteBlock(f, buf, 0);
            }
        }

        std::unique_ptr<TMetaGroup> LoadMetaGroup(const std::string& path, size_t idx) {
            return {};
        }

        void InitMetaGroups() {
            for (size_t i = 0; i < 10'000; ++i) { // TODO
                const auto path = MakeMetaGroupFilePath(i);
                if (!std::filesystem::exists(path)) {
                    break;
                }
                MetaGroups_.push_back(LoadMetaGroup(path, i));
            }
        }

        std::string MakeSuperBlockFilePath() const {
            return Directory_ + "/super_block";
        }

        std::string MakeMetaGroupFilePath(ui32 idx) const {
            return Directory_ + "/meta_group_" + std::to_string(idx);
        }

        const TSuperBlock& GetSuperBlock() const {
            return SuperBlock_;
        }

        static TSuperBlock CalcSuperBlock(const TSettings& settings) {
            TSuperBlock sb;
            sb.BlockSize = settings.BlockSize;
            sb.MaxFileSize = settings.MaxFileSize;
            sb.BlockGroupSize = settings.BlockSize * (
                1 // 1 block for Data Block Bitmap
                + 1 // 1 block for Inode Bitmap
                + TInode::OnDiskSize * 8 // blocks for Inode Table
                + settings.BlockSize * 8 // blocks for Data Blocks (FIXME Less)
            );
            sb.MaxBlockGroupCount = sb.MaxFileSize / (TBlockGroupDescr::OnDiskSize + sb.BlockGroupSize);
            sb.BlockGroupDescriptorsBlockCount = (sb.MaxBlockGroupCount * TBlockGroupDescr::OnDiskSize - 1) / settings.BlockSize + 1;
            sb.ZeroBlockGroupOffset = sb.BlockGroupDescriptorsBlockCount * sb.BlockSize;
            sb.BlockGroupInodeCount = sb.BlockSize * 8;
            sb.MetaGroupInodeCount = sb.BlockGroupInodeCount * sb.MaxBlockGroupCount;
            return sb;
        }

        // Super Block (1 block)
        // Block Group Descriptors (1-several blocks) (16 descriptors if 2 GiB file size and 128 MiB block group)
        // Block Groups [
        //    Data Block Bitmap (1 block)
        //    Inode Bitmap (1 block)
        //    Inode Table (inode_size blocks)
        //    Data Blocks (32K blocks at maximum (128 MiB)) 
        // ]

        struct TInode {
            enum class EType {
            };
            //bool Used = false; See in bitmap
            EType Type;
            ui32 BlockCount = 0; // bool Inline = true;
            ui32 CreationTime{};
            ui32 ModTime{};

            static constexpr ui32 OnDiskSize = 64;
        };

    private:
        std::string Directory_;
        TSuperBlock SuperBlock_;
        std::vector<std::unique_ptr<TMetaGroup>> MetaGroups_;
    };

}
