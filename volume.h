#pragma once

#include "common.h"
#include "bitset.h"
#include "direct_io.h"
#include "block_file.h"
#include "stream.h"
#include "saveload.h"
#include "datetime.h"
#include "fixed_buffer.h"

#include <vector>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <memory>

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
            ui32 BlockGroupDataBlockCount = 0;
            ui32 MetaGroupInodeCount = 0;

            void Serialize(IOutputStream& out) const;
            void Deserialize(IInputStream& in);

            TFixedBuffer NewBuffer() const {
                return TFixedBuffer::Aligned(BlockSize);
            }

            static constexpr ui32 OnDiskSize = 44;
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
        class TBlockGroup {
        public:
            TBlockGroup(size_t inFileOffset, ui32 inodeOffset, TDirectIoFile& file, const TSuperBlock& sb)
                : SuperBlock(&sb)
                , File_(file, sb.BlockSize, inFileOffset)
                , InodeIndexOffset(inodeOffset)
                , InodesBitmap(SuperBlock->NewBuffer())
                , DataBlocksBitmap(SuperBlock->NewBuffer())
            {
                File_.ReadBlock(InodesBitmap.Buf(), InodesBitmapBlockIndex);
                File_.ReadBlock(DataBlocksBitmap.Buf(), DataBlocksBitmapBlockIndex);
            }

            ~TBlockGroup() {
                Flush();
            }

        private:
            ui32 CalcInodeBlockIndex(ui32 inodeId) const {
                return 2 + (inodeId - InodeIndexOffset) * TInode::OnDiskSize / SuperBlock->BlockSize;
            }

            ui32 CalcDataBlockIndex(ui32 inodeId) const {
                // ...
                return 0; // TODO
                //return 2 + (inodeId - InodeIndexOffset) * TInode::OnDiskSize / SuperBlock->BlockSize;
            }

            void Flush() {
                File_.WriteBlock(InodesBitmap.Buf(), InodesBitmapBlockIndex);
                File_.WriteBlock(DataBlocksBitmap.Buf(), DataBlocksBitmapBlockIndex);
            }

            static constexpr size_t InodesBitmapBlockIndex = 0;
            static constexpr size_t DataBlocksBitmapBlockIndex = 1;

            const TSuperBlock* SuperBlock{};
            TBlockDirectIoFileView File_;

            // in-memory only
            //ui32 InFileOffset = 0;
            const ui32 InodeIndexOffset = 0;

            // in-memory only
            //ui32 FreeInodeCount = 0;
            //ui32 FreeDataBlockCount = 0;
            //ui32 DirectoryCount = 0; // debug

            // on disk
            TBlockBitSet InodesBitmap; // 4096 bytes
            TBlockBitSet DataBlocksBitmap; // 4096 bytes

            // FIXME Inodes Cache?

        };

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

            void Serialize(IOutputStream& out) const;
            void Deserialize(IInputStream& in);
        };

        // One data file up to 2 GiB by default
        class TMetaGroup {
        public:
            TMetaGroup(const std::string& file, size_t idx, const TSuperBlock& sb)
                : SuperBlock(&sb)
                , Index(idx)
                , FileName(file)
                , File(FileName)
            {
                BlockGroupDescrs.resize(SuperBlock->MaxBlockGroupCount); // FIXME Better
                BlockGroups.resize(SuperBlock->MaxBlockGroupCount); // FIXME Better

                if (File.GetSize() == 0) {
                    File.Truncate(CalcExpectedFileSize());
                    SaveBlockGroupDescriptors();
                }
                LoadBlockGroupDescriptors();
                VerifyFile();
                //AllocateNewBlockGroup();
            }

            ~TMetaGroup() {
                Flush();
            }

            ui32 GetIndex() const {
                return Index;
            }

        private:
            void AllocateNewBlockGroup() {
                Y_ENSURE(AliveBlockGroupCount < SuperBlock->MaxBlockGroupCount);

                auto& bg = BlockGroupDescrs[AliveBlockGroupCount];
                bg.D.CreationTime = NowSeconds();
                bg.D.FreeInodeCount = SuperBlock->BlockGroupInodeCount;
                bg.D.FreeDataBlockCount = SuperBlock->BlockGroupDataBlockCount;

                // TODO Better
                FreeInodeCount += bg.D.FreeInodeCount;
                FreeDataBlockCount += bg.D.FreeDataBlockCount;

                // FIXME Better offset
                BlockGroups[AliveBlockGroupCount] = std::make_unique<TBlockGroup>(
                    CalcExpectedFileSize(),
                    AliveBlockGroupCount * SuperBlock->BlockGroupInodeCount, // TODO Add MetaGroup Inode offset
                    File,
                    *SuperBlock);

                ++AliveBlockGroupCount;
                File.Truncate(CalcExpectedFileSize());

            }

            void LoadBlockGroupDescriptors() {
                // TODO Not only one block (for general case)
                auto buf = ReadBlock(0);
                TBufInput in(buf);
                DeserializeFixedVector(in, BlockGroupDescrs);
                for (const auto& bg : BlockGroupDescrs) {
                    if (bg.D.CreationTime) {
                        ++AliveBlockGroupCount;
                        FreeInodeCount += bg.D.FreeInodeCount;
                        FreeDataBlockCount += bg.D.FreeDataBlockCount;
                    }
                    // TODO Check for holes
                }
            }

            ui32 CalcExpectedFileSize() const {
                return SuperBlock->ZeroBlockGroupOffset + AliveBlockGroupCount * SuperBlock->BlockGroupSize;
            }

            void VerifyFile() {
                auto expectedSize = CalcExpectedFileSize();
                std::cerr << "+ file size: " << FileName << ": " << File.GetSize() << '\n';
                std::cerr << "+ expected size: " << expectedSize << '\n';
                Y_ENSURE(File.GetSize() == SuperBlock->ZeroBlockGroupOffset + AliveBlockGroupCount * SuperBlock->BlockGroupSize);
            }

            void SaveBlockGroupDescriptors() {
                // TODO Not only one block (for general case)
                auto buf = SuperBlock->NewBuffer();
                TBufOutput out(buf);
                SerializeFixedVector(out, BlockGroupDescrs);
                WriteBlock(buf, 0);
            }

            void Flush() {
                SaveBlockGroupDescriptors();
            }

            // FIXME Separate wrapper for index/offset/blocked operations
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

        private:
            // in-memory only
            const TSuperBlock* SuperBlock{};
            ui32 Index = 0;
            std::string FileName;
            TDirectIoFile File;

            // in-memory only
            ui32 AliveBlockGroupCount = 0;
            ui32 FreeInodeCount = 0;
            ui32 FreeDataBlockCount = 0;
            //ui32 DirectoryCount = 0; // debug

            // on disk
            std::vector<TBlockGroupDescr> BlockGroupDescrs;
            std::vector<std::unique_ptr<TBlockGroup>> BlockGroups;
        };

        TVolume(const std::string& dir, const TSettings& settings)
            : Directory_(dir)
        {
            InitSuperBlock(settings);
            InitMetaGroups();
        }

        void InitSuperBlock(const TSettings& settings) {
            if (std::filesystem::exists(Directory_)) {
                auto buf = TFixedBuffer::Aligned(settings.BlockSize); // FIXME
                TDirectIoFile f(MakeSuperBlockFilePath());
                ReadBlock(f, buf, 0);
                TBufInput in(buf);
                SuperBlock_.Deserialize(in);
            } else {
                SuperBlock_ = CalcSuperBlock(settings);
                std::filesystem::create_directories(Directory_);
                auto buf = SuperBlock_.NewBuffer();
                TBufOutput out(buf);
                SuperBlock_.Serialize(out);
                TDirectIoFile f(MakeSuperBlockFilePath());
                WriteBlock(f, buf, 0);
            }
        }

        std::unique_ptr<TMetaGroup> CreateMetaGroup(size_t idx) {
            return std::make_unique<TMetaGroup>(MakeMetaGroupFilePath(idx), idx, SuperBlock_);
        }

        void InitMetaGroups() {
            for (size_t i = 0; i < 10'000; ++i) { // TODO
                const auto path = MakeMetaGroupFilePath(i);
                if (!std::filesystem::exists(path)) {
                    break;
                }
                MetaGroups_.push_back(std::make_unique<TMetaGroup>(path, i, SuperBlock_));
            }

            if (MetaGroups_.empty()) {
                MetaGroups_.push_back(CreateMetaGroup(0));
            }
        }

        std::string MakeSuperBlockFilePath() const {
            return Directory_ + "/super_block";
        }

        std::string MakeMetaGroupFilePath(ui32 idx) const {
            std::stringstream out;
            out << Directory_ << "/meta_group_"
                << std::setfill('0') << std::setw(6) << idx
                << std::setfill(' '); // FIXME better
            return out.str();
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
            sb.BlockGroupDataBlockCount = sb.BlockSize * 8;
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
            enum class EType: ui32 {
            };
            //bool Used = false; See in bitmap
            EType Type{};
            ui32 BlockCount = 0; // bool Inline = true;
            ui32 CreationTime{};
            ui32 ModTime{};

            void Serialize(IOutputStream& out) const {
                SerializeMany(out, Type, BlockCount, CreationTime, ModTime);
                out.SkipWrite(48);
            }
            void Deserialize(IInputStream& in) {
                DeserializeMany(in, Type, BlockCount, CreationTime, ModTime);
                in.SkipRead(48);
            }

            static constexpr ui32 OnDiskSize = 64;
        };

    private:
        std::string Directory_;
        TSuperBlock SuperBlock_;
        std::vector<std::unique_ptr<TMetaGroup>> MetaGroups_;
    };

}
