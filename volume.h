#pragma once

#include "common.h"
#include "volume_fwd.h"
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
#include <variant>
#include <optional>

#include <mutex>

namespace NJK {

    // Limits: up to several terrabytes (10 for example)
    //   => 5000 files (TMetaGroups)
    //   FIXME Need files hierarchy?
    //
    // Limits: up to several millions keys (10 million keys for example)
    //   => 10M inodes => 20 files only (1 file -- ~500k inodes)

    //inline void XReadBlock(TDirectIoFile& file, TFixedBuffer& buf, off_t offset) {
    //    Y_ENSURE(file.Read(buf.MutableData(), buf.Size(), offset) == buf.Size());
    //    buf.ResetDirtiness();
    //}

    //inline void XWriteBlock(TDirectIoFile& file, const TFixedBuffer& buf, off_t offset) {
    //    Y_ENSURE(file.Write(buf.Data(), buf.Size(), offset) == buf.Size());
    //}

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

            Y_DECLARE_SERIALIZATION

            TFixedBuffer NewBuffer() const {
                return TFixedBuffer::Aligned(BlockSize);
            }

            static constexpr ui32 OnDiskSize = 44;
        };

        struct TInode {
            // in-memory
            ui32 Id = 0;

            enum class EType: ui8 {
                Undefined,
                Bool,
                i32,
                Ui32,
                i64,
                Ui64,
                Float,
                Double,
                String,
                Blob,
            };

            // on-disk
            ui32 CreationTime{};
            ui32 ModTime{};
            struct {
                EType Type{};
                ui16 BlockCount = 0; // up to 256 MiB
                ui32 FirstBlockId = 0;
                ui32 Deadline = 0;
                // ui16 Version = 0; // TODO Generation
            } Val;
            struct {
                bool HasChildren = false;
                ui16 BlockCount = 0; // FIXME ui8 is enough
                ui32 FirstBlockId = 0;
            } Dir;
            char Data[38] = {0};

            static constexpr ui32 ToSkip = 0;

            Y_DECLARE_SERIALIZATION

            static constexpr ui32 OnDiskSize = 64;
            static_assert(512 % OnDiskSize == 0);
        };

        struct TDataBlock {
            ui32 Id = 0;
            TFixedBuffer Buf;
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
            TBlockGroup(size_t inFileOffset, ui32 inodeOffset, TCachedBlockFile& file, const TSuperBlock& sb);
            ~TBlockGroup();

            TInode AllocateInode();
            void DeallocateInode(const TInode& inode);

            TInode ReadInode(ui32 id);
            void WriteInode(const TInode& inode);

            ui32 AllocateDataBlock();
            void DeallocateDataBlock(ui32);

            TCachedBlockFile::TPage<false> GetDataBlock(ui32 id);
            TCachedBlockFile::TPage<true> GetMutableDataBlock(ui32 id);
            //void WriteDataBlock(const TDataBlock&);

        private:
            TFixedBuffer NewBuffer() {
                return SuperBlock->NewBuffer();
            }

            ui32 CalcInodeInBlockOffset(ui32 inodeId) const {
                return (inodeId - InodeIndexOffset) * TInode::OnDiskSize % SuperBlock->BlockSize;
            }
            ui32 CalcInodeBlockIndex(ui32 inodeId) const {
                return 2 + (inodeId - InodeIndexOffset) * TInode::OnDiskSize / SuperBlock->BlockSize;
            }

            ui32 CalcDataBlockIndex(ui32 blockId) const {
                const ui32 inodeBlocks = SuperBlock->BlockGroupInodeCount * TInode::OnDiskSize / SuperBlock->BlockSize;
                const ui32 ret = 2 + inodeBlocks + (blockId - DataBlockIndexOffset);
                //std::cerr << "+++ CalcDataBlockIndex for " << blockId << ", indeBlocks = " << inodeBlocks
                    //<< ", result = " << ret << "\n";
                return ret;
            }

            void Flush() {
                InodesBitmap.Buf().CopyTo(File_.GetMutableBlock(InodesBitmapBlockIndex).Buf());
                DataBlocksBitmap.Buf().CopyTo(File_.GetMutableBlock(DataBlocksBitmapBlockIndex).Buf());
            }

            static constexpr size_t InodesBitmapBlockIndex = 0;
            static constexpr size_t DataBlocksBitmapBlockIndex = 1;

            const TSuperBlock* SuperBlock{};
            TCachedBlockFileRegion File_;

            // in-memory only
            //ui32 InFileOffset = 0;
            const ui32 InodeIndexOffset = 0;
            const ui32 DataBlockIndexOffset = 0;

            // in-memory only
            //ui32 FreeInodeCount = 0;
            //ui32 FreeDataBlockCount = 0;
            //ui32 DirectoryCount = 0; // debug

            // on disk
            TBlockBitSet InodesBitmap; // 4096 bytes
            TBlockBitSet DataBlocksBitmap; // 4096 bytes

            // FIXME Inodes Cache?

        };

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

            TInodeDataOps(TBlockGroup& group);

            TInode AddChild(TInode& parent, const std::string& name);
            void RemoveChild(TInode& parent, const std::string& name);
            std::optional<TInode> LookupChild(TInode& parent, const std::string& name);
            TInode EnsureChild(TInode& parent, const std::string& name);
            std::vector<TDirEntry> ListChildren(TInode& parent);

            void SetValue(TInode& inode, const TValue& value, const ui32 deadline = 0);
            TValue GetValue(const TInode& inode);
            void UnsetValue(TInode& inode);

        private:
            // TODO Write Deserialization of:
            // 1. std::string
            // 2. TDirEntry
            std::vector<TDirEntry> DeserializeDirectoryEntries(const TFixedBuffer& buf);
            void SerializeDirectoryEntries(TFixedBuffer& buf, const std::vector<TDirEntry>& entries);

        private:
            // TODO We must operate on whole TVolume here,
            //      because inodes lives on specific Block Groups
            //      but data may lay anywhere
            TBlockGroup& Group_;
            //TVolume& Volume_;
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
            static_assert(512 % OnDiskSize == 0);

            Y_DECLARE_SERIALIZATION
        };

        // One data file up to 2 GiB by default
        class TMetaGroup {
        public:
            TMetaGroup(const std::string& file, size_t idx, const TSuperBlock& sb)
                : SuperBlock(&sb)
                , Index(idx)
                , FileName(file)
                , RawFile(FileName, SuperBlock->BlockSize)
                , File(RawFile)
            {
                BlockGroupDescrs.resize(SuperBlock->MaxBlockGroupCount); // FIXME Better
                BlockGroups.resize(SuperBlock->MaxBlockGroupCount); // FIXME Better

                if (RawFile.GetSizeInBlocks() == 0) {
                    RawFile.TruncateInBlocks(CalcExpectedFileSize() / SuperBlock->BlockSize); // TODO Better
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
        public:
            void AllocateNewBlockGroup() {
                Y_ENSURE(AliveBlockGroupCount < SuperBlock->MaxBlockGroupCount);

                const auto blockGroupIdx = AliveBlockGroupCount;

                auto& bg = BlockGroupDescrs[blockGroupIdx];
                bg.D.CreationTime = NowSeconds();
                bg.D.FreeInodeCount = SuperBlock->BlockGroupInodeCount;
                bg.D.FreeDataBlockCount = SuperBlock->BlockGroupDataBlockCount;

                // TODO Better
                FreeInodeCount += bg.D.FreeInodeCount;
                FreeDataBlockCount += bg.D.FreeDataBlockCount;

                //const size_t offset = CalcExpectedFileSize();

                ++AliveBlockGroupCount;
                RawFile.TruncateInBlocks(CalcExpectedFileSize() / SuperBlock->BlockSize); // TODO Better

                // FIXME Better offset
                BlockGroups[blockGroupIdx] = CreateBlockGroup(blockGroupIdx);
            }

            std::unique_ptr<TBlockGroup> CreateBlockGroup(ui32 blockGroupIdx) {
                return std::make_unique<TBlockGroup>(
                    CalcBlockGroupOffset(blockGroupIdx) / SuperBlock->BlockSize,
                    blockGroupIdx * SuperBlock->BlockGroupInodeCount, // TODO Add MetaGroup Inode offset
                    File,
                    *SuperBlock
                );
            }

            void LoadBlockGroupDescriptors() {
                // TODO Not only one block (for general case)
                auto block = File.GetBlock(0);
                TBufInput in(block.Buf());
                DeserializeFixedVector(in, BlockGroupDescrs);
                for (const auto& bg : BlockGroupDescrs) {
                    if (bg.D.CreationTime) {
                        // TODO Better, lazy, this code ugly
                        BlockGroups[AliveBlockGroupCount] = CreateBlockGroup(AliveBlockGroupCount);

                        ++AliveBlockGroupCount;
                        FreeInodeCount += bg.D.FreeInodeCount;
                        FreeDataBlockCount += bg.D.FreeDataBlockCount;
                    }
                    // TODO Check for holes
                }
            }

            ui32 CalcBlockGroupOffset(ui32 blockGroupIdx) const {
                return SuperBlock->ZeroBlockGroupOffset + blockGroupIdx * SuperBlock->BlockGroupSize;
            }

            ui32 CalcExpectedFileSize() const {
                return CalcBlockGroupOffset(AliveBlockGroupCount);
            }

            void VerifyFile() {
                auto expectedSize = CalcExpectedFileSize();
                std::cerr << "+ file size: " << FileName << ": " << RawFile.GetSizeInBytes() << '\n';
                std::cerr << "+ expected size: " << expectedSize << '\n';
                Y_ENSURE(RawFile.GetSizeInBytes() == SuperBlock->ZeroBlockGroupOffset + AliveBlockGroupCount * SuperBlock->BlockGroupSize);
            }

            void SaveBlockGroupDescriptors() {
                // TODO Not only one block (for general case)
                auto block = File.GetMutableBlock(0);
                TBufOutput out(block.Buf());
                SerializeFixedVector(out, BlockGroupDescrs);
            }

            void Flush() {
                SaveBlockGroupDescriptors();
            }

            // FIXME Separate wrapper for index/offset/blocked operations
            //TFixedBuffer XReadBlock(size_t idx) {
            //    auto buf = SuperBlock->NewBuffer();
            //    size_t offset = SuperBlock->BlockSize * idx;
            //    ::NJK::ReadBlock(RawFile, buf, offset);
            //    return buf;
            //}

            //void XWriteBlock(const TFixedBuffer& buf, size_t idx) {
            //    size_t offset = SuperBlock->BlockSize * idx;
            //    ::NJK::WriteBlock(RawFile, buf, offset);
            //}

            //TFixedBuffer ReadInodeBitMapBlock() {
            //    return ReadBlock(0);
            //}

            //TFixedBuffer ReadDataBitMapBlock() {
            //    return ReadBlock(1);
            //}

        private:
        public:
            // in-memory only
            const TSuperBlock* SuperBlock{};
            ui32 Index = 0;
            std::string FileName;
            TBlockDirectIoFile RawFile;
            TCachedBlockFile File;

            // in-memory only
            ui32 AliveBlockGroupCount = 0;
            ui32 FreeInodeCount = 0;
            ui32 FreeDataBlockCount = 0;
            //ui32 DirectoryCount = 0; // debug

            // on disk
            std::vector<TBlockGroupDescr> BlockGroupDescrs;
            std::vector<std::unique_ptr<TBlockGroup>> BlockGroups;
        };

        TVolume(const std::string& dir, const TSettings& settings, bool ensureRoot = false)
            : Directory_(dir)
        {
            InitSuperBlock(settings);
            LoadMetaGroups();
            if (MetaGroups_.empty()) {
                MetaGroups_.push_back(CreateMetaGroup(0));
                if (ensureRoot) {
                    auto& meta = *MetaGroups_[0];
                    meta.AllocateNewBlockGroup();
                    auto& bg = *meta.BlockGroups[0];
                    Y_ENSURE(bg.AllocateInode().Id == 0);
                }
            }
        }

        TInode GetRoot() {
            return MetaGroups_[0]->BlockGroups[0]->ReadInode(0);
        }

        void InitSuperBlock(const TSettings& settings) {
            const auto& sbPath = MakeSuperBlockFilePath();
            if (std::filesystem::exists(sbPath)) {
                auto buf = TFixedBuffer::Aligned(settings.BlockSize); // FIXME
                TBlockDirectIoFile f(sbPath, settings.BlockSize);
                f.ReadBlock(buf, 0);
                TBufInput in(buf);
                SuperBlock_.Deserialize(in);
            } else {
                SuperBlock_ = CalcSuperBlock(settings);
                std::filesystem::create_directories(Directory_);
                auto buf = SuperBlock_.NewBuffer();
                TBufOutput out(buf);
                SuperBlock_.Serialize(out);
                TBlockDirectIoFile f(sbPath, SuperBlock_.BlockSize);
                f.WriteBlock(buf, 0);
            }
        }

        std::unique_ptr<TMetaGroup> CreateMetaGroup(size_t idx) {
            return std::make_unique<TMetaGroup>(MakeMetaGroupFilePath(idx), idx, SuperBlock_);
        }

        void LoadMetaGroups() {
            for (size_t i = 0; i < 10'000; ++i) { // TODO
                const auto path = MakeMetaGroupFilePath(i);
                if (!std::filesystem::exists(path)) {
                    break;
                }
                MetaGroups_.push_back(std::make_unique<TMetaGroup>(path, i, SuperBlock_));
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

    private:
    public:
        std::string Directory_;
        TSuperBlock SuperBlock_;
        std::vector<std::unique_ptr<TMetaGroup>> MetaGroups_;
    };

}
