#pragma once

#include "block_group.h"

#include "../datetime.h"
#include <vector>
#include <memory>

namespace NJK::NVolume {

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
            //auto expectedSize = CalcExpectedFileSize();
            //std::cerr << "+ file size: " << FileName << ": " << RawFile.GetSizeInBytes() << '\n';
            //std::cerr << "+ expected size: " << expectedSize << '\n';
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

}