#include "volume.h"
#include "block_group.h"
#include "meta_group.h"
#include "../stream.h"

#include <vector>
#include <string>
#include <filesystem>

namespace NJK {

    using namespace NVolume;

    class TVolume::TImpl {
    public:
        //struct TDataBlock {
        //    ui32 Id = 0;
        //    TFixedBuffer Buf;
        //};

        TImpl(const std::string& dir, const TSettings& settings, bool ensureRoot)
            : Directory_(dir)
        {
            InitSuperBlock(settings);
            LoadMetaGroups();
            //if (MetaGroups_.empty()) {
                //Y_ENSURE(AllocateInode().Id == 0);
            //}
            if (MetaGroups_.empty()) {
                MetaGroups_.push_back(CreateMetaGroup(0));
                if (ensureRoot) {
                    auto& meta = *MetaGroups_[0];
                    meta.AllocateNewBlockGroup();
                    auto& bg = *meta.BlockGroups[0];

                    Y_TODO("");
                    Y_ENSURE(bg.TryAllocateInode()->Id == 0);
                }
            }
        }

        TInode GetRoot() {
            return ReadInode(0);
        }

        TInode AllocateInode();
        void DeallocateInode(const TInode& inode);

        ui32 AllocateDataBlock();
        void DeallocateDataBlock(ui32);

        TBlockGroup& GetInodeBlockGroupById(ui32 id) {
            auto metaIdx = id / SuperBlock_.MetaGroupInodeCount;
            auto bgIdx = (id % SuperBlock_.MetaGroupInodeCount) / SuperBlock_.BlockGroupInodeCount;
            return *MetaGroups_[metaIdx]->BlockGroups[bgIdx];
        }

        TBlockGroup& GetDataBlockGroupById(ui32 id) {
            auto metaIdx = id / SuperBlock_.MetaGroupDataBlockCount;
            auto bgIdx = (id % SuperBlock_.MetaGroupDataBlockCount) / SuperBlock_.BlockGroupDataBlockCount;
            return *MetaGroups_[metaIdx]->BlockGroups[bgIdx];
        }

        TInode ReadInode(ui32 id) {
            return GetInodeBlockGroupById(id).ReadInode(id);
        }
        void WriteInode(const TInode& inode) {
            return GetInodeBlockGroupById(inode.Id).WriteInode(inode);
        }

        TCachedBlockFile::TPage<false> GetDataBlock(ui32 id) {
            return GetDataBlockGroupById(id).GetDataBlock(id);
        }
        TCachedBlockFile::TPage<true> GetMutableDataBlock(ui32 id) {
            return GetDataBlockGroupById(id).GetMutableDataBlock(id);
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
            sb.MetaGroupDataBlockCount = sb.BlockGroupDataBlockCount * sb.MaxBlockGroupCount;
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
        

namespace NJK {

    TVolume::TVolume(const std::string& dir, const TSettings& settings, bool ensureRoot)
        : Impl_(new TImpl(dir, settings, ensureRoot))
    {
    }

    TInode TVolume::GetRoot() {
        return Impl_->GetRoot();
    }

    TInode TVolume::AllocateInode() {
        return Impl_->AllocateInode();
    }

    void TVolume::DeallocateInode(const TInode& inode) {
        return Impl_->DeallocateInode(inode);
    }

    TInode TVolume::ReadInode(ui32 id) {
        return Impl_->ReadInode(id);
    }

    void TVolume::WriteInode(const TInode& inode) {
        Impl_->WriteInode(inode);
    }

    ui32 TVolume::AllocateDataBlock() {
        return Impl_->AllocateDataBlock();
    }

    void TVolume::DeallocateDataBlock(ui32 id) {
        Impl_->DeallocateDataBlock(id);
    }

    TCachedBlockFile::TPage<false> TVolume::GetDataBlock(ui32 id) {
        return Impl_->GetDataBlock(id);
    }

    TCachedBlockFile::TPage<true> TVolume::GetMutableDataBlock(ui32 id) {
        return Impl_->GetMutableDataBlock(id);
    }

    const TSuperBlock& TVolume::GetSuperBlock() const {
        return Impl_->GetSuperBlock();
    }

    TSuperBlock TVolume::CalcSuperBlock(const TSettings& settings) {
        return TImpl::CalcSuperBlock(settings);
    }

    TVolume::~TVolume() = default;

}