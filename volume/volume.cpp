#include "volume.h"
#include "block_group.h"
#include "meta_group.h"
#include "../stream.h"

#include <vector>
#include <string>
#include <filesystem>
#include <shared_mutex>

namespace NJK {

    using namespace NVolume;

    class TVolume::TImpl {
    public:
        static constexpr size_t MaxMetaGroupCount = 5120; // 10 TiB volume

        TImpl(const std::string& dir, const TSettings& settings, bool ensureRoot);

        TInode GetRoot() {
            return ReadInode(0);
        }

        TInode AllocateInode();

        void DeallocateInode(const TInode& inode) {
            GetInodeMetaGroup(inode).DeallocateInode(inode);
        }

        ui32 AllocateDataBlock();
        ui32 AllocateDataBlock(const TInode& owner);

        void DeallocateDataBlock(ui32 id) {
            GetDataBlockMetaGroup(id).DeallocateDataBlock(id);
        }

        TMetaGroup& GetInodeMetaGroup(ui32 id) {
            return *MetaGroups_[id / SuperBlock_.MetaGroupInodeCount];
        }

        TMetaGroup& GetInodeMetaGroup(const TInode& inode) {
            return GetInodeMetaGroup(inode.Id);
        }

        TMetaGroup& GetDataBlockMetaGroup(ui32 id) {
            return *MetaGroups_[id / SuperBlock_.MetaGroupDataBlockCount];
        }

        TInode ReadInode(ui32 id) {
            return GetInodeMetaGroup(id).ReadInode(id);
        }

        void WriteInode(const TInode& inode) {
            return GetInodeMetaGroup(inode).WriteInode(inode);
        }

        TCachedBlockFile::TPage<false> GetDataBlock(ui32 id) {
            return GetDataBlockMetaGroup(id).GetDataBlock(id);
        }
        TCachedBlockFile::TPage<true> GetMutableDataBlock(ui32 id) {
            return GetDataBlockMetaGroup(id).GetMutableDataBlock(id);
        }

        void InitSuperBlock(const TSettings& settings);

        std::unique_ptr<TMetaGroup> CreateMetaGroup(size_t idx) {
            return std::make_unique<TMetaGroup>(MakeMetaGroupFilePath(idx), SuperBlock_);
        }

        void LoadMetaGroups() {
            for (size_t i = 0; i < MaxMetaGroupCount; ++i) {
                const auto path = MakeMetaGroupFilePath(i);
                if (!std::filesystem::exists(path)) {
                    break;
                }
                MetaGroups_.push_back(std::make_unique<TMetaGroup>(path, SuperBlock_));
                ++AliveMetaGroupCount_;
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

        const std::string& GetFsDir() const {
            return Directory_;
        }

        static TSuperBlock CalcSuperBlock(const TSettings& settings);

        // Super Block (1 block)
        // Block Group Descriptors (1-several blocks) (16 descriptors if 2 GiB file size and 128 MiB block group)
        // Block Groups [
        //    Data Block Bitmap (1 block)
        //    Inode Bitmap (1 block)
        //    Inode Table (inode_size blocks)
        //    Data Blocks (32K blocks at maximum (128 MiB)) 
        // ]

    private:
        std::string Directory_;
        TSuperBlock SuperBlock_;
        std::atomic<size_t> AliveMetaGroupCount_{0};
        std::mutex Lock_;
        std::vector<std::unique_ptr<TMetaGroup>> MetaGroups_;
    };

    TVolume::TImpl::TImpl(const std::string& dir, const TSettings& settings, bool ensureRoot)
        : Directory_(dir)
    {
        // We can implement this similar to std::deque (fixed vector of fixed vectors)
        // but in fixed capacity (that anyway will overcome any reasonable requirements)
        // Now this vector allocate 40 KiB on 64 bit architecture
        MetaGroups_.reserve(MaxMetaGroupCount);

        InitSuperBlock(settings);
        LoadMetaGroups();
        //if (MetaGroups_.empty()) {
            //Y_ENSURE(AllocateInode().Id == 0);
        //}

        if (MetaGroups_.empty()) {
            MetaGroups_.push_back(CreateMetaGroup(0));
            ++AliveMetaGroupCount_;

            if (ensureRoot) {
                Y_ENSURE(AllocateInode().Id == 0);
            }
        }
    }

    TSuperBlock TVolume::TImpl::CalcSuperBlock(const TSettings& settings) {
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

    void TVolume::TImpl::InitSuperBlock(const TSettings& settings) {
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

    TInode TVolume::TImpl::AllocateInode() {
        // TODO No checks for volume overflow

        while (true) {
            // TODO First try last, than random

            // TODO XXX This array will can be too huge (5120 elements)
            // Need to optimize Allocate

            TODO_BETTER_CONCURRENCY

            size_t alive = 0;

            while (true) {
                const size_t alive = AliveMetaGroupCount_.load();
                //for (size_t i = 0; i < alive; ++i)
                {
                    auto inode = MetaGroups_[alive - 1]->TryAllocateInode();
                    if (inode) {
                        return *inode;
                    }
                }
                if (alive == AliveMetaGroupCount_.load()) {
                    break;
                }
            }

            std::unique_lock g(Lock_);
            const auto newAlive = AliveMetaGroupCount_.load();
            if (alive != newAlive) {
                continue;
            }
            MetaGroups_.push_back(CreateMetaGroup(newAlive));
            ++AliveMetaGroupCount_;
        }
    }

    ui32 TVolume::TImpl::AllocateDataBlock(const TInode& owner) {
        i32 id = GetInodeMetaGroup(owner).TryAllocateDataBlock(owner);
        if (id != -1) {
            return id;
        }
        return AllocateDataBlock();
    }

    ui32 TVolume::TImpl::AllocateDataBlock() {
        while (true) {
            // TODO First try last, than random

            // TODO XXX This array will can be too huge (5120 elements)
            // Need to optimize Allocate

            TODO_BETTER_CONCURRENCY

            size_t alive = 0;

            while (true) {
                const size_t alive = AliveMetaGroupCount_.load();
                //for (size_t i = 0; i < alive; ++i)
                {
                    i32 id = MetaGroups_[alive - 1]->TryAllocateDataBlock();
                    if (id != -1) {
                        return id;
                    }
                }
                if (alive == AliveMetaGroupCount_.load()) {
                    break;
                }
            }

            std::unique_lock g(Lock_);
            const auto newAlive = AliveMetaGroupCount_.load();
            if (alive != newAlive) {
                continue;
            }
            MetaGroups_.push_back(CreateMetaGroup(newAlive));
            ++AliveMetaGroupCount_;
        }
    }

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

    ui32 TVolume::AllocateDataBlock(const TInode& owner) {
        return Impl_->AllocateDataBlock(owner);
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

    const std::string& TVolume::GetFsDir() const {
        return Impl_->GetFsDir();
    }

}