#include "volume.h"
#include "fixed_buffer.h"
#include <cassert>
#include <iostream>
#include <sstream>

void TestDefaultSuperBlockCalc() {
    using namespace NJK;

    TVolume::TSettings settings;
    settings.BlockSize = 4096;
    settings.MaxFileSize = 2_GiB;
    settings.NameMaxLen = 32;

    const auto sb = TVolume::CalcSuperBlock(settings);
    assert(sb.BlockSize == 4096);
    assert(sb.MaxFileSize == 2_GiB);
    assert(sb.BlockGroupSize == (4096 + 4096 + /* inodes */ 2_MiB + /* data */ 128_MiB));
    assert(sb.MaxBlockGroupCount == 15);
    assert(sb.BlockGroupDescriptorsBlockCount == 1);
    assert(sb.ZeroBlockGroupOffset == 4096);
    assert(sb.BlockGroupInodeCount == 32768);
    assert(sb.MetaGroupInodeCount == 491520);
}

void TestSuperBlockSerialization() {
    using namespace NJK;

    const auto src = TVolume::CalcSuperBlock({});
    auto buf = TFixedBuffer::Aligned(src.BlockSize);
    TBufOutput out(buf);
    //src.Serialize(out);
    SerializeChecked(out, src);

    TVolume::TSuperBlock dst;
    TBufInput in(buf);
    DeserializeChecked(in, dst);
    //dst.Deserialize(in);

    assert(src.BlockSize == dst.BlockSize);
#define CMP(field) assert(src.field == dst.field);
    CMP(BlockSize);
    CMP(BlockGroupCount);
    CMP(MaxBlockGroupCount);
    CMP(BlockGroupSize);
    CMP(BlockGroupDescriptorsBlockCount);
    CMP(MetaGroupCount); // File count
    CMP(MaxFileSize);
#undef CMP
}

template <typename T>
void CheckOnDiskSize() {
    using namespace NJK;

    //auto buf = TFixedBuffer::Aligned(4096);
    T src;
    //TBufOutput out(buf);
    TNullOutput out;
    SerializeChecked(out, src);

    T dst;
    //TBufInput in(buf);
    TNullInput in;
    DeserializeChecked(in, dst);
}

void TestInodeAllocation() {
    using namespace NJK;

    const std::string volumePath = "./var/volume_inodes";
    std::filesystem::remove_all(volumePath);
    {
        TVolume vol(volumePath, {});

        auto& meta = *vol.MetaGroups_[0];
        if (meta.AliveBlockGroupCount == 0) {
            meta.AllocateNewBlockGroup();
        }

        auto& bg = *meta.BlockGroups[0];

        for (size_t i = 0; i < 10; ++i) {
            auto inode = bg.AllocateInode();
            assert(inode.Id == i);
            inode.CreationTime = i;
            inode.BlockCount = i;
            bg.WriteInode(inode);
        }
    }

    {
        TVolume vol(volumePath, {});

        auto& meta = *vol.MetaGroups_[0];
        assert(meta.AliveBlockGroupCount = 1);

        auto& bg = *meta.BlockGroups[0];

        for (size_t i = 0; i < 10; ++i) {
            auto inode = bg.AllocateInode();
            assert(inode.Id == 10 + i);
        }
        for (size_t i = 0; i < 10; ++i) {
            auto inode = bg.ReadInode(i);
            assert(inode.Id == i);
            assert(inode.CreationTime == i);
        }

        assert(bg.AllocateInode().Id == 20);

        bg.DeallocateInode({.Id = 7});
        assert(bg.AllocateInode().Id == 7);

        bg.DeallocateInode({.Id = 13});
        bg.DeallocateInode({.Id = 17});
        assert(bg.AllocateInode().Id == 13);
    }
}

int main() {
    using namespace NJK;

    TestDefaultSuperBlockCalc();
    TestSuperBlockSerialization();

    CheckOnDiskSize<TVolume::TSuperBlock>();
    CheckOnDiskSize<TVolume::TInode>();
    CheckOnDiskSize<TVolume::TBlockGroupDescr>();

    TestInodeAllocation();

    TVolume vol("./var/volume1", {});
    (void)vol;


    return 0;
}