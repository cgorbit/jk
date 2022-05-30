#include "common.h"
#include "volume.h"
#include "storage.h"
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
            inode.Val.BlockCount = i;
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
    }

    {
        TVolume vol(volumePath, {});

        auto& meta = *vol.MetaGroups_[0];
        assert(meta.AliveBlockGroupCount = 1);

        auto& bg = *meta.BlockGroups[0];

        assert(bg.AllocateInode().Id == 13);
    }
}

void TestDataBlockAllocation() {
    using namespace NJK;

    const std::string volumePath = "./var/volume_data";
    std::filesystem::remove_all(volumePath);
    {
        TVolume vol(volumePath, {});

        auto& meta = *vol.MetaGroups_[0];
        if (meta.AliveBlockGroupCount == 0) {
            meta.AllocateNewBlockGroup();
        }

        auto& bg = *meta.BlockGroups[0];

        for (size_t i = 0; i < 10; ++i) {
            auto block = bg.AllocateDataBlock();
            assert(block.Id == i);
            block.Buf.Data()[100] = i;
            block.Buf.Data()[10] = i;
            bg.WriteDataBlock(block);
        }
    }

    {
        TVolume vol(volumePath, {});

        auto& meta = *vol.MetaGroups_[0];
        assert(meta.AliveBlockGroupCount = 1);

        auto& bg = *meta.BlockGroups[0];

        for (size_t i = 0; i < 10; ++i) {
            auto inode = bg.AllocateDataBlock();
            assert(inode.Id == 10 + i);
        }
        for (size_t i = 0; i < 10; ++i) {
            auto inode = bg.ReadDataBlock(i);
            assert(inode.Id == i);
            assert(inode.Buf.Data()[10] == i);
            assert(inode.Buf.Data()[100] == i);
        }

        assert(bg.AllocateDataBlock().Id == 20);

        bg.DeallocateDataBlock({.Id = 7, .Buf = TFixedBuffer::Aligned(4096)});
        assert(bg.AllocateDataBlock().Id == 7);

        bg.DeallocateDataBlock({.Id = 13, .Buf = TFixedBuffer::Aligned(4096)});
        bg.DeallocateDataBlock({.Id = 17, .Buf = TFixedBuffer::Aligned(4096)});
    }

    {
        TVolume vol(volumePath, {});

        auto& meta = *vol.MetaGroups_[0];
        assert(meta.AliveBlockGroupCount = 1);

        auto& bg = *meta.BlockGroups[0];

        assert(bg.AllocateDataBlock().Id == 13);
    }
}

template <typename T>
void AssertValue(const NJK::TVolume::TInode& inode, const T& expect, NJK::TVolume::TInodeDataOps& ops) {
    auto sbinVal = ops.GetValue(inode);
    auto sbinValPtr = std::get_if<T>(&sbinVal);
    if (!sbinValPtr) {
        throw std::runtime_error("Value not set");
    }
    if (*sbinValPtr != expect) {
        std::stringstream s;
        if constexpr (std::is_same_v<T, std::monostate>) {
            s << "Values not equal";
        } else {
            s << "Values not equal: expect " << expect << ", got " << *sbinValPtr;
        }
        throw std::runtime_error(s.str());
    }
}

void TestInodeDataOps() {
    using namespace NJK;

    const std::string volumePath = "./var/volume_inode_data_ops";
    std::filesystem::remove_all(volumePath);
    {
        TVolume vol(volumePath, {});
        auto& meta = *vol.MetaGroups_[0];
        if (meta.AliveBlockGroupCount == 0) {
            meta.AllocateNewBlockGroup();
        }
        auto& bg = *meta.BlockGroups[0];

        TVolume::TInodeDataOps ops(bg);

        auto root = bg.AllocateInode();

        ops.AddChild(root, "bin");
        auto sbin = ops.AddChild(root, "sbin");
        ops.AddChild(root, "root");
        auto home = ops.AddChild(root, "home");
        ops.AddChild(root, "etc");

        auto trofimenkov = ops.AddChild(home, "trofimenkov");
        ops.AddChild(home, "snowball");

        ops.AddChild(trofimenkov, ".vim");

        ops.SetValue(sbin, (ui32)777);
        ops.SetValue(trofimenkov, std::string{"Handsome"});
        ops.SetValue(home, std::string{"Sweet Home"});
    }

    {
        TVolume vol(volumePath, {});
        auto& meta = *vol.MetaGroups_[0];
        auto& bg = *meta.BlockGroups[0];
        TVolume::TInodeDataOps ops(bg);

        auto root = bg.ReadInode(0);
        auto got0 = ops.ListChildren(root);
        const std::vector<TVolume::TInodeDataOps::TDirEntry> expect0{{ 1, "bin" }, { 2, "sbin" }, { 3, "root" }, { 4, "home" }, { 5, "etc" }};
        assert(expect0 == got0);

        auto home = bg.ReadInode(4);
        auto got1 = ops.ListChildren(home);
        const std::vector<TVolume::TInodeDataOps::TDirEntry> expect1{{ 6, "trofimenkov" }, { 7, "snowball" }};
        assert(expect1 == got1);

        assert(ops.LookupChild(home, "snowball")->Id == 7);
        assert(!ops.LookupChild(home, "sdf").has_value());

        auto trofimenkov = bg.ReadInode(6);
        auto got2 = ops.ListChildren(trofimenkov);
        const std::vector<TVolume::TInodeDataOps::TDirEntry> expect2{{ 8, ".vim" } };
        assert(expect2 == got2);

        ops.RemoveChild(root, "root");
        auto got3 = ops.ListChildren(root);
        const std::vector<TVolume::TInodeDataOps::TDirEntry> expect3{{ 1, "bin" }, { 2, "sbin" }, { 4, "home" }, { 5, "etc" }};
        assert(expect3 == got3);

        assert(ops.LookupChild(root, "home")->Id == 4);

        auto sbin = ops.LookupChild(root, "sbin");
        assert(sbin.has_value() && sbin->Id == 2);

        AssertValue(*sbin, (ui32)777, ops);
        AssertValue(trofimenkov, std::string{"Handsome"}, ops);

        ops.SetValue(trofimenkov, (float)1.46);
    }
    {
        TVolume vol(volumePath, {});
        auto& meta = *vol.MetaGroups_[0];
        auto& bg = *meta.BlockGroups[0];
        TVolume::TInodeDataOps ops(bg);

        auto trofimenkov = bg.ReadInode(6);
        auto sbin = bg.ReadInode(2);

        AssertValue(sbin, (ui32)777, ops);
        AssertValue(trofimenkov, (float)1.46, ops);

        ops.UnsetValue(trofimenkov);
    }

    {
        TVolume vol(volumePath, {});
        auto& meta = *vol.MetaGroups_[0];
        auto& bg = *meta.BlockGroups[0];
        TVolume::TInodeDataOps ops(bg);

        auto home = bg.ReadInode(4);
        auto sbin = bg.ReadInode(2);
        auto trofimenkov = bg.ReadInode(6);

        AssertValue(home, std::string{"Sweet Home"}, ops);
        AssertValue(sbin, (ui32)777, ops);
        AssertValue(trofimenkov, std::monostate{}, ops);

        ops.SetValue(trofimenkov, (ui32)1987);
    }

    {
        TVolume vol(volumePath, {});
        auto& meta = *vol.MetaGroups_[0];
        auto& bg = *meta.BlockGroups[0];
        TVolume::TInodeDataOps ops(bg);

        auto trofimenkov = bg.ReadInode(6);

        AssertValue(trofimenkov, (ui32)1987, ops);
    }
}

void AssertValuesEqual(const NJK::TInodeValue& lhs, const NJK::TInodeValue& rhs) {
    using namespace NJK;

    assert(lhs.index() == rhs.index());
    if (std::holds_alternative<NJK::TBlobView>(lhs)) {
        throw std::runtime_error("Can't compare TBlobView");
    } else if (std::holds_alternative<std::string>(lhs)) {
        assert(std::get<std::string>(lhs) == std::get<std::string>(rhs));
    } else if (std::holds_alternative<ui32>(lhs)) {
        assert(std::get<ui32>(lhs) == std::get<ui32>(rhs));
    } else {
        Y_FAIL("todo");
    }
}

void TestStorage() {
    using namespace NJK;
    using TValue = TInodeValue;

    const std::string volumePath = "./var/volume_storage";
    std::filesystem::remove_all(volumePath);
    {
        TStorage storage({volumePath});

        storage.Set("/home/trofimenkov/bar/.vimrc", (ui32)10);
        storage.Set("/home/trofimenkov/bar", std::string{"Hello"});

        AssertValuesEqual(storage.Get("/home/trofimenkov/bar/.vimrc"), TValue{(ui32)10});
        AssertValuesEqual(storage.Get("//home////trofimenkov/bar/"), TValue{std::string{"Hello"}});
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
    TestDataBlockAllocation();
    TestInodeDataOps();

    TestStorage();

    return 0;
}