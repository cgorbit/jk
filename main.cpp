#include "common.h"
#include "volume.h"
#include "volume/ops.h"
#include "volume/block_group.h"
#include "storage.h"
#include "fixed_buffer.h"

#include <cassert>
#include <iostream>
#include <sstream>
#include <shared_mutex>
#include <condition_variable>
#include <filesystem>

using NJK::NVolume::TInodeDataOps;
using NJK::NVolume::TInodeValue;

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
    assert(sb.BlockGroupDataBlockCount == 32768);
    assert(sb.MetaGroupInodeCount == 491520);
    assert(sb.MetaGroupDataBlockCount == 491520);

    assert(TVolume::TInode::OnDiskSize == 64);
    const ui32 inodeBlocks = sb.BlockGroupInodeCount * TVolume::TInode::OnDiskSize / sb.BlockSize;
    assert(inodeBlocks == 64);
    assert((sb.BlockGroupInodeCount * TVolume::TInode::OnDiskSize) % sb.BlockSize == 0);
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
        TVolume vol(volumePath, {}, false);

        //auto& meta = *vol.MetaGroups_[0];
        //if (meta.AliveBlockGroupCount == 0) {
        //    meta.AllocateNewBlockGroup();
        //}

        //auto& bg = *meta.BlockGroups[0];

        for (size_t i = 0; i < 10; ++i) {
            auto inode = vol.AllocateInode();
            assert(inode.Id == i);
            inode.CreationTime = i;
            inode.Val.BlockCount = i; // just set meaningless value
            vol.WriteInode(inode);

            assert(vol.ReadInode(i).CreationTime == i);
        }
    }

    {
        TVolume vol(volumePath, {}, false);

        //auto& meta = *vol.MetaGroups_[0];
        //assert(meta.AliveBlockGroupCount = 1);

        //auto& bg = *meta.BlockGroups[0];

        for (size_t i = 0; i < 10; ++i) {
            auto inode = vol.AllocateInode();
            assert(inode.Id == 10 + i);
        }
        for (size_t i = 0; i < 10; ++i) {
            auto inode = vol.ReadInode(i);
            assert(inode.Id == i);
            assert(inode.CreationTime == i);
            assert(inode.Val.BlockCount == i);
        }

        assert(vol.AllocateInode().Id == 20);

        vol.DeallocateInode({.Id = 7});
        assert(vol.AllocateInode().Id == 7);

        vol.DeallocateInode({.Id = 13});
        vol.DeallocateInode({.Id = 17});
    }

    {
        TVolume vol(volumePath, {}, false);

        //auto& meta = *vol.MetaGroups_[0];
        //assert(meta.AliveBlockGroupCount = 1);

        //auto& bg = *meta.BlockGroups[0];

        assert(vol.AllocateInode().Id == 13);
    }
}

void TestDataBlockAllocation() {
    using namespace NJK;

    const std::string volumePath = "./var/volume_data";
    std::filesystem::remove_all(volumePath);
    {
        TVolume vol(volumePath, {}, false);

        //auto& meta = *vol.MetaGroups_[0];
        //if (meta.AliveBlockGroupCount == 0) {
            //meta.AllocateNewBlockGroup();
        //}

        //auto& bg = *meta.BlockGroups[0];

        for (size_t i = 0; i < 10; ++i) {
            auto blockId = vol.AllocateDataBlock();
            assert(blockId == i);
            auto block = vol.GetMutableDataBlock(blockId);
            block.Buf().MutableData()[100] = i;
            block.Buf().MutableData()[10] = i;
        }
    }

    {
        TVolume vol(volumePath, {}, false);

        //auto& meta = *vol.MetaGroups_[0];
        //assert(meta.AliveBlockGroupCount = 1);

        //auto& bg = *meta.BlockGroups[0];

        for (size_t i = 0; i < 10; ++i) {
            auto blockId = vol.AllocateDataBlock();
            assert(blockId == 10 + i);
        }
        for (size_t i = 0; i < 10; ++i) {
            auto block = vol.GetDataBlock(i);
            assert(block.Buf().Data()[10] == i);
            assert(block.Buf().Data()[100] == i);
        }

        assert(vol.AllocateDataBlock() == 20);

        vol.DeallocateDataBlock(7);
        assert(vol.AllocateDataBlock() == 7);

        vol.DeallocateDataBlock(13);
        vol.DeallocateDataBlock(17);
    }

    {
        TVolume vol(volumePath, {}, false);

        //auto& meta = *vol.MetaGroups_[0];
        //assert(meta.AliveBlockGroupCount = 1);

        //auto& bg = *meta.BlockGroups[0];

        assert(vol.AllocateDataBlock() == 13);
    }
}

template <typename T>
void AssertValue(const NJK::TVolume::TInode& inode, const T& expect, TInodeDataOps& ops) {
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
        TVolume vol(volumePath, {}, false);
        //auto& meta = *vol.MetaGroups_[0];
        //if (meta.AliveBlockGroupCount == 0) {
        //    meta.AllocateNewBlockGroup();
        //}
        //auto& bg = *meta.BlockGroups[0];

        TInodeDataOps ops(&vol);

        auto root = vol.AllocateInode();

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

//ops.DumpTree(std::cerr);
    }

    {
        TVolume vol(volumePath, {}, false);
        //auto& meta = *vol.MetaGroups_[0];
        //auto& bg = *meta.BlockGroups[0];
        TInodeDataOps ops(&vol);

        auto root = vol.ReadInode(0);
        auto got0 = ops.ListChildren(root);
        const std::vector<TInodeDataOps::TDirEntry> expect0{{ 1, "bin" }, { 2, "sbin" }, { 3, "root" }, { 4, "home" }, { 5, "etc" }};
        assert(expect0 == got0);

        auto home = vol.ReadInode(4);
        auto got1 = ops.ListChildren(home);
        const std::vector<TInodeDataOps::TDirEntry> expect1{{ 6, "trofimenkov" }, { 7, "snowball" }};
        assert(expect1 == got1);

        assert(ops.LookupChild(home, "snowball")->Id == 7);
        assert(!ops.LookupChild(home, "sdf").has_value());

        auto trofimenkov = vol.ReadInode(6);
        auto got2 = ops.ListChildren(trofimenkov);
        const std::vector<TInodeDataOps::TDirEntry> expect2{{ 8, ".vim" } };
        assert(expect2 == got2);

        ops.RemoveChild(root, "root");
        auto got3 = ops.ListChildren(root);
        const std::vector<TInodeDataOps::TDirEntry> expect3{{ 1, "bin" }, { 2, "sbin" }, { 4, "home" }, { 5, "etc" }};
        assert(expect3 == got3);

        assert(ops.LookupChild(root, "home")->Id == 4);

        auto sbin = ops.LookupChild(root, "sbin");
        assert(sbin.has_value() && sbin->Id == 2);

        AssertValue(*sbin, (ui32)777, ops);
        AssertValue(trofimenkov, std::string{"Handsome"}, ops);

        ops.SetValue(trofimenkov, (float)1.46);
//ops.DumpTree(std::cerr);
    }
    {
        TVolume vol(volumePath, {}, false);
        //auto& meta = *vol.MetaGroups_[0];
        //auto& bg = *meta.BlockGroups[0];
        TInodeDataOps ops(&vol);

        auto trofimenkov = vol.ReadInode(6);
        auto sbin = vol.ReadInode(2);

        AssertValue(sbin, (ui32)777, ops);
        AssertValue(trofimenkov, (float)1.46, ops);

        ops.UnsetValue(trofimenkov);
//ops.DumpTree(std::cerr);
    }

    {
        TVolume vol(volumePath, {}, false);
        //auto& meta = *vol.MetaGroups_[0];
        //auto& bg = *meta.BlockGroups[0];
        TInodeDataOps ops(&vol);

        auto home = vol.ReadInode(4);
        auto sbin = vol.ReadInode(2);
        auto trofimenkov = vol.ReadInode(6);

        AssertValue(home, std::string{"Sweet Home"}, ops);
        AssertValue(sbin, (ui32)777, ops);
        AssertValue(trofimenkov, std::monostate{}, ops);

        ops.SetValue(trofimenkov, (ui32)1987);
//ops.DumpTree(std::cerr);
    }

    {
        TVolume vol(volumePath, {}, false);
        //auto& meta = *vol.MetaGroups_[0];
        //auto& bg = *meta.BlockGroups[0];
        TInodeDataOps ops(&vol);

        auto trofimenkov = vol.ReadInode(6);

        AssertValue(trofimenkov, (ui32)1987, ops);
//ops.DumpTree(std::cerr);
    }
}

void AssertValuesEqual(const TInodeValue& lhs, const TInodeValue& rhs) {
    using namespace NJK;

    assert(lhs.index() == rhs.index());
    if (std::holds_alternative<NVolume::TBlobView>(lhs)) {
        throw std::runtime_error("Can't compare TBlobView");
    } else if (std::holds_alternative<std::string>(lhs)) {
        assert(std::get<std::string>(lhs) == std::get<std::string>(rhs));
    } else if (std::holds_alternative<ui32>(lhs)) {
        assert(std::get<ui32>(lhs) == std::get<ui32>(rhs));
    } else if (std::holds_alternative<float>(lhs)) {
        assert(std::get<float>(lhs) == std::get<float>(rhs));
    } else if (std::holds_alternative<double>(lhs)) {
        assert(std::get<double>(lhs) == std::get<double>(rhs));
    } else if (std::holds_alternative<bool>(lhs)) {
        assert(std::get<bool>(lhs) == std::get<bool>(rhs));
    } else if (std::holds_alternative<std::monostate>(lhs)) {
        // nothing
    } else {
        Y_FAIL("TODO AssertValuesEqual");
    }
}

static_assert(sizeof(std::mutex) == 40, ""); // FIXME Why?
static_assert(sizeof(std::shared_mutex) == 56, ""); // FIXME Why?
static_assert(sizeof(std::condition_variable) == 48, ""); // FIXME Why?
//static_assert(sizeof(std::stack<TMount*>) == 80, ""); // FIXME Why?
//static_assert(sizeof(std::stack<std::unique_ptr<TMount>>) == 80, "");
//static_assert(sizeof(TInode) == 72, "");
//static_assert(sizeof(std::unique_ptr<size_t>) == 8, "");
//static_assert(sizeof(std::shared_ptr<size_t>) == 16, "");

// must be sorted in ascii order
void AssertTreeEqual(NJK::TVolume& vol, std::string_view expect) {
    using namespace NJK;
    TInodeDataOps ops(&vol);
    while (expect.size() && expect[0] == '\n') {
        expect = std::string_view(&*(expect.begin() + 1), expect.size() - 1);
    }
    const auto& got = ops.DumpTree();
    if (got != expect) {
        std::cerr << "Trees are not equal, expect: <[\n" << expect << "]>, but got: <[\n" << got << "]>\n";
        abort();
    }
}

void TestStorage0() {
    using namespace NJK;
    using TValue = TInodeValue;

    const std::string rootVolumePath = "./var/volume_root";
    const std::string homeVolumePath = "./var/volume_home";
    std::filesystem::remove_all(rootVolumePath);
    std::filesystem::remove_all(homeVolumePath);

    //auto MakeVolume = [](const std::string& path) {
    //    return TVolumePtr(new TVolume(path, {}));
    //};

    {
        TVolume root(rootVolumePath, {});
        TVolume home(homeVolumePath, {});
        {
            TStorage storage(&root);
            storage.Mount("/home", &home);

            storage.Set("/home/trofimenkov/bar/.vimrc", (ui32)10);
            storage.Set("//home/trofimenkov/bar", std::string{"Hello"});
            storage.Set("/root/.bashrc///", std::string{"config"});
            storage.Set("/README", std::string{"Linux"});

            AssertValuesEqual(storage.Get("/home/trofimenkov/bar/.vimrc"), TValue{(ui32)10});
            AssertValuesEqual(storage.Get("//home////trofimenkov/bar/"), TValue{std::string{"Hello"}});
            AssertValuesEqual(storage.Get("//root//.bashrc"), TValue{std::string{"config"}});
        }

        {
            AssertTreeEqual(root, R"(
README = string "Linux"
home
root
    .bashrc = string "config"
)");

            TInodeDataOps ops(&root);
            auto rootInode = root.GetRoot();

            assert(ops.ListChildren(rootInode).size() == 3);

            auto rootDirInode = ops.LookupChild(rootInode, "root");
            assert(rootDirInode.has_value());

            auto homeDirInode = ops.LookupChild(rootInode, "home");
            assert(homeDirInode.has_value());

            //AssertValuesEqual(, TValue{std::string{"config"}});
        }

        {
            AssertTreeEqual(home, R"(
trofimenkov
    bar = string "Hello"
        .vimrc = ui32 10
)");
            TInodeDataOps ops(&home);
            auto rootInode = home.GetRoot();

            assert(ops.ListChildren(rootInode).size() == 1);

            auto trofimenkovDirInode = ops.LookupChild(rootInode, "trofimenkov");
            assert(trofimenkovDirInode.has_value());

            //AssertValuesEqual(, TValue{std::string{"config"}});
        }
    }
}

#define VOLUME_PATH(alias) \
    const std::string alias##VolumePath = "./var/volume_" #alias; \
    std::filesystem::remove_all(alias##VolumePath);

#define VOLUME(alias) \
    TVolume alias(alias##VolumePath);

void TestStorage1() {
    using namespace NJK;
    using TValue = TInodeValue;

    VOLUME_PATH(root)
    VOLUME_PATH(homeV0)
    VOLUME_PATH(homeV1)
    VOLUME_PATH(homeV2)
    VOLUME_PATH(lib)
    VOLUME_PATH(cdrom)

    {
        VOLUME(root);
        VOLUME(homeV0);
        VOLUME(homeV1);
        VOLUME(homeV2);
        VOLUME(cdrom);
        VOLUME(lib);

        {
            TStorage storage(&root);
            storage.Set("/home/petrk/age", (ui32)43);
            storage.Set("/home/petrk", true);
            storage.Set("/home", (float)1024);
        }
        AssertTreeEqual(root, R"(
home = float 1024
    petrk = bool true
        age = ui32 43
)");

        {
            TStorage storage(&homeV0);
            storage.Set("/lazy", std::string{"old_lazy_attr"}); // will be overriden
            storage.Set("/lazy/.bashrc", std::string{"lazy-old-bashrc"}); // will be hidden
        }
        AssertTreeEqual(homeV0, R"(
lazy = string "old_lazy_attr"
    .bashrc = string "lazy-old-bashrc"
)");
        {
            TStorage storage(&homeV1);
            storage.Set("/leva/.vimrc", std::string{"Yandex.News"}); // will not be unavailable
            storage.Set("/leva", (ui32)40);
        }
        AssertTreeEqual(homeV1, R"(
leva = ui32 40
    .vimrc = string "Yandex.News"
)");

        {
            TStorage storage(&homeV2);
            storage.Set("/trofimenkov/.vimrc", std::string{"set hls"});
            storage.Set("/trofimenkov", (ui32)35);
            storage.Set("/lazy", std::string{"new-lazy-attr"});
            storage.Set("/lazy/.vimrc", std::string{"lazy-vimrc"});
        }
        AssertTreeEqual(homeV2, R"(
lazy = string "new-lazy-attr"
    .vimrc = string "lazy-vimrc"
trofimenkov = ui32 35
    .vimrc = string "set hls"
)");

        {
            TStorage storage(&lib);
            storage.Set("/ld-linux.so.2", std::string{"attr0"});
            storage.Set("/distbuild/libdistbuild.so.2", (float)0.5);
            storage.Set("/distbuild/libdistbuild.so.3", (double)0.25);
        }
        AssertTreeEqual(lib, R"(
distbuild
    libdistbuild.so.2 = float 0.5
    libdistbuild.so.3 = double 0.25
ld-linux.so.2 = string "attr0"
)");

        {
            TStorage storage(&root);
            storage.Mount("/home", &homeV0);
            storage.Mount("/home", &homeV1);
            storage.Mount("/home", &homeV2);
            storage.Mount("/mnt", &cdrom);
            storage.Mount("/lib", &lib);
            storage.Mount("/usr/lib", &lib);

            AssertValuesEqual(storage.Get("/home"), (float)1024);

            AssertValuesEqual(storage.Get("/home/lazy"), TValue{std::string{"new-lazy-attr"}});
            AssertValuesEqual(storage.Get("/home/lazy/.bashrc"), TValue{});
            AssertValuesEqual(storage.Get("/home/lazy/.vimrc"), TValue{std::string{"lazy-vimrc"}});

            AssertValuesEqual(storage.Get("/home/leva"), (ui32)40);
            AssertValuesEqual(storage.Get("/home/leva/.vimrc"), TValue{});

            AssertValuesEqual(storage.Get("/home/unknown"), {});
            AssertValuesEqual(storage.Get("/home/unknown/attr"), {});
            AssertValuesEqual(storage.Get("/home/trofimenkov/NOSUCHPATH/.vimrc"), TValue{});

            // this hidden by mounts in any case
            AssertValuesEqual(storage.Get("/home/petrk"), {});
            AssertValuesEqual(storage.Get("/home/petrk/age"), {});

            AssertValuesEqual(storage.Get("/lib/distbuild/libdistbuild.so.2"), (float)0.5);
            AssertValuesEqual(storage.Get("/usr/lib/distbuild/libdistbuild.so.2"), (float)0.5);

            AssertValuesEqual(storage.Get("/lib/ld-linux.so.2"), TValue{std::string{"attr0"}});
            AssertValuesEqual(storage.Get("/usr/lib/ld-linux.so.2"), TValue{std::string{"attr0"}});
        }
        
        // Check that nothing changed

        AssertTreeEqual(root, R"(
home = float 1024
    petrk = bool true
        age = ui32 43
lib
mnt
usr
    lib
)");
        AssertTreeEqual(homeV0, R"(
lazy = string "old_lazy_attr"
    .bashrc = string "lazy-old-bashrc"
)");
        AssertTreeEqual(homeV1, R"(
leva = ui32 40
    .vimrc = string "Yandex.News"
)");
        AssertTreeEqual(homeV2, R"(
lazy = string "new-lazy-attr"
    .vimrc = string "lazy-vimrc"
trofimenkov = ui32 35
    .vimrc = string "set hls"
)");
        AssertTreeEqual(lib, R"(
distbuild
    libdistbuild.so.2 = float 0.5
    libdistbuild.so.3 = double 0.25
ld-linux.so.2 = string "attr0"
)");

        {
            TStorage storage(&root);
            storage.Mount("/home", &homeV0);
            storage.Mount("/home", &homeV1);
            storage.Mount("/home", &homeV2);
            storage.Mount("/mnt", &cdrom);
            storage.Mount("/lib", &lib);
            storage.Mount("/usr/lib", &lib);

            storage.Set("/home/alex-sh", (ui32)12);
            storage.Set("/home/alex-sh/philosophy/fromm", TValue{std::string{"Erich Fromm"}});

            storage.Set("/usr/lib/libfoo.so", (ui32)155);
            AssertValuesEqual(storage.Get("/lib/libfoo.so"), (ui32)155);

            storage.Set("/home/leva", (ui32)42); // XXX Will update old homeV1
            storage.Set("/home/lazy/.bashrc", TValue{std::string{"lazy-new-bashrc"}});

            AssertValuesEqual(storage.Get("/home/leva"), (ui32)42);
            AssertValuesEqual(storage.Get("/home/lazy/.bashrc"), TValue{std::string{"lazy-new-bashrc"}});

            storage.Set("/etc/hosts", TValue{std::string{"127.0.0.1 localhost"}});
        }

        AssertTreeEqual(root, R"(
etc
    hosts = string "127.0.0.1 localhost"
home = float 1024
    petrk = bool true
        age = ui32 43
lib
mnt
usr
    lib
)");
        AssertTreeEqual(homeV0, R"(
lazy = string "old_lazy_attr"
    .bashrc = string "lazy-old-bashrc"
)");
        AssertTreeEqual(homeV1, R"(
leva = ui32 42
    .vimrc = string "Yandex.News"
)");
        AssertTreeEqual(homeV2, R"(
alex-sh = ui32 12
    philosophy
        fromm = string "Erich Fromm"
lazy = string "new-lazy-attr"
    .bashrc = string "lazy-new-bashrc"
    .vimrc = string "lazy-vimrc"
trofimenkov = ui32 35
    .vimrc = string "set hls"
)");
        AssertTreeEqual(lib, R"(
distbuild
    libdistbuild.so.2 = float 0.5
    libdistbuild.so.3 = double 0.25
ld-linux.so.2 = string "attr0"
libfoo.so = ui32 155
)");

    }
}

void TestStorageNonRoot() {
    using namespace NJK;
    //using TValue = TInodeValue;

    VOLUME_PATH(rootOld)
    VOLUME_PATH(rootNew)
    VOLUME_PATH(home)

    {
        VOLUME(rootOld);
        VOLUME(rootNew);
        VOLUME(home);

        {
            TStorage storage(&rootOld);
            storage.Set("/home", std::string{"old-root-home"});
            storage.Set("/home/trofimenkov", std::string{"dev"});
            storage.Set("/home/trofimenkov/attr", false);
            storage.Set("/etc/issue", std::string{"Debian"});
            storage.Set("/bin/ls", true);
            storage.Set("/bin/du", (ui32)111);
            storage.Set("/bin", std::string{"old-root-bin"});
        }
        AssertTreeEqual(rootOld, R"(
bin = string "old-root-bin"
    du = ui32 111
    ls = bool true
etc
    issue = string "Debian"
home = string "old-root-home"
    trofimenkov = string "dev"
        attr = bool false
)");

        {
            TStorage storage(&rootNew);
            storage.Set("/home", std::string{"new-root-home"});
        }
        AssertTreeEqual(rootNew, R"(
home = string "new-root-home"
)");

        {
            TStorage storage(&rootNew);
            storage.Mount("/home", &home);
            storage.Mount("/etc", &rootOld, "/etc");
            storage.Mount("/etc", &rootOld, "/etc");
            storage.Mount("/bin", &rootOld, "/bin");

            AssertValuesEqual(storage.Get("/home/trofimenkov"), {});
            AssertValuesEqual(storage.Get("/home/trofimenkov/attr"), {});

            AssertValuesEqual(storage.Get("/etc"), {});
            AssertValuesEqual(storage.Get("/home"), std::string{"new-root-home"});
            AssertValuesEqual(storage.Get("/bin"), {});

            AssertValuesEqual(storage.Get("/bin/du"), (ui32)111);
            AssertValuesEqual(storage.Get("/bin/ls"), true);
            AssertValuesEqual(storage.Get("/etc/issue"), std::string{"Debian"});
        }
        

    }
}

int main() {
    using namespace NJK;

    TestDefaultSuperBlockCalc();
    TestSuperBlockSerialization();

    CheckOnDiskSize<TVolume::TSuperBlock>();
    CheckOnDiskSize<TVolume::TInode>();
    CheckOnDiskSize<NVolume::TBlockGroupDescr>();

    TestInodeAllocation();
    TestDataBlockAllocation();
    TestInodeDataOps();

    TestStorage0();
    TestStorage1();
    TestStorageNonRoot();

    return 0;
}