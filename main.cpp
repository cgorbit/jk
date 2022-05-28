#include "volume.h"
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
    TFixedBuffer buf(src.BlockSize);
    TBufOutput out(buf);
    src.Serialize(out);

    TVolume::TSuperBlock dst;
    TBufInput in(buf);
    dst.Deserialize(in);

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

int main() {
    using namespace NJK;

    TestDefaultSuperBlockCalc();
    TestSuperBlockSerialization();

    TVolume vol("./var/volume1", {});
    (void)vol;
    assert(vol.GetSuperBlock().BlockSize == 4096);

    return 0;
}