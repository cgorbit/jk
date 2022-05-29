#include "volume.h"
#include "saveload.h"

namespace NJK {

    void TVolume::TSuperBlock::Serialize(IOutputStream& out) const {
        ::NJK::Serialize(out, BlockSize);
        ::NJK::Serialize(out, BlockGroupCount);
        ::NJK::Serialize(out, MaxBlockGroupCount);
        ::NJK::Serialize(out, BlockGroupSize);
        ::NJK::Serialize(out, BlockGroupDescriptorsBlockCount);
        ::NJK::Serialize(out, MetaGroupCount); // File count
        ::NJK::Serialize(out, MaxFileSize);
        ::NJK::Serialize(out, ZeroBlockGroupOffset);
        ::NJK::Serialize(out, BlockGroupInodeCount);
        ::NJK::Serialize(out, BlockGroupDataBlockCount);
        ::NJK::Serialize(out, MetaGroupInodeCount);
    }

    void TVolume::TSuperBlock::Deserialize(IInputStream& in) {
        ::NJK::Deserialize(in, BlockSize);
        ::NJK::Deserialize(in, BlockGroupCount);
        ::NJK::Deserialize(in, MaxBlockGroupCount);
        ::NJK::Deserialize(in, BlockGroupSize);
        ::NJK::Deserialize(in, BlockGroupDescriptorsBlockCount);
        ::NJK::Deserialize(in, MetaGroupCount); // File count
        ::NJK::Deserialize(in, MaxFileSize);
        ::NJK::Deserialize(in, ZeroBlockGroupOffset);
        ::NJK::Deserialize(in, BlockGroupInodeCount);
        ::NJK::Deserialize(in, BlockGroupDataBlockCount);
        ::NJK::Deserialize(in, MetaGroupInodeCount);
    }

    void TVolume::TBlockGroupDescr::Serialize(IOutputStream& out) const {
        ::NJK::Serialize(out, D.CreationTime);
        ::NJK::Serialize(out, D.FreeInodeCount);
        ::NJK::Serialize(out, D.FreeDataBlockCount);
        ::NJK::Serialize(out, D.DirectoryCount);
    }
    void TVolume::TBlockGroupDescr::Deserialize(IInputStream& in) {
        ::NJK::Deserialize(in, D.CreationTime);
        ::NJK::Deserialize(in, D.FreeInodeCount);
        ::NJK::Deserialize(in, D.FreeDataBlockCount);
        ::NJK::Deserialize(in, D.DirectoryCount);
    }
}