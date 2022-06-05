#include "super_block.h"

namespace NJK::NVolume {

    Y_DEFINE_SERIALIZATION(TSuperBlock,
        BlockSize,
        BlockGroupCount,
        MaxBlockGroupCount,
        BlockGroupSize,
        BlockGroupDescriptorsBlockCount,
        MetaGroupCount, // File count for check
        MaxFileSize,
        ZeroBlockGroupOffset,
        BlockGroupInodeCount,
        BlockGroupDataBlockCount,
        MetaGroupInodeCount,
        MetaGroupDataBlockCount
    );

}