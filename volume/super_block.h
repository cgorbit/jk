#pragma once

#include "../saveload.h"

namespace NJK::NVolume {

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
        ui32 MetaGroupDataBlockCount = 0;

        Y_DECLARE_SERIALIZATION

        TFixedBuffer NewBuffer() const {
            return TFixedBuffer::Aligned(BlockSize);
        }

        static constexpr ui32 OnDiskSize = 48;
    };

}