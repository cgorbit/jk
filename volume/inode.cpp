
#include "inode.h"

namespace NJK::NVolume {

    Y_DEFINE_SERIALIZATION(TInode,
        CreationTime, ModTime,
        Val.Type, Val.BlockCount, Val.FirstBlockId, Val.Deadline,
        Dir.HasChildren, Dir.BlockCount, Dir.FirstBlockId,
        Data,
        TSkipMe{ToSkip}
    )

}