#include "bitset.h"

namespace NJK {

    i32 TBlockBitSet::FindUnset() const {
        // TODO Buf must be aligned to machine word
        const size_t* buf = reinterpret_cast<const size_t*>(Buf_.Data());
        for (size_t i = 0; i < Buf_.Size() / sizeof(size_t); ++i) {
            auto v = buf[i];
            if (v != (size_t)-1) {
                // TODO Binary search here
                for (size_t j = 0; j < sizeof(size_t); ++j) {
                    if ((v & 0xff) != 0xff) {
                        for (size_t k = 0; k < 8; ++k) {
                            if ((v & 1) == 0) {
                                return i * sizeof(size_t) + j * 8 + k;
                            }
                            v >>= 1;
                        }
                    }
                    v >>= 8;
                }
            }
        }
        return -1;
    }

}