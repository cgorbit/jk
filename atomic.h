#pragma once

#include <cstddef>
#include <atomic>

namespace NJK {

    inline bool TrySub(std::atomic<size_t>& counter) {
        size_t count = counter.load();
        while (true) {
            if (!count) {
                return false;
            }
            if (counter.compare_exchange_weak(count, count - 1)) {
                return true;
            }
        }
    }

}