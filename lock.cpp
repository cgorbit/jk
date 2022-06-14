#include "lock.h"

namespace NJK {

    Y_NO_INLINE
    void TCondVar::FutexWait(int val) {
        syscall(SYS_futex, Word(), FUTEX_WAIT, val, nullptr, nullptr, 0);
    }

    Y_NO_INLINE
    void TCondVar::FutexWake(int count) {
        syscall(SYS_futex, Word(), FUTEX_WAKE, count, nullptr, nullptr, 0);
    }

}