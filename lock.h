#pragma once

#include "common.h"

#include <atomic>
#include <linux/futex.h>
#include <sys/time.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <climits>

namespace NJK {

    class TNaiveSpinLock {
    public:
        void lock() {
            while (Value_.test_and_set(std::memory_order::acquire)) {
                //while (Value_.test(std::memory_order::relaxed)) {
                    // TODO pause
                //}
            }
        }

        void unlock() {
            Value_.clear(std::memory_order::release);
        }

    private:
        std::atomic_flag Value_;
    };

    template <typename T>
    class TLockGuard {
    public:
        TLockGuard(T& lock)
            : Lock_(&lock)
        {
        }

        TLockGuard(const TLockGuard&) = delete;

        TLockGuard(TLockGuard&& other) noexcept {
            Swap(other);
        }

        ~TLockGuard() {
            if (Lock_) {
                Lock_->unlock();
            }
        }

        TLockGuard& operator= (const TLockGuard&) = delete;

        TLockGuard& operator= (TLockGuard&& other) noexcept {
            TLockGuard tmp(std::move(other));
            Swap(tmp);
            return *this;
        }

        void unlock() {
            Lock_->unlock();
            Lock_ = nullptr;
        }

    private:
        void Swap(TLockGuard& other) noexcept {
            std::swap(Lock_, other.Lock_);
        }

    private:
        T* Lock_{};
    };

    template <typename T>
    inline auto MakeGuard(T& lock) {
        return TLockGuard{lock};
    }

    class TCondVar {
    public:
        template <typename T>
        void Wait(T& lock) {
            auto iter = Iter_.load();
            lock.unlock();
            ++Waiting_;
            FutexWait(iter);
            --Waiting_;
            lock.lock();
        }

        void NotifyOne() {
            ++Iter_;
            if (Waiting_.load()) {
                FutexWake(1);
            }
        }

        void NotifyAll() {
            ++Iter_;
            if (Waiting_.load()) {
                FutexWake(INT_MAX);
            }
        }
    private:
        int* Word() {
            static_assert(sizeof(int) == sizeof(Iter_));
            return reinterpret_cast<int*>(&Iter_);
        }

        void FutexWait(int val) {
            syscall(SYS_futex, Word(), FUTEX_WAIT, val, nullptr, nullptr, 0);
        }

        void FutexWake(int count) {
            syscall(SYS_futex, Word(), FUTEX_WAKE, count, nullptr, nullptr, 0);
        }

    private:
        std::atomic<int> Iter_;
        std::atomic<size_t> Waiting_;
    };

}