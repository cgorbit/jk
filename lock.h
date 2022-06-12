#pragma once

#include "common.h"

#include <atomic>

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
        void Wait(T&) {
            Y_TODO("");
        }

        void NotifyAll() {
            Y_TODO("");
        }

        void NotifyOne() {
            Y_TODO("");
        }
    };

}