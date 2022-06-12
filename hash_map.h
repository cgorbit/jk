#pragma once

#include "lock.h"

#include <unordered_map>
#include <shared_mutex>
#include <list>
#include <atomic>
#include <vector>

namespace NJK {

    extern const std::size_t HashTablePrimes[];

    template <typename K, typename T, typename Hash = std::hash<K>, typename L = TNaiveSpinLock>
    class THashMap {
    private:
        struct TKeyValue;

    public:
        using TKey = K;
        using TLock = L;

        class TValuePtr {
        public:
            TValuePtr() = default;

            explicit TValuePtr(TKeyValue* ptr)
                : Ptr_(ptr)
            {
            }

            TValuePtr(const TValuePtr&) = delete;

            TValuePtr(TValuePtr&& other) noexcept {
                Swap(other);
            }

            ~TValuePtr() {
                if (Ptr_) {
                    --Ptr_->RefCount;
                }
            }

            explicit operator bool () const {
                return Ptr_ != nullptr;
            }

            TValuePtr& operator=(const TValuePtr&) = delete;

            TValuePtr& operator=(TValuePtr&& other) noexcept {
                TValuePtr tmp(std::move(other));
                Swap(tmp);
                return *this;
            }

            T* operator-> () const {
                return &Ptr_->Value;
            }

            T& operator* () const {
                return Ptr_->Value;
            }

            T* Ptr() const {
                return &Ptr_->Value;
            }

            void Swap(TValuePtr& other) {
                std::swap(Ptr_, other.Ptr_);
            }

        private:
            TKeyValue* Ptr_ = nullptr;
        };

        THashMap() {
            std::unique_lock g(ResizeLock_);
            Resize(HashTablePrimes[CapacityIdx_]);
        }

        size_t bucket_count() {
            std::shared_lock g(ResizeLock_);
            return Buckets_.size();
        }

        size_t size() const {
            return Size_.load(std::memory_order::relaxed);
        }

        float load_factor() {
            return size() * 1.0 / bucket_count();
        }

        struct TLookupResult {
            TValuePtr Obj;
            bool Created = false;
        };

        TValuePtr Find(const TKey& key) {
            return Lookup(key, false).Obj;
        }

        TValuePtr operator[] (const TKey& key) {
            return Lookup(key, true).Obj;
        }

        TLookupResult emplace_key(const TKey& key) {
            return Lookup(key, true);
        }

    private:
        TLookupResult Lookup(const TKey& key, bool create) {
            const auto hash = Hash_(key);
            float loadFactor = 0;
            bool created = false;

            TKeyValue* kv = nullptr;
            {
                std::shared_lock g(ResizeLock_);
                auto& bucket = Buckets_[hash % Buckets_.size()];

                auto g1 = MakeGuard(*bucket.Lock);

                for (auto& item : bucket.Chain) {
                    if (item.Key == key) {
                        kv = &item;
                        break;
                    } 
                }

                if (!kv && create) {
                    kv = &bucket.Chain.emplace_front(key);
                    ++Size_;
                    loadFactor = Size_.load() * 1.0 / Buckets_.size();
                    created = true;
                }

                if (kv) {
                    ++kv->RefCount;
                }
            }

            if (!kv && !create) {
                return TLookupResult{TValuePtr{}, false};
            }

            if (loadFactor > MaxLoadFactor_) {
                std::unique_lock g(ResizeLock_);
                loadFactor = Size_.load() * 1.0 / Buckets_.size();
                if (loadFactor > MaxLoadFactor_) {
                    Resize(HashTablePrimes[++CapacityIdx_]);
                }
            }

            return {TValuePtr{kv}, created};
        }

    private:
        void Resize(size_t size) {
            std::vector<TBucket> newBuckets;
            newBuckets.resize(size);

            for (auto& oldBucket : Buckets_) {
                auto& oldChain = oldBucket.Chain;
                for (auto it = oldChain.begin(); it != oldChain.end();) {
                    auto next = it;
                    ++next;
                    const auto hash = Hash_(it->Key);
                    auto& newBucket = newBuckets[hash % newBuckets.size()];
                    newBucket.Chain.splice(newBucket.Chain.begin(), oldChain, it);
                    it = next;
                }
            }
            for (auto& bucket : newBuckets) {
                bucket.Lock.reset(new TLock());
            }

            Buckets_.swap(newBuckets);
        }

    private:
        struct TKeyValue {
            TKeyValue() = default;

            template <typename U>
            TKeyValue(U&& key)
                : Key(std::forward<U>(key))
            {
            }

            template <typename U, typename V>
            TKeyValue(U&& key, V&& value)
                : Key(std::forward<U>(key))
                , Value(std::forward<V>(value))
            {
            }

            TKey Key{};
            T Value{};
            std::atomic<size_t> RefCount{0}; // atomic, because bucket lock is changed on hash table resize
        };

        const float MaxLoadFactor_ = 1.0;
        std::shared_mutex ResizeLock_;
        //using TKeyValue = std::pair<TKey, TValueWithRefCount>;
        struct TBucket {
            std::unique_ptr<TLock> Lock;
            std::list<TKeyValue> Chain;
        };
        std::vector<TBucket> Buckets_;
        std::atomic<size_t> Size_{0};
        Hash Hash_{};
        size_t CapacityIdx_ = 0;
    };

}