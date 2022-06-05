#pragma once

#include <unordered_map>
#include <shared_mutex>
#include <list>
#include <atomic>
#include <vector>

namespace NJK {

    template <typename K, typename T, typename Hash = std::hash<K>>
    class THashMap {
    public:
        using TKey = K;

        THashMap() {
            std::unique_lock g(ResizeLock_);
            Resize(1);
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

        T& operator[] (const TKey& key) {
            const auto hash = Hash_(key);
            float loadFactor = 0;

            T* value = nullptr;
            {
                std::shared_lock g(ResizeLock_);
                auto& bucket = Buckets_[hash % Buckets_.size()];

                auto& lock = *bucket.Lock;
                while (lock.test_and_set(std::memory_order::acquire)) {
                    //while (lock.test(std::memory_order::relaxed)) {
                    //}
                }

                for (auto& item : bucket.Chain) {
                    if (item.first == key) {
                        value = &item.second;
                        break;
                    } 
                }

                if (!value) {
                    value = &bucket.Chain.emplace_front(key, T{}).second;
                    ++Size_;
                    loadFactor = Size_.load() * 1.0 / Buckets_.size();
                }

                lock.clear(std::memory_order::release);
            }

            if (loadFactor > MaxLoadFactor_) {
                std::unique_lock g(ResizeLock_);
                loadFactor = Size_.load() * 1.0 / Buckets_.size();
                if (loadFactor > MaxLoadFactor_) {
                    Resize(Buckets_.size() << 1);
                }
            }

            return *value;
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
                    const auto hash = Hash_(it->first);
                    auto& newBucket = newBuckets[hash % newBuckets.size()];
                    newBucket.Chain.splice(newBucket.Chain.begin(), oldChain, it);
                    it = next;
                }
            }
            for (auto& bucket : newBuckets) {
                bucket.Lock.reset(new std::atomic_flag());
            }

            Buckets_.swap(newBuckets);
        }

    private:
        const float MaxLoadFactor_ = 1.0;
        std::shared_mutex ResizeLock_;
        using TValueType = std::pair<TKey, T>;
        //struct TValueType {
        //    TKey Key{};
        //    T Value{};
        //};
        struct TBucket {
            std::unique_ptr<std::atomic_flag> Lock;
            std::list<TValueType> Chain;
        };
        std::vector<TBucket> Buckets_;
        std::atomic<size_t> Size_{0};
        Hash Hash_{};
    };

}