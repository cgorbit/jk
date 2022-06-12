#pragma once

#include "volume.h"
#include "volume/value.h"
#include <memory>
#include <variant>

namespace NJK {

    class TStorage {
    public:
        using TValue = NVolume::TInodeValue; // TODO Copy + static_assert?

        friend class TStorageBuilder;
        ~TStorage();

        TStorage(TStorage&&) noexcept;
        TStorage& operator= (TStorage&&) noexcept;

        static TStorage Build(TVolume* root, const std::string& dir = "/");

        void Set(const std::string& path, const TValue& value, ui32 deadline = 0);
        TValue Get(const std::string& path);
        void Erase(const std::string& path);

    private:
        explicit TStorage(TVolume* root, const std::string& dir = "/");
        void Mount(const std::string& mountPoint, TVolume* src, const std::string& srcDir = "/");

    private:
        class TImpl;
        std::unique_ptr<TImpl> Impl_;
    };

    class TStorageBuilder {
    public:
        explicit TStorageBuilder(TVolume* root, const std::string& dir = "/")
            : Storage_(root, dir)
        {
        }

        TStorageBuilder& Mount(const std::string& mountPoint, TVolume* src, const std::string& srcDir = "/") {
            Storage_.Mount(mountPoint, src, srcDir);
            return *this;
        }

        TStorage Build() {
            return std::move(Storage_);
        }

    private:
        TStorage Storage_;
    };

    inline TStorage TStorage::Build(TVolume* root, const std::string& dir) {
        return TStorageBuilder(root, dir).Build();
    }

}