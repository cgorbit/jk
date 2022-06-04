#pragma once

#include "volume.h"
#include <memory>
#include <variant>

namespace NJK {

    class TStorage {
    public:
        using TValue = TInodeValue; // TODO Copy + static_assert?

        // TODO mount info
        // And the most INTERESTING part: TMountTreeBuilder
        explicit TStorage(TVolume* root, const std::string& dir = "/");
        ~TStorage();

        void Mount(const std::string& mountPoint, TVolume* src, const std::string& srcDir = "/");

        void Set(const std::string& path, const TValue& value, ui32 deadline = 0);
        TValue Get(const std::string& path);

    private:
        class TImpl;
        std::unique_ptr<TImpl> Impl_;
    };

}