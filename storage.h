#pragma once

#include "volume_fwd.h"
#include <memory>
#include <variant>

namespace NJK {

    class TStorage {
    public:
        using TValue = TInodeValue; // TODO Copy + static_assert?

        // TODO mount info
        // And the most INTERESTING part: TMountTreeBuilder
        TStorage(std::initializer_list<std::string> volumes);
        ~TStorage();

        void Set(const std::string& path, const TValue& value);
        TValue Get(const std::string& path);

    private:
        class TImpl;
        std::unique_ptr<TImpl> Impl_;
    };

}