#pragma once

#include "common.h"
#include <variant>

namespace NJK {

    struct TBlobView {
        TBlobView() {
        }

        TBlobView(const std::string& s)
            : Ptr(s.data())
            , Size(s.size())
        {
        }

        TBlobView(const char* ptr, size_t size)
            : Ptr(ptr)
            , Size(size)
        {
        }

        const char* Ptr{};
        size_t Size = 0;
    };

    using TInodeValue = std::variant<
        std::monostate,
        bool,
        i32,
        ui32,
        int64_t,
        uint64_t,
        float,
        double,
        std::string,
        TBlobView
    >;

}