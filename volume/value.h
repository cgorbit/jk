#pragma once

#include "../common.h"
#include <variant>

namespace NJK::NVolume {

    class TBlobView {
    public:
        TODO("Not fully supported")

        TBlobView() {
        }

        TBlobView(const std::string& s)
            : Ptr_(s.data())
            , Size_(s.size())
        {
        }

        TBlobView(const char* ptr, size_t size)
            : Ptr_(ptr)
            , Size_(size)
        {
        }

        size_t Size() const {
            return Size_;
        }

        const char* Data() const {
            return Ptr_;
        }

    private:
        const char* Ptr_{};
        size_t Size_ = 0;
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