#pragma once

#include <cstddef>
#include <cstdint>
#include <exception>
#include <stdexcept>
#include <memory>
#include <cstdlib>
#include <system_error>
#include <string.h>

namespace NJK {

    inline size_t operator "" _GiB(unsigned long long v) {
        return v << 30;
    }

    inline size_t operator "" _MiB(unsigned long long v) {
        return v << 20;
    }

    using ui32 = uint32_t;
    using ui16 = uint16_t;
    using ui8 = uint8_t;

    #define Y_ENSURE(cond) \
        if (!(cond)) { throw std::runtime_error(#cond); }

    #define Y_SYSCALL(expr) \
        if ((expr) == -1) { throw std::system_error(errno, std::generic_category(), std::string(#expr)); }


}