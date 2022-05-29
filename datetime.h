#pragma once

#include "common.h"

#include <chrono>

namespace NJK {

    inline ui32 NowSeconds() {
        return std::chrono::system_clock::now().time_since_epoch().count();
    }

}