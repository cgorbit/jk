#pragma once

#include "common.h"
#include "stream.h"

#include <bit>
#include <vector>

namespace NJK {

    inline void SwapBytes(ui32& v) {
        std::byte* buf = (std::byte*)&v;
        std::swap(buf[0], buf[3]);
        std::swap(buf[1], buf[2]);
    }

    inline void SwapBytes(ui16& v) {
        std::byte* buf = (std::byte*)&v;
        std::swap(buf[0], buf[1]);
    }

    template <typename T>
    constexpr bool is_multibyte_integral_v = sizeof(T) != 1 && std::is_integral_v<T>;

    template <typename T, std::enable_if_t<is_multibyte_integral_v<T>, bool> = true>
    void Serialize(IOutputStream& out, T v) {
        if constexpr (std::endian::native != std::endian::little) {
            SwapBytes(v);
        }
        out.Save(reinterpret_cast<const char*>(&v), sizeof(v));
    }

    template <typename T, std::enable_if_t<is_multibyte_integral_v<T>, bool> = true>
    void Deserialize(IInputStream& in, T& v) {
        in.Load(reinterpret_cast<char*>(&v), sizeof(v));
        if constexpr (std::endian::native != std::endian::little) {
            SwapBytes(v);
        }
    }

    template <typename T>
    struct TDeserializeFixedVector;

    template <typename T>
    struct TDeserializeFixedVector<std::vector<T>> {
        void operator() (IInputStream& in, std::vector<T>& dst) {
            for (auto& i : dst) {
                i.Deserialize(in);
            }
        }
    };

    template <typename T>
    void DeserializeFixedVector(IInputStream& in, T& dst) {
        TDeserializeFixedVector<T>()(in, dst);
    }

    template <typename T>
    struct TSerializeFixedVector;

    template <typename T>
    struct TSerializeFixedVector<std::vector<T>> {
        void operator() (IOutputStream& out, std::vector<T>& dst) {
            for (auto& i : dst) {
                i.Serialize(out);
            }
        }
    };

    template <typename T>
    void SerializeFixedVector(IOutputStream& out, T& dst) {
        TSerializeFixedVector<T>()(out, dst);
    }
}