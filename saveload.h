#pragma once

#include "common.h"
#include "stream.h"

#include <bit>
#include <vector>
#include <sstream>

namespace NJK {

    // TODO Optimize swaps

    inline void SwapBytes(ui32& v) {
        std::byte* buf = (std::byte*)&v;
        std::swap(buf[0], buf[3]);
        std::swap(buf[1], buf[2]);
    }

    inline void SwapBytes(uint64_t& v) {
        std::byte* buf = (std::byte*)&v;
        std::swap(buf[0], buf[7]);
        std::swap(buf[1], buf[6]);
        std::swap(buf[2], buf[5]);
        std::swap(buf[3], buf[4]);
    }
    inline void SwapBytes(ui16& v) {
        std::byte* buf = (std::byte*)&v;
        std::swap(buf[0], buf[1]);
    }

    inline void SwapBytes(float& v) {
        static_assert(sizeof(v) == 4);
        SwapBytes(reinterpret_cast<ui32&>(v));
    }

    inline void SwapBytes(double& v) {
        static_assert(sizeof(v) == 8);
        SwapBytes(reinterpret_cast<uint64_t&>(v));
    }

    template <typename T, std::enable_if_t<std::is_same_v<T, ui8>, bool> = true>
    inline void Deserialize(IInputStream& in, ui8& v) {
        in.Load((char*)&v, 1);
    }
    inline void Deserialize(IInputStream& in, ui8& v) {
        in.Load((char*)&v, 1);
    }

    template <typename T, std::enable_if_t<std::is_same_v<T, ui8>, bool> = true>
    inline void Serialize(IOutputStream& out, const ui8& v) {
        out.Save((const char*)&v, 1);
    }
    inline void Serialize(IOutputStream& out, const ui8& v) {
        out.Save((const char*)&v, 1);
    }

    inline void Deserialize(IInputStream& in, bool& v) {
        char c{};
        in.Load(&c, 1);
        v = c == 1;
    }
    inline void Serialize(IOutputStream& out, bool v) {
        const char c = v ? 1 : 0;
        out.Save(&c, 1);
    }

    template <size_t N>
    inline void Serialize(IOutputStream& out, const char (&arr)[N]) {
        out.Save(arr, N);
    }

    template <size_t N>
    inline void Deserialize(IInputStream& in, char (&arr)[N]) {
        in.Load(arr, N);
    }

    template <typename T>
    constexpr bool is_multibyte_integral_v = sizeof(T) != 1 && (std::is_integral_v<T> || std::is_same_v<T, float> || std::is_same_v<T, double>);

    template <typename T, std::enable_if_t<is_multibyte_integral_v<T>, bool> = true>
    void Serialize(IOutputStream& out, T v) {
        if constexpr (std::endian::native != std::endian::little) {
            SwapBytes(v);
        }
        out.Save(reinterpret_cast<const char*>(&v), sizeof(v));
    }

    template <typename T, std::enable_if_t<std::is_enum_v<T>, bool> = true>
    void Serialize(IOutputStream& out, T v) {
        using U = std::underlying_type_t<T>;
        Serialize<U>(out, reinterpret_cast<U&>(v));
    }

    template <typename T, std::enable_if_t<is_multibyte_integral_v<T>, bool> = true>
    void Deserialize(IInputStream& in, T& v) {
        in.Load(reinterpret_cast<char*>(&v), sizeof(v));
        if constexpr (std::endian::native != std::endian::little) {
            SwapBytes(v);
        }
    }

    template <typename T, std::enable_if_t<std::is_enum_v<T>, bool> = true>
    void Deserialize(IInputStream& out, T& v) {
        using U = std::underlying_type_t<T>;
        Deserialize<U>(out, reinterpret_cast<U&>(v));
    }

    struct TSkipMe {
        size_t Count = 0;
    };

    inline void Deserialize(IInputStream& out, const TSkipMe& v) {
        out.SkipRead(v.Count);
    }

    inline void Serialize(IOutputStream& out, const TSkipMe& v) {
        out.SkipWrite(v.Count);
    }

    inline void DeserializeMany(IInputStream&) {
    }

    template <typename T, typename ...Args>
    void DeserializeMany(IInputStream& in, T&& v, Args&& ...args) {
        Deserialize(in, std::forward<T>(v));
        DeserializeMany(in, std::forward<Args>(args)...);
    }

    inline void SerializeMany(IOutputStream&) {
    }

    template <typename T, typename ...Args>
    void SerializeMany(IOutputStream& out, T&& v, Args&& ...args) {
        Serialize(out, std::forward<T>(v));
        SerializeMany(out, std::forward<Args>(args)...);
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

    class TCheckedInputStream: public IInputStream {
    public:
        TCheckedInputStream(IInputStream& backend)
            : Backend_(&backend)
        {
        }

        size_t Read(char* buf, size_t count) override {
            auto ret = Backend_->Read(buf, count);
            BytesRead_ += ret;
            return ret;
        }

        void SkipRead(size_t count) override {
            Backend_->SkipRead(count);
            BytesRead_ += count;
        }

        size_t BytesRead() const {
            return BytesRead_;
        }

    private:
        IInputStream* Backend_{};
        size_t BytesRead_ = 0;
    };

    class TCheckedOutputStream: public IOutputStream {
    public:
        TCheckedOutputStream(IOutputStream& backend)
            : Backend_(&backend)
        {
        }

        size_t Write(const char* buf, size_t count) override {
            auto ret = Backend_->Write(buf, count);
            BytesWritten_ += ret;
            return ret;
        }

        void SkipWrite(size_t count) override {
            Backend_->SkipWrite(count);
            BytesWritten_ += count;
        }

        size_t BytesWritten() const {
            return BytesWritten_;
        }

    private:
        IOutputStream* Backend_{};
        size_t BytesWritten_ = 0;
    };

    template <typename T>
    void SerializeChecked(IOutputStream& out, const T& obj) {
        #ifdef NDEBUG
            obj.Serialize(out);
        #else
            TCheckedOutputStream proxy(out);
            obj.Serialize(proxy);
            if (T::OnDiskSize != proxy.BytesWritten()) {
                std::stringstream s;
                s << "Serialize: wrong number of bytes written for "
                    << typeid(T).name() << ", expect " << T::OnDiskSize
                    << ", but got " << proxy.BytesWritten();
                throw std::runtime_error(s.str());
            }
        #endif
    }

    template <typename T>
    void DeserializeChecked(IInputStream& out, T& obj) {
        #ifdef NDEBUG
            obj.Deserialize(out);
        #else
            TCheckedInputStream proxy(out);
            obj.Deserialize(proxy);
            if (T::OnDiskSize != proxy.BytesRead()) {
                std::stringstream s;
                s << "Deserialize: wrong number of bytes read for "
                    << typeid(T).name() << ", expect " << T::OnDiskSize
                    << ", but got " << proxy.BytesRead();
                throw std::runtime_error(s.str());
            }
        #endif
    }

    #define Y_DECLARE_SERIALIZATION \
        void Serialize(IOutputStream& out) const; \
        void Deserialize(IInputStream& in);

    #define Y_DEFINE_SERIALIZATION(type, ...) \
        void type::Serialize(IOutputStream& out) const { \
            SerializeMany(out, __VA_ARGS__); \
        } \
\
        void type::Deserialize(IInputStream& in) { \
            DeserializeMany(in, __VA_ARGS__); \
        }

}