#pragma once

#include "common.h"
#include "stream.h"

#include <bit>
#include <vector>
#include <sstream>

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
}