#pragma once

#include <cstddef>
#include <cstdint>
#include <exception>
#include <cassert>
#include <stdexcept>
#include <memory>
#include <cstdlib>
#include <system_error>
#include <string.h>
#include <sstream>

namespace NJK {

    inline size_t operator "" _GiB(unsigned long long v) {
        return v << 30;
    }

    inline size_t operator "" _MiB(unsigned long long v) {
        return v << 20;
    }

    using i16 = int16_t;
    using i32 = int32_t;

    using ui32 = uint32_t;
    using ui16 = uint16_t;
    using ui8 = uint8_t;

    #define Y_ENSURE(cond) \
        if (!(cond)) { throw std::runtime_error(#cond); }

    #define Y_SYSCALL(expr) \
        if ((expr) == -1) { throw std::system_error(errno, std::generic_category(), std::string(#expr)); }

    #define CONCAT_INNER(a, b) a ## b
    #define CONCAT(a, b) CONCAT_INNER(a, b)

    class TStringBuilder {
    public:
        template <typename T>
        TStringBuilder& operator<< (T&& val) {
            Impl_ << val;
            return *this;
        }

        //operator std::string () const {
        std::string Str() const {
            return Impl_.str();
        }

    private:
        std::stringstream Impl_;
    };

    #define Y_FAIL(msg) throw std::runtime_error((TStringBuilder() << __FILE__ << ':' << __LINE__ << ": " << (msg)).Str())
    #define Y_TODO(msg) Y_FAIL("TODO " msg)
    #define Y_NOT_IMPLEMENTED(msg) Y_FAIL("TODO " msg)

    #define Y_ASSERT(cond) assert(cond)

    #define Y_VERIFY(cond) if (!(cond)) { std::cerr << #cond << '\n'; std::abort(); }

    #define Y_UNREACHABLE Y_VERIFY(false)

    #define TODO_BETTER_CONCURRENCY
    #define TODO_PERFORMANCE
    #define TODO(msg)
    #define FIXME(msg)

    namespace NPrivate {
        template <typename F>
        class TDeferCall {
        public:
            TDeferCall(F&& f)
                : Func_(std::forward<F>(f))
            {
            }

            ~TDeferCall() {
                Func_();
            }

        private:
            F Func_;
        };
    }

    #define Y_DEFER(f) const NPrivate::TDeferCall CONCAT(defer, __LINE__)(f)
}