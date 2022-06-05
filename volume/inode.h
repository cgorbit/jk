#pragma once

#include "../saveload.h"

namespace NJK::NVolume {

    struct TInode {
        using TId = ui32;

        // in-memory
        TId Id = 0;

        enum class EType: ui8 {
            Undefined,
            Bool,
            i32,
            Ui32,
            i64,
            Ui64,
            Float,
            Double,
            String,
            Blob,
        };

        // on-disk
        ui32 CreationTime{};
        ui32 ModTime{};
        struct {
            EType Type{};
            ui16 BlockCount = 0; // up to 256 MiB
            ui32 FirstBlockId = 0;
            ui32 Deadline = 0;
            // ui16 Version = 0; // TODO Generation
        } Val;
        struct {
            bool HasChildren = false;
            ui16 BlockCount = 0; // FIXME ui8 is enough
            ui32 FirstBlockId = 0;
        } Dir;
        char Data[38] = {0};

        static constexpr ui32 ToSkip = 0;

        Y_DECLARE_SERIALIZATION

        static constexpr ui32 OnDiskSize = 64;
        static_assert(512 % OnDiskSize == 0);
    };

}
