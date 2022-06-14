#! /usr/bin/env bash

set -e

GOOGLE_BENCHMARK=/home/trofimenkov/benchmark

OPTIMIZE_LEVEL=2

if [ "$1" = 'tsan' ]; then
    THREAD_SANITIZER="-fsanitize=thread -fPIE -pie -g"
else
    THREAD_SANITIZER=
fi

FRAME_POINTER=
FRAME_POINTER=-fno-omit-frame-pointer

set -x

clang++-12 -lpthread -g -std=c++20 -Werror -Wall *.cpp volume/*.cpp \
    -O$OPTIMIZE_LEVEL \
    -I$GOOGLE_BENCHMARK/include \
    -I$GOOGLE_BENCHMARK/build/include \
    -L$GOOGLE_BENCHMARK/build/src -lbenchmark \
    $THREAD_SANITIZER \
    $FRAME_POINTER
