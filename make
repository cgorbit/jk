#! /usr/bin/env bash

clang++-12 -lpthread -g -std=c++20 -Werror -Wall *.cpp volume/*.cpp \
    -fsanitize=thread -fPIE -pie -g \
    -O2
