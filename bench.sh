#! /usr/bin/env bash

set -x
set -e

NAME=$1

CMD="./a.out increment"

./make tsan
$CMD

./make
./a.out tests

cp a.out reports/${NAME}.bin
git diff > reports/${NAME}.git-diff

$CMD &> reports/${NAME}.out
$CMD &> reports/${NAME}.out.1

ya tool perf stat $CMD &> reports/${NAME}.stat
ya tool perf stat $CMD &> reports/${NAME}.stat.1

ya tool perf record $CMD
ya tool perf report --stdio &> reports/${NAME}.report-plain

ya tool perf record -g --call-graph=fp $CMD
ya tool perf report --stdio &> reports/${NAME}.report-tree
