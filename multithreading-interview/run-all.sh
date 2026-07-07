#!/usr/bin/env bash
# 编译并运行全部多线程 Demo。需要 JDK 11+（推荐 17/21）。
set -euo pipefail

cd "$(dirname "$0")"

OUT="out"
rm -rf "$OUT"
mkdir -p "$OUT"

echo "==> 编译所有 Java 源文件到 $OUT/"
find src -name "*.java" -print0 | xargs -0 javac -d "$OUT"

run() {
  echo
  echo "======================================================================"
  echo "运行: $1"
  echo "======================================================================"
  java -cp "$OUT" "$1"
}

run forkjoin.ForkJoinSumDemo
run forkjoin.ForkJoinMergeSortDemo
run forkjoin.ForkJoinWordCountDemo
run threadpool.ThreadPoolDemo
run lock.DeadlockDemo          # 安全版本，不加参数
run cas.CasCounterDemo
run future.CompletableFutureDemo
run queue.ProducerConsumerDemo

echo
echo "==> 全部 Demo 运行完毕。"
