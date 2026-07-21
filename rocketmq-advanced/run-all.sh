#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "=== RocketMQ Advanced Demos ==="
echo

# 无需 Broker 的纯概念 Demo
echo ">> StorageModelDemo (no broker required)"
mvn -q exec:java -Dexec.mainClass="com.demo.storage.StorageModelDemo"
echo

if ! nc -z 127.0.0.1 9876 2>/dev/null; then
  echo "NameServer not detected on 127.0.0.1:9876"
  echo "Start RocketMQ first: docker compose up -d"
  echo "Skipping broker-dependent demos."
  exit 0
fi

mvn -q compile

demos=(
  "com.demo.basic.BasicProducerConsumerDemo"
  "com.demo.ordered.OrderedMessageDemo"
  "com.demo.delay.DelayMessageDemo"
  "com.demo.transaction.TransactionMessageDemo"
  "com.demo.filter.TagFilterDemo"
  "com.demo.retry.RetryAndDlqDemo"
)

for demo in "${demos[@]}"; do
  echo ">> $demo"
  mvn -q exec:java -Dexec.mainClass="$demo"
  echo
  sleep 2
done

echo "=== All demos finished ==="
