#!/bin/bash
# Test script to produce JSON messages and check timestamp field

# Produce 3 JSON messages
for i in 1 2 3; do
  TS=$(date +%s%N)
  echo "{\"id\":\"test-msg-$i\",\"timestamp\":$TS,\"producer_id\":999,\"counter\":$i,\"user_id\":\"user-test\",\"event_type\":\"test\"}"
done | docker run --rm -i --network kafka-client-loadtest \
  edenhill/kcat:1.7.1 \
  -P -b kafka-gateway:9093 -t test-json-topic

echo "Messages produced. Waiting 2 seconds for processing..."
sleep 2

echo "Querying messages..."
cd /Users/chrislu/go/src/github.com/seaweedfs/seaweedfs/test/kafka/kafka-client-loadtest
docker compose exec kafka-gateway /usr/local/bin/weed sql \
  -master=seaweedfs-master:9333 \
  -database=kafka \
  -query="SELECT id, timestamp, producer_id, counter, user_id, event_type FROM \"test-json-topic\" LIMIT 5;"

