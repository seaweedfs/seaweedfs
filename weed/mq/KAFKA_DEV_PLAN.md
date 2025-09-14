## Kafka Client Compatibility for SeaweedFS Message Queue — Development Plan

### Goals
- **Kafka client support**: Allow standard Kafka clients (Java, sarama, kafka-go) to Produce/Fetch to SeaweedMQ.
- **Semantics**: At-least-once delivery, in-order per partition, consumer groups with committed offsets.
- **Performance**: Horizontal scalability via stateless gateways; efficient batching and IO.
- **Security (initial)**: TLS listener; SASL/PLAIN later.

### Non-goals (initial)
- Idempotent producers, transactions (EOS), log compaction semantics.
- Kafka’s broker replication factor (durability comes from SeaweedFS).

## Architecture Overview

### Kafka Gateway
- New stateless process that speaks the Kafka wire protocol and translates to SeaweedMQ.
- Listens on Kafka TCP port (e.g., 9092); communicates with SeaweedMQ brokers over gRPC.
- Persists lightweight control state (topic metadata, offset ledgers, group commits) in the filer.
- Multiple gateways can be deployed; any gateway can serve any client.

### Topic and Partition Mapping
- A Kafka topic’s partition count N is fixed at create-time for client compatibility.
- Map Kafka partitions to SMQ’s ring-based partitions by dividing the ring (size 4096) into N stable ranges.
- Message routing: `hash(key) -> kafka partition -> ring slot -> SMQ partition covering that slot`.
- SMQ’s internal segment split/merge remains transparent; ordering is preserved per Kafka partition.

### Offset Model (updated)
- Use SMQ native per-partition sequential offsets. Kafka offsets map 1:1 to SMQ offsets.
- Earliest/latest and timestamp-based lookups come from SMQ APIs; minimize translation.
- Consumer group commits store SMQ offsets directly.

### Consumer Groups and Assignment
- Gateway implements Kafka group coordinator: Join/Sync/Heartbeat/Leave.
- Assignment strategy starts with Range assignor; Sticky assignor later.
- Gateway uses SeaweedMQ subscriber APIs per assigned Kafka partition; stores group and commit state in filer.

### Protocol Coverage (initial)
- ApiVersions, Metadata, CreateTopics/DeleteTopics.
- Produce (v2+) with record-batch v2 parsing, compression, and CRC validation; Fetch (v2+) with wait/maxBytes semantics.
- ListOffsets (earliest/latest; timestamp in a later phase).
- FindCoordinator/JoinGroup/SyncGroup/Heartbeat/LeaveGroup.
- OffsetCommit/OffsetFetch.

### Security
- TLS for the Kafka listener (configurable cert/key/CA).
- SASL/PLAIN in a later phase, backed by SeaweedFS auth.

### Observability
- Prometheus metrics: per-topic/partition produce/fetch rates, latencies, rebalance counts, offset lag.
- Structured logs; optional tracing around broker RPC and ledger IO.

### Compatibility Limits (initial)
- No idempotent producers, transactions, or compaction policies.
- Compression codecs (GZIP/Snappy/LZ4/ZSTD) are available via the record-batch parser.

### Milestones
- **M1**: Gateway skeleton; ApiVersions/Metadata/Create/Delete; single-partition Produce/Fetch; plaintext; SMQ native offsets.
- **M2**: Multi-partition mapping, ListOffsets (earliest/latest), OffsetCommit/Fetch, group coordinator (Range), TLS.
- **M3**: Record-batch compression codecs + CRC; timestamp ListOffsets; Sticky assignor; SASL/PLAIN; metrics.
- **M4**: SCRAM, admin HTTP, ledger compaction tooling, performance tuning.
- **M5** (optional): Idempotent producers groundwork, EOS design exploration.

---

## Phase 1 (M1) — Detailed Plan

### Scope
- Kafka Gateway process scaffolding and configuration.
- Protocol: ApiVersions, Metadata, CreateTopics, DeleteTopics.
- Produce (single topic-partition path) and Fetch using v2 record-batch parser with compression and CRC.
- Basic filer-backed topic registry; offsets via SMQ native offsets (no separate ledger files).
- Plaintext only; no consumer groups yet (direct Fetch by offset).

### Deliverables
- New command: `weed mq.kafka.gateway` (or `weed mq.kafka`) to start the Kafka Gateway.
- Protocol handlers for ApiVersions/Metadata/CreateTopics/DeleteTopics/Produce/Fetch/ListOffsets (earliest/latest only).
- Filer layout for Kafka compatibility metadata and ledgers under:
  - `mq/kafka/<namespace>/<topic>/meta.json`
  - `mq/kafka/<namespace>/<topic>/partitions/<pid>/ledger.log`
  - `mq/kafka/<namespace>/<topic>/partitions/<pid>/ledger.index` (sparse; phase 2 fills)
- E2E tests using sarama and kafka-go for basic produce/fetch.

### Work Breakdown

1) Component Scaffolding
- Add command: `weed/command/mq_kafka_gateway.go` with flags:
  - `-listen=0.0.0.0:9092`, `-filer=`, `-master=`, `-namespace=default`.
  - (M1) TLS off; placeholder flags added but disabled.
- Service skeleton in `weed/mq/kafka/gateway/*` with lifecycle, readiness, and basic logging.

2) Protocol Layer
- Use `segmentio/kafka-go/protocol` for parsing/encoding.
- Implement request router and handlers for:
  - ApiVersions: advertise minimal supported versions.
  - Metadata: topics/partitions and leader endpoints (this gateway instance).
  - CreateTopics/DeleteTopics: validate, persist topic metadata in filer, create SMQ topic.
  - ListOffsets: earliest/latest using SMQ bounds.
  - Produce: parse v2 record batches (compressed/uncompressed), extract records, publish to SMQ; return baseOffset.
  - Fetch: read from SMQ starting at requested offset; construct proper v2 record batches honoring `maxBytes`/`maxWait`.

3) Topic Registry and Mapping
- Define `meta.json` schema:
  - `{ name, namespace, partitions, createdAtNs, configVersion }`.
- Map Kafka partition id to SMQ ring range: divide ring (4096) into `partitions` contiguous ranges.
- Enforce fixed partition count post-create.

4) Offset Handling (M1)
- Use SMQ native offsets; remove separate ledger and translation in the gateway.
- Earliest/Latest come from SMQ; timestamp lookups added in later phase.

5) Produce Path
- For each topic-partition in request:
  - Validate topic existence and partition id.
  - Parse record-batch v2; extract records (varints/headers), handle compression and CRC.
  - Publish to SMQ via broker (batch if available); SMQ assigns offsets; return `baseOffset` per partition.

6) Fetch Path (no groups)
- For each topic-partition in request:
  - If offset is `-1` (latest) or `-2` (earliest), use SMQ bounds.
  - Read from SMQ starting at the requested offset; construct proper v2 record batches.
  - Page results up to `maxBytes` or `minBytes`/`maxWait` semantics.

7) Metadata and SMQ Integration
- Create/delete topic maps to SMQ topic lifecycle using existing MQ APIs.
- No auto-scaling of partitions in M1 (Kafka partition count fixed).

8) Testing
- Unit tests for record-batch parser (compression, CRC), earliest/latest via SMQ.
- E2E:
  - sarama producer -> gateway -> SMQ; fetch and validate ordering/offsets.
  - kafka-go fetch from earliest/latest.
  - Metadata and create/delete topic via Kafka Admin client (happy path).

### Acceptance Criteria
- Can create a topic with N partitions via Kafka Admin client and see it in `meta.json`.
- Produce uncompressed records to a specific partition; responses carry correct baseOffset.
- Fetch by offset from earliest and latest returns correct records in order.
- Restart gateway: offsets and earliest/latest preserved; produce/fetch continue correctly.
- Basic concurrency: multiple producers to different partitions; correctness maintained.

### Open Questions / Follow-ups
- Exact `ApiVersions` and version ranges to advertise for maximal client compatibility.
- Whether to expose namespace as Kafka cluster or encode in topic names (`ns.topic`).
- Offset state compaction not applicable in gateway; defer SMQ-side retention considerations to later phases.


