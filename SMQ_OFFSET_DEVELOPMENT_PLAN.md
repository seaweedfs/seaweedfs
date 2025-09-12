# SMQ Native Offset Development Plan

## Overview

Add native per-partition sequential offsets to SeaweedMQ to eliminate the need for external offset mapping and provide better interoperability with message queue protocols.

## Architecture Changes

### Data Model
- Add `offset` field (int64) to each record alongside existing `ts_ns`
- Offset domain: per `schema_pb.Partition` (ring range)
- Offsets are strictly monotonic within a partition
- Leader assigns offsets; followers replicate

### Storage
- Use `_index` as hidden SQL table column for offset storage
- Maintain per-partition offset counters in broker state
- Checkpoint offset state periodically for recovery

## Development Phases

### Phase 1: Proto and Data Model Changes

**Scope**: Update protobuf definitions and core data structures

**Tasks**:
1. Update `mq_schema.proto`:
   - Add `offset` field to record storage format
   - Add offset-based `OffsetType` enums
   - Add `offset_value` field to subscription requests
2. Update `mq_agent.proto`:
   - Add `base_offset` and `last_offset` to `PublishRecordResponse`
   - Add `offset` field to `SubscribeRecordResponse`
3. Regenerate protobuf Go code
4. Update core data structures in broker code
5. Add offset field to SQL schema with `_index` column

**Tests**:
- Proto compilation tests
- Data structure serialization tests
- SQL schema migration tests

**Deliverables**:
- Updated proto files
- Generated Go code
- Updated SQL schema
- Basic unit tests

### Phase 2: Offset Assignment Logic

**Scope**: Implement offset assignment in broker

**Tasks**:
1. Add `PartitionOffsetManager` component:
   - Track `next_offset` per partition
   - Assign sequential offsets to records
   - Handle offset recovery on startup
2. Integrate with existing record publishing flow:
   - Assign offsets before storage
   - Update `PublishRecordResponse` with offset info
3. Add offset persistence to storage layer:
   - Store offset alongside record data
   - Index by offset for efficient lookups
4. Implement offset recovery:
   - Load highest offset on partition leadership
   - Handle clean and unclean restarts

**Tests**:
- Offset assignment unit tests
- Offset persistence tests
- Recovery scenario tests
- Concurrent assignment tests

**Deliverables**:
- `PartitionOffsetManager` implementation
- Integrated publishing with offsets
- Offset recovery logic
- Comprehensive test suite

### Phase 3: Subscription by Offset

**Scope**: Enable consumers to subscribe using offsets

**Tasks**:
1. Extend subscription logic:
   - Support `EXACT_OFFSET` and `RESET_TO_OFFSET` modes
   - Add offset-based seeking
   - Maintain backward compatibility with timestamp-based seeks
2. Update `SubscribeRecordResponse`:
   - Include offset in response messages
   - Ensure offset ordering in delivery
3. Add offset validation:
   - Validate requested offsets are within valid range
   - Handle out-of-range offset requests gracefully
4. Implement offset-based filtering and pagination

**Tests**:
- Offset-based subscription tests
- Seek functionality tests
- Out-of-range offset handling tests
- Mixed timestamp/offset subscription tests

**Deliverables**:
- Offset-based subscription implementation
- Updated subscription APIs
- Validation and error handling
- Integration tests

### Phase 4: High Water Mark and Lag Calculation

**Scope**: Implement native offset-based metrics

**Tasks**:
1. Add high water mark tracking:
   - Track highest committed offset per partition
   - Expose via broker APIs
   - Update on successful replication
2. Implement lag calculation:
   - Consumer lag = high_water_mark - consumer_offset
   - Partition lag metrics
   - Consumer group lag aggregation
3. Add offset-based monitoring:
   - Partition offset metrics
   - Consumer position tracking
   - Lag alerting capabilities
4. Update existing monitoring integration

**Tests**:
- High water mark calculation tests
- Lag computation tests
- Monitoring integration tests
- Metrics accuracy tests

**Deliverables**:
- High water mark implementation
- Lag calculation logic
- Monitoring integration
- Metrics and alerting

### Phase 5: Kafka Gateway Integration

**Scope**: Update Kafka gateway to use native SMQ offsets

**Tasks**:
1. Remove offset mapping layer:
   - Delete `kafka-system/offset-mappings` topic usage
   - Remove `PersistentLedger` and `SeaweedMQStorage`
   - Simplify offset translation logic
2. Update Kafka protocol handlers:
   - Use native SMQ offsets in Produce responses
   - Map SMQ offsets directly to Kafka offsets
   - Update ListOffsets and Fetch handlers
3. Simplify consumer group offset management:
   - Store Kafka consumer offsets as SMQ offsets
   - Remove timestamp-based offset translation
4. Update integration tests:
   - Test Kafka client compatibility
   - Verify offset consistency
   - Test long-term disconnection scenarios

**Tests**:
- Kafka protocol compatibility tests
- End-to-end integration tests
- Performance comparison tests
- Migration scenario tests

**Deliverables**:
- Simplified Kafka gateway
- Removed offset mapping complexity
- Updated integration tests
- Performance improvements

### Phase 6: Performance Optimization and Production Readiness

**Scope**: Optimize performance and prepare for production

**Tasks**:
1. Optimize offset assignment performance:
   - Batch offset assignment
   - Reduce lock contention
   - Optimize recovery performance
2. Add offset compaction and cleanup:
   - Implement offset-based log compaction
   - Add retention policies based on offsets
   - Cleanup old offset checkpoints
3. Enhance monitoring and observability:
   - Detailed offset metrics
   - Performance dashboards
   - Alerting on offset anomalies
4. Load testing and benchmarking:
   - Compare performance with timestamp-only approach
   - Test under high load scenarios
   - Validate memory usage patterns

**Tests**:
- Performance benchmarks
- Load testing scenarios
- Memory usage tests
- Stress testing under failures

**Deliverables**:
- Optimized offset implementation
- Production monitoring
- Performance benchmarks
- Production deployment guide

## Implementation Guidelines

### Code Organization
```
weed/mq/
├── offset/
│   ├── manager.go          # PartitionOffsetManager
│   ├── recovery.go         # Offset recovery logic
│   └── checkpoint.go       # Offset checkpointing
├── broker/
│   ├── partition_leader.go # Updated with offset assignment
│   └── subscriber.go       # Updated with offset support
└── storage/
    └── offset_store.go     # Offset persistence layer
```

### Testing Strategy
- Unit tests for each component
- Integration tests for cross-component interactions
- Performance tests for offset assignment and recovery
- Compatibility tests with existing SMQ features
- End-to-end tests with Kafka gateway

### Commit Strategy
- One commit per completed task within a phase
- All tests must pass before commit
- No binary files in commits
- Clear commit messages describing changes

### Rollout Plan
1. Deploy to development environment after Phase 2
2. Integration testing after Phase 3
3. Performance testing after Phase 4
4. Kafka gateway migration after Phase 5
5. Production rollout after Phase 6

## Success Criteria

### Phase Completion Criteria
- All tests pass
- Code review completed
- Documentation updated
- Performance benchmarks meet targets

### Overall Success Metrics
- Eliminate external offset mapping complexity
- Maintain or improve performance
- Full Kafka protocol compatibility
- Native SMQ offset support for all protocols
- Simplified consumer group offset management

## Risk Mitigation

### Technical Risks
- **Offset assignment bottlenecks**: Implement batching and optimize locking
- **Recovery performance**: Use checkpointing and incremental recovery
- **Storage overhead**: Optimize offset storage and indexing

### Operational Risks
- **Migration complexity**: Implement gradual rollout with rollback capability
- **Data consistency**: Extensive testing of offset assignment and recovery
- **Performance regression**: Continuous benchmarking and monitoring

## Timeline Estimate

- Phase 1: 1-2 weeks
- Phase 2: 2-3 weeks  
- Phase 3: 2-3 weeks
- Phase 4: 1-2 weeks
- Phase 5: 2-3 weeks
- Phase 6: 2-3 weeks

**Total: 10-16 weeks**

## Next Steps

1. Review and approve development plan
2. Set up development branch
3. Begin Phase 1 implementation
4. Establish testing and CI pipeline
5. Regular progress reviews and adjustments
