# Erasure Coding Integration Tests

This directory contains integration tests for the EC (Erasure Coding) encoding volume location timing bug fix.

## The Bug

The bug caused **double storage usage** during EC encoding because:

1. **Silent failure**: Functions returned `nil` instead of proper error messages
2. **Timing race condition**: Volume locations were collected **AFTER** EC encoding when master metadata was already updated
3. **Missing cleanup**: Original volumes weren't being deleted after EC encoding

This resulted in both original `.dat` files AND EC `.ec00-.ec13` files coexisting, effectively **doubling storage usage**.

## The Fix

The fix addresses all three issues:

1. **Fixed silent failures**: Updated `doDeleteVolumes()` and `doEcEncode()` to return proper errors
2. **Fixed timing race condition**: Created `doDeleteVolumesWithLocations()` that uses pre-collected volume locations
3. **Enhanced cleanup**: Volume locations are now collected **BEFORE** EC encoding, preventing the race condition

## Integration Tests

### TestECEncodingVolumeLocationTimingBug
The main integration test that:
- **Simulates master timing race condition**: Tests what happens when volume locations are read from master AFTER EC encoding has updated the metadata
- **Verifies fix effectiveness**: Checks for the "Collecting volume locations...before EC encoding" message that proves the fix is working
- **Tests multi-server distribution**: Runs EC encoding with 6 volume servers to test shard distribution
- **Validates cleanup**: Ensures original volumes are properly cleaned up after EC encoding

### TestECEncodingMasterTimingRaceCondition
A focused test that specifically targets the **master metadata timing race condition**:
- **Simulates the exact race condition**: Tests volume location collection timing relative to master metadata updates
- **Detects timing fix**: Verifies that volume locations are collected BEFORE EC encoding starts
- **Demonstrates bug impact**: Shows what happens when volume locations are unavailable after master metadata update

### TestECEncodingRegressionPrevention
Regression tests that ensure:
- **Function signatures**: Fixed functions still exist and return proper errors
- **Timing patterns**: Volume location collection happens in the correct order

## Test Architecture

The tests use:
- **Real SeaweedFS cluster**: 1 master server + 6 volume servers
- **Multi-server setup**: Tests realistic EC shard distribution across multiple servers
- **Timing simulation**: Goroutines and delays to simulate race conditions
- **Output validation**: Checks for specific log messages that prove the fix is working

## Why Integration Tests Were Necessary

Unit tests could not catch this bug because:
1. **Race condition**: The bug only occurred in real-world timing scenarios
2. **Master-volume server interaction**: Required actual master metadata updates
3. **File system operations**: Needed real volume creation and EC shard generation
4. **Cleanup timing**: Required testing the sequence of operations in correct order

The integration tests successfully catch the timing bug by:
- **Testing real command execution**: Uses actual `ec.encode` shell command
- **Simulating race conditions**: Creates timing scenarios that expose the bug
- **Validating output messages**: Checks for the key "Collecting volume locations...before EC encoding" message
- **Monitoring cleanup behavior**: Ensures original volumes are properly deleted

## Running the Tests

```bash
# Run all integration tests
go test -v

# Run only the main timing test
go test -v -run TestECEncodingVolumeLocationTimingBug

# Run only the race condition test
go test -v -run TestECEncodingMasterTimingRaceCondition

# Skip integration tests (short mode)
go test -v -short
```

## Manual Testing with Makefile

A Makefile is provided for manual EC testing.

**Requirements:** `curl`, `jq` (command-line JSON processor)

```bash
# Quick start: start cluster and populate data
make setup

# Open weed shell to run EC commands
make shell

# Individual targets
make start      # Start test cluster (master + 6 volume servers + filer)
make stop       # Stop test cluster
make populate   # Populate ~300MB of test data
make status     # Show cluster and EC shard status
make clean      # Stop cluster and remove all test data
make help       # Show all targets
```

### EC Rebalance Limited Slots (Unit Test)

The "no free ec shard slots" issue is tested with a **unit test** that works directly on
topology data structures without requiring a running cluster.

**Location**: `weed/shell/ec_rebalance_slots_test.go`

Tests included:
- `TestECRebalanceWithLimitedSlots`: Tests a topology with 6 servers, 7 EC volumes (98 shards)
- `TestECRebalanceZeroFreeSlots`: Reproduces the exact 0 free slots scenario

**Known Issue**: When volume servers are at capacity (`volumeCount == maxVolumeCount`),
the rebalance step fails with "no free ec shard slots" instead of recognizing that
moving shards frees slots on source servers.

## Test Results

**With the fix**: Shows "Collecting volume locations for N volumes before EC encoding..." message
**Without the fix**: No collection message, potential timing race condition

The tests demonstrate that the fix prevents the volume location timing bug that caused double storage usage in EC encoding operations. 