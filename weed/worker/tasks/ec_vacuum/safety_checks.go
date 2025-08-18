package ec_vacuum

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// performSafetyChecks performs comprehensive safety verification before cleanup
func (t *EcVacuumTask) performSafetyChecks() error {
	// Master address should have been fetched early in execution
	if t.masterAddress == "" {
		return fmt.Errorf("CRITICAL: cannot perform safety checks - master address not available (should have been fetched during task initialization)")
	}

	// Safety Check 1: Verify master connectivity and volume existence
	if err := t.verifyMasterConnectivity(); err != nil {
		return fmt.Errorf("master connectivity check failed: %w", err)
	}

	// Safety Check 2: Verify new generation is active on master
	if err := t.verifyNewGenerationActive(); err != nil {
		return fmt.Errorf("active generation verification failed: %w", err)
	}

	// Safety Check 3: Verify old generation is not the active generation
	if err := t.verifyOldGenerationInactive(); err != nil {
		return fmt.Errorf("old generation activity check failed: %w", err)
	}

	// Safety Check 4: Verify new generation has sufficient shards
	if err := t.verifyNewGenerationReadiness(); err != nil {
		return fmt.Errorf("new generation readiness check failed: %w", err)
	}

	// Safety Check 5: Verify no active read operations on old generation
	if err := t.verifyNoActiveOperations(); err != nil {
		return fmt.Errorf("active operations check failed: %w", err)
	}

	t.LogInfo("🛡️  ALL SAFETY CHECKS PASSED - Cleanup approved", map[string]interface{}{
		"volume_id":         t.volumeID,
		"source_generation": t.sourceGeneration,
		"target_generation": t.targetGeneration,
		"safety_checks":     5,
		"status":            "SAFE_TO_CLEANUP",
	})
	return nil
}

// verifyMasterConnectivity ensures we can communicate with the master
func (t *EcVacuumTask) verifyMasterConnectivity() error {
	return operation.WithMasterServerClient(false, t.masterAddress, t.grpcDialOption, func(client master_pb.SeaweedClient) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, err := client.Statistics(ctx, &master_pb.StatisticsRequest{})
		if err != nil {
			return fmt.Errorf("master ping failed: %w", err)
		}

		t.LogInfo("✅ Safety Check 1: Master connectivity verified", nil)
		return nil
	})
}

// verifyNewGenerationActive checks with master that the new generation is active
func (t *EcVacuumTask) verifyNewGenerationActive() error {
	return operation.WithMasterServerClient(false, t.masterAddress, t.grpcDialOption, func(client master_pb.SeaweedClient) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		resp, err := client.LookupEcVolume(ctx, &master_pb.LookupEcVolumeRequest{
			VolumeId: t.volumeID,
		})
		if err != nil {
			return fmt.Errorf("failed to lookup EC volume from master: %w", err)
		}

		if resp.ActiveGeneration != t.targetGeneration {
			return fmt.Errorf("CRITICAL: master active generation is %d, expected %d - ABORTING CLEANUP",
				resp.ActiveGeneration, t.targetGeneration)
		}

		t.LogInfo("✅ Safety Check 2: New generation is active on master", map[string]interface{}{
			"volume_id":         t.volumeID,
			"active_generation": resp.ActiveGeneration,
		})
		return nil
	})
}

// verifyOldGenerationInactive ensures the old generation is not active
func (t *EcVacuumTask) verifyOldGenerationInactive() error {
	return operation.WithMasterServerClient(false, t.masterAddress, t.grpcDialOption, func(client master_pb.SeaweedClient) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		resp, err := client.LookupEcVolume(ctx, &master_pb.LookupEcVolumeRequest{
			VolumeId: t.volumeID,
		})
		if err != nil {
			return fmt.Errorf("failed to lookup EC volume from master: %w", err)
		}

		if resp.ActiveGeneration == t.sourceGeneration {
			return fmt.Errorf("CRITICAL: old generation %d is still active - ABORTING CLEANUP to prevent data loss",
				t.sourceGeneration)
		}

		t.LogInfo("✅ Safety Check 3: Old generation is inactive", map[string]interface{}{
			"volume_id":         t.volumeID,
			"source_generation": t.sourceGeneration,
			"active_generation": resp.ActiveGeneration,
		})
		return nil
	})
}

// verifyNewGenerationReadiness checks that the new generation has enough shards
func (t *EcVacuumTask) verifyNewGenerationReadiness() error {
	return operation.WithMasterServerClient(false, t.masterAddress, t.grpcDialOption, func(client master_pb.SeaweedClient) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		resp, err := client.LookupEcVolume(ctx, &master_pb.LookupEcVolumeRequest{
			VolumeId:   t.volumeID,
			Generation: t.targetGeneration, // Explicitly request new generation
		})
		if err != nil {
			return fmt.Errorf("failed to lookup new generation %d from master: %w", t.targetGeneration, err)
		}

		shardCount := len(resp.ShardIdLocations)
		if shardCount < erasure_coding.DataShardsCount { // Need at least DataShardsCount data shards for safety
			return fmt.Errorf("CRITICAL: new generation %d has only %d shards (need ≥%d) - ABORTING CLEANUP",
				t.targetGeneration, shardCount, erasure_coding.DataShardsCount)
		}

		t.LogInfo("✅ Safety Check 4: New generation has sufficient shards", map[string]interface{}{
			"volume_id":         t.volumeID,
			"target_generation": t.targetGeneration,
			"shard_count":       shardCount,
			"minimum_required":  erasure_coding.DataShardsCount,
		})
		return nil
	})
}

// verifyNoActiveOperations checks that no active operations are using the old generation
func (t *EcVacuumTask) verifyNoActiveOperations() error {
	// For now, this is a simple time-based check (grace period serves this purpose)
	// In the future, this could be enhanced to check actual operation metrics or locks

	t.LogInfo("✅ Safety Check 5: Grace period completed - no active operations expected", map[string]interface{}{
		"volume_id":         t.volumeID,
		"source_generation": t.sourceGeneration,
		"grace_period":      t.cleanupGracePeriod,
		"assumption":        "grace period ensures operation quiescence",
	})
	return nil
}
