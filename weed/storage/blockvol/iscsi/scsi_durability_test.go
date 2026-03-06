package iscsi

import (
	"errors"
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/blockerr"
)

func TestMapBlockVolError_Durability_HardwareError(t *testing.T) {
	durErrs := []error{
		blockerr.ErrDurabilityBarrierFailed,
		blockerr.ErrDurabilityQuorumLost,
		fmt.Errorf("wrapped: %w", blockerr.ErrDurabilityBarrierFailed),
		fmt.Errorf("%w: 1 of 2 barriers failed", blockerr.ErrDurabilityBarrierFailed),
		fmt.Errorf("%w: 1 durable of 2 needed", blockerr.ErrDurabilityQuorumLost),
	}
	for _, err := range durErrs {
		result := mapBlockVolError(err)
		if result.SenseKey != SenseHardwareError {
			t.Errorf("mapBlockVolError(%v) SenseKey = %d, want %d (HARDWARE_ERROR)",
				err, result.SenseKey, SenseHardwareError)
		}
		if result.SenseASC != 0x08 {
			t.Errorf("mapBlockVolError(%v) ASC = 0x%02x, want 0x08",
				err, result.SenseASC)
		}
	}
}

func TestMapBlockVolError_NonDurability_MediumError(t *testing.T) {
	normalErrs := []error{
		errors.New("disk I/O error"),
		fmt.Errorf("write failed: %w", errors.New("EIO")),
	}
	for _, err := range normalErrs {
		result := mapBlockVolError(err)
		if result.SenseKey != SenseMediumError {
			t.Errorf("mapBlockVolError(%v) SenseKey = %d, want %d (MEDIUM_ERROR)",
				err, result.SenseKey, SenseMediumError)
		}
		if result.SenseASC != 0x0C {
			t.Errorf("mapBlockVolError(%v) ASC = 0x%02x, want 0x0C",
				err, result.SenseASC)
		}
	}
}

func TestMapBlockVolError_StringMatchNoLongerWorks(t *testing.T) {
	// Verify that string-matching is NOT used — a plain error with the
	// same text should NOT be detected as a durability error.
	fakeErr := errors.New("blockvol: sync_all durability barrier failed")
	result := mapBlockVolError(fakeErr)
	// This should be MEDIUM_ERROR because it's not the real sentinel.
	if result.SenseKey != SenseMediumError {
		t.Errorf("string-based error should NOT match as durability error: SenseKey=%d", result.SenseKey)
	}
}
