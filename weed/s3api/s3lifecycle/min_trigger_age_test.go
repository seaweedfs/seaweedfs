package s3lifecycle

import (
	"testing"
	"time"
)

func TestMinTriggerAge_AgeBased(t *testing.T) {
	got := MinTriggerAge(&Rule{ExpirationDays: 7})
	if got != 7*24*time.Hour {
		t.Fatalf("want 7d, got %v", got)
	}
}

func TestMinTriggerAge_TakesMinAcrossActions(t *testing.T) {
	rule := &Rule{
		ExpirationDays:                  30,
		NoncurrentVersionExpirationDays: 7, // smaller wins
		AbortMPUDaysAfterInitiation:     14,
	}
	if got := MinTriggerAge(rule); got != 7*24*time.Hour {
		t.Fatalf("want 7d, got %v", got)
	}
}

func TestMinTriggerAge_DateOnlyReturnsZero(t *testing.T) {
	rule := &Rule{ExpirationDate: mustTime(t, "2025-06-15T00:00:00Z")}
	if got := MinTriggerAge(rule); got != 0 {
		t.Fatalf("date-only should return 0, got %v", got)
	}
}

func TestMinTriggerAge_CountOnlyReturnsZero(t *testing.T) {
	rule := &Rule{NewerNoncurrentVersions: 3}
	if got := MinTriggerAge(rule); got != 0 {
		t.Fatalf("count-only should return 0, got %v", got)
	}
}

func TestMinTriggerAge_ExpiredDeleteMarkerOnlyReturnsZero(t *testing.T) {
	rule := &Rule{ExpiredObjectDeleteMarker: true}
	if got := MinTriggerAge(rule); got != 0 {
		t.Fatalf("delete-marker-only should return 0, got %v", got)
	}
}

func TestMinTriggerAge_NilRule(t *testing.T) {
	if got := MinTriggerAge(nil); got != 0 {
		t.Fatalf("nil should return 0, got %v", got)
	}
}
