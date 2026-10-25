package s3lifecycle

import (
	"testing"
)

func TestComputeDueAt_ExpirationDays(t *testing.T) {
	rule := &Rule{Status: StatusEnabled, ExpirationDays: 30}
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	info := &ObjectInfo{Key: "a", IsLatest: true, ModTime: mod}
	want := mod.Add(DaysToDuration(30))
	if got := ComputeDueAt(rule, ActionKindExpirationDays, info); !got.Equal(want) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func TestComputeDueAt_ExpirationDate(t *testing.T) {
	date := mustTime(t, "2025-06-15T00:00:00Z")
	rule := &Rule{Status: StatusEnabled, ExpirationDate: date}
	info := &ObjectInfo{Key: "a", IsLatest: true, ModTime: mustTime(t, "2024-01-01T00:00:00Z")}
	if got := ComputeDueAt(rule, ActionKindExpirationDate, info); !got.Equal(date) {
		t.Fatalf("want %v, got %v", date, got)
	}
}

func TestComputeDueAt_DeleteMarker_NotSoleSurvivor(t *testing.T) {
	rule := &Rule{Status: StatusEnabled, ExpiredObjectDeleteMarker: true}
	info := &ObjectInfo{Key: "a", IsLatest: true, IsDeleteMarker: true, NumVersions: 3}
	if got := ComputeDueAt(rule, ActionKindExpiredDeleteMarker, info); !got.IsZero() {
		t.Fatalf("non-sole marker should be zero, got %v", got)
	}
}

func TestComputeDueAt_KindNotDeclared(t *testing.T) {
	// Asking for a kind the rule doesn't declare returns zero.
	rule := &Rule{Status: StatusEnabled, ExpirationDays: 1}
	info := &ObjectInfo{Key: "a", IsLatest: true, ModTime: mustTime(t, "2024-01-01T00:00:00Z")}
	if got := ComputeDueAt(rule, ActionKindAbortMPU, info); !got.IsZero() {
		t.Fatalf("undeclared kind should be zero, got %v", got)
	}
}

func TestComputeDueAt_WrongShapeForKind(t *testing.T) {
	// Non-current object asked under EXPIRATION_DAYS (which is current-only): zero.
	rule := &Rule{Status: StatusEnabled, ExpirationDays: 1}
	info := &ObjectInfo{Key: "a", IsLatest: false, ModTime: mustTime(t, "2024-01-01T00:00:00Z")}
	if got := ComputeDueAt(rule, ActionKindExpirationDays, info); !got.IsZero() {
		t.Fatalf("non-current under current rule should be zero, got %v", got)
	}
}

func TestComputeDueAt_NoncurrentDeleteMarkerHonorsNoncurrentDays(t *testing.T) {
	rule := &Rule{Status: StatusEnabled, NoncurrentVersionExpirationDays: 7}
	successor := mustTime(t, "2024-01-01T00:00:00Z")
	info := &ObjectInfo{
		Key:              "a",
		IsLatest:         false,
		IsDeleteMarker:   true,
		NumVersions:      3,
		SuccessorModTime: successor,
	}
	want := successor.Add(DaysToDuration(7))
	if got := ComputeDueAt(rule, ActionKindNoncurrentDays, info); !got.Equal(want) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func TestComputeDueAt_NoncurrentSuccessorMtime(t *testing.T) {
	rule := &Rule{Status: StatusEnabled, NoncurrentVersionExpirationDays: 30}
	successor := mustTime(t, "2024-01-01T00:00:00Z")
	info := &ObjectInfo{Key: "a", IsLatest: false, SuccessorModTime: successor}
	want := successor.Add(DaysToDuration(30))
	if got := ComputeDueAt(rule, ActionKindNoncurrentDays, info); !got.Equal(want) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func TestComputeDueAt_FilterRejects(t *testing.T) {
	rule := &Rule{Status: StatusEnabled, Prefix: "logs/", ExpirationDays: 1}
	info := &ObjectInfo{Key: "data/x", IsLatest: true, ModTime: mustTime(t, "2024-01-01T00:00:00Z")}
	if got := ComputeDueAt(rule, ActionKindExpirationDays, info); !got.IsZero() {
		t.Fatalf("filter rejection should be zero, got %v", got)
	}
}

func TestComputeDueAt_DisabledRule(t *testing.T) {
	rule := &Rule{Status: StatusDisabled, ExpirationDays: 1}
	info := &ObjectInfo{Key: "a", IsLatest: true, ModTime: mustTime(t, "2024-01-01T00:00:00Z")}
	if got := ComputeDueAt(rule, ActionKindExpirationDays, info); !got.IsZero() {
		t.Fatalf("disabled rule should be zero, got %v", got)
	}
}

func TestComputeDueAt_MPUInit(t *testing.T) {
	rule := &Rule{Status: StatusEnabled, AbortMPUDaysAfterInitiation: 7}
	init := mustTime(t, "2024-01-01T00:00:00Z")
	info := &ObjectInfo{Key: ".uploads/u1/", IsMPUInit: true, ModTime: init}
	want := init.Add(DaysToDuration(7))
	if got := ComputeDueAt(rule, ActionKindAbortMPU, info); !got.Equal(want) {
		t.Fatalf("want %v, got %v", want, got)
	}
}
