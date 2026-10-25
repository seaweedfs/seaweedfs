package s3lifecycle

import (
	"reflect"
	"testing"
)

func TestRuleActionKinds_SingleAction(t *testing.T) {
	cases := []struct {
		name string
		rule *Rule
		want []ActionKind
	}{
		{"expiration_days", &Rule{ExpirationDays: 30}, []ActionKind{ActionKindExpirationDays}},
		{"expiration_date", &Rule{ExpirationDate: mustTime(t, "2025-06-15T00:00:00Z")}, []ActionKind{ActionKindExpirationDate}},
		{"expired_delete_marker", &Rule{ExpiredObjectDeleteMarker: true}, []ActionKind{ActionKindExpiredDeleteMarker}},
		{"noncurrent_days", &Rule{NoncurrentVersionExpirationDays: 30}, []ActionKind{ActionKindNoncurrentDays}},
		{"newer_noncurrent_alone", &Rule{NewerNoncurrentVersions: 3}, []ActionKind{ActionKindNewerNoncurrent}},
		{"abort_mpu", &Rule{AbortMPUDaysAfterInitiation: 7}, []ActionKind{ActionKindAbortMPU}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := RuleActionKinds(c.rule); !reflect.DeepEqual(got, c.want) {
				t.Fatalf("want %v, got %v", c.want, got)
			}
		})
	}
}

func TestRuleActionKinds_MultiAction(t *testing.T) {
	// The bug this fixes: a single XML <Rule> can declare ExpirationDays,
	// AbortMPU, and NoncurrentDays in parallel. Each must compile to its
	// own action with its own delay group / mode / pending — not be
	// collapsed into one entry whose delay is the smallest.
	rule := &Rule{
		ExpirationDays:                  90,
		AbortMPUDaysAfterInitiation:     7,
		NoncurrentVersionExpirationDays: 30,
	}
	want := []ActionKind{
		ActionKindExpirationDays,
		ActionKindNoncurrentDays,
		ActionKindAbortMPU,
	}
	if got := RuleActionKinds(rule); !reflect.DeepEqual(got, want) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func TestRuleActionKinds_NoncurrentDaysSubsumesNewerNoncurrent(t *testing.T) {
	// When both NoncurrentDays and NewerNoncurrent are set on the same rule,
	// they are AWS-paired conditions for ONE noncurrent expiration action,
	// not two; only NoncurrentDays is emitted.
	rule := &Rule{NoncurrentVersionExpirationDays: 30, NewerNoncurrentVersions: 3}
	want := []ActionKind{ActionKindNoncurrentDays}
	if got := RuleActionKinds(rule); !reflect.DeepEqual(got, want) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func TestRuleActionKinds_Empty(t *testing.T) {
	if got := RuleActionKinds(&Rule{}); len(got) != 0 {
		t.Fatalf("want empty, got %v", got)
	}
	if got := RuleActionKinds(nil); got != nil {
		t.Fatalf("want nil, got %v", got)
	}
}

func TestActionKind_StringIsStable(t *testing.T) {
	// The string is the leaf-directory name on disk; renaming would
	// orphan existing state, so pin the values.
	cases := map[ActionKind]string{
		ActionKindExpirationDays:      "expiration_days",
		ActionKindExpirationDate:      "expiration_date",
		ActionKindNoncurrentDays:      "noncurrent_days",
		ActionKindNewerNoncurrent:     "newer_noncurrent",
		ActionKindAbortMPU:            "abort_mpu",
		ActionKindExpiredDeleteMarker: "expired_delete_marker",
	}
	for k, want := range cases {
		if got := k.String(); got != want {
			t.Fatalf("ActionKind(%d).String() = %q, want %q", k, got, want)
		}
	}
}
