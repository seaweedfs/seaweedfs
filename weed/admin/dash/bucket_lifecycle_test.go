package dash

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

func minimalLifecycleRule() BucketLifecycleRule {
	return BucketLifecycleRule{
		ID:             "rule-1",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 30,
	}
}

func TestValidateBucketLifecycleRules_Valid(t *testing.T) {
	if err := validateBucketLifecycleRules([]BucketLifecycleRule{minimalLifecycleRule()}); err != nil {
		t.Fatalf("expected valid rule set to pass, got: %v", err)
	}
}

func TestValidateBucketLifecycleRules_Empty(t *testing.T) {
	if err := validateBucketLifecycleRules(nil); err != nil {
		t.Fatalf("expected empty rule set to pass, got: %v", err)
	}
}

func TestValidateBucketLifecycleRules_TooManyRules(t *testing.T) {
	rules := make([]BucketLifecycleRule, MaxBucketLifecycleRules+1)
	for i := range rules {
		rules[i] = BucketLifecycleRule{Status: s3lifecycle.StatusEnabled, ExpirationDays: 30}
	}
	if err := validateBucketLifecycleRules(rules); err == nil {
		t.Fatal("expected error for exceeding the rule count cap")
	}
}

func TestValidateBucketLifecycleRules_DuplicateIDs(t *testing.T) {
	rule := minimalLifecycleRule()
	if err := validateBucketLifecycleRules([]BucketLifecycleRule{rule, rule}); err == nil {
		t.Fatal("expected error for duplicate rule IDs")
	}
}

func TestValidateBucketLifecycleRules_IDTooLong(t *testing.T) {
	rule := minimalLifecycleRule()
	longID := ""
	for i := 0; i < MaxBucketLifecycleRuleIDLength+1; i++ {
		longID += "a"
	}
	rule.ID = longID
	if err := validateBucketLifecycleRules([]BucketLifecycleRule{rule}); err == nil {
		t.Fatal("expected error for an ID longer than the cap")
	}
}

func TestValidateBucketLifecycleRules_InvalidStatus(t *testing.T) {
	rule := minimalLifecycleRule()
	rule.Status = "sort-of-enabled"
	if err := validateBucketLifecycleRules([]BucketLifecycleRule{rule}); err == nil {
		t.Fatal("expected error for an invalid status")
	}
}

func TestValidateBucketLifecycleRules_ExpirationDaysAndDateMutuallyExclusive(t *testing.T) {
	rule := minimalLifecycleRule()
	rule.ExpirationDate = "2030-01-01"
	if err := validateBucketLifecycleRules([]BucketLifecycleRule{rule}); err == nil {
		t.Fatal("expected error when both expiration_days and expiration_date are set")
	}
}

func TestValidateBucketLifecycleRules_InvalidExpirationDate(t *testing.T) {
	rule := BucketLifecycleRule{Status: s3lifecycle.StatusEnabled, ExpirationDate: "01/01/2030"}
	if err := validateBucketLifecycleRules([]BucketLifecycleRule{rule}); err == nil {
		t.Fatal("expected error for a malformed expiration_date")
	}
}

func TestValidateBucketLifecycleRules_NegativeExpirationDays(t *testing.T) {
	rule := minimalLifecycleRule()
	rule.ExpirationDays = -1
	if err := validateBucketLifecycleRules([]BucketLifecycleRule{rule}); err == nil {
		t.Fatal("expected error for negative expiration_days")
	}
}

func TestValidateBucketLifecycleRules_NewerNoncurrentVersionsRequiresExpiration(t *testing.T) {
	rule := BucketLifecycleRule{
		Status:                  s3lifecycle.StatusEnabled,
		NewerNoncurrentVersions: 2,
	}
	if err := validateBucketLifecycleRules([]BucketLifecycleRule{rule}); err == nil {
		t.Fatal("expected error when newer_noncurrent_versions is set without noncurrent_version_expiration_days")
	}
}

func TestValidateBucketLifecycleRules_NegativeAbortMultipartDays(t *testing.T) {
	rule := minimalLifecycleRule()
	rule.AbortMultipartDays = -5
	if err := validateBucketLifecycleRules([]BucketLifecycleRule{rule}); err == nil {
		t.Fatal("expected error for negative abort_multipart_days")
	}
}

func TestValidateBucketLifecycleRules_SizeGreaterThanNotLessThanSizeLessThan(t *testing.T) {
	rule := minimalLifecycleRule()
	rule.SizeGreaterThan = 2048
	rule.SizeLessThan = 1024
	if err := validateBucketLifecycleRules([]BucketLifecycleRule{rule}); err == nil {
		t.Fatal("expected error when size_greater_than >= size_less_than")
	}
}

func TestValidateBucketLifecycleRules_NegativeSizeBounds(t *testing.T) {
	rule := minimalLifecycleRule()
	rule.SizeGreaterThan = -1
	if err := validateBucketLifecycleRules([]BucketLifecycleRule{rule}); err == nil {
		t.Fatal("expected error for a negative size_greater_than")
	}
}

func TestValidateBucketLifecycleRules_EmptyTagKey(t *testing.T) {
	rule := minimalLifecycleRule()
	rule.Tags = map[string]string{"": "value"}
	if err := validateBucketLifecycleRules([]BucketLifecycleRule{rule}); err == nil {
		t.Fatal("expected error for an empty tag key")
	}
}

func TestValidateBucketLifecycleRules_EmptyTagValue(t *testing.T) {
	rule := minimalLifecycleRule()
	rule.Tags = map[string]string{"env": " "}
	if err := validateBucketLifecycleRules([]BucketLifecycleRule{rule}); err == nil {
		t.Fatal("expected error for a blank tag value")
	}
}

func TestValidateBucketLifecycleRules_NoAction(t *testing.T) {
	rule := BucketLifecycleRule{Status: s3lifecycle.StatusEnabled, Prefix: "logs/"}
	if err := validateBucketLifecycleRules([]BucketLifecycleRule{rule}); err == nil {
		t.Fatal("expected error for a rule with no action")
	}
}

func TestFromBucketLifecycleRule_MapsAllFields(t *testing.T) {
	rule := BucketLifecycleRule{
		ID:                              "rule-1",
		Status:                          s3lifecycle.StatusEnabled,
		Prefix:                          "logs/",
		Tags:                            map[string]string{"env": "dev"},
		SizeGreaterThan:                 1024,
		SizeLessThan:                    2048,
		ExpirationDate:                  "2030-01-15",
		ExpiredObjectDeleteMarker:       true,
		NoncurrentVersionExpirationDays: 30,
		NewerNoncurrentVersions:         2,
		AbortMultipartDays:              7,
	}

	out, err := fromBucketLifecycleRule(rule)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if out.ID != rule.ID {
		t.Errorf("ID = %q, want %q", out.ID, rule.ID)
	}
	if out.Status != rule.Status {
		t.Errorf("Status = %q, want %q", out.Status, rule.Status)
	}
	if out.Prefix != rule.Prefix {
		t.Errorf("Prefix = %q, want %q", out.Prefix, rule.Prefix)
	}
	if len(out.FilterTags) != 1 || out.FilterTags["env"] != "dev" {
		t.Errorf("FilterTags = %v, want {env: dev}", out.FilterTags)
	}
	if out.FilterSizeGreaterThan != rule.SizeGreaterThan {
		t.Errorf("FilterSizeGreaterThan = %d, want %d", out.FilterSizeGreaterThan, rule.SizeGreaterThan)
	}
	if out.FilterSizeLessThan != rule.SizeLessThan {
		t.Errorf("FilterSizeLessThan = %d, want %d", out.FilterSizeLessThan, rule.SizeLessThan)
	}
	wantDate, _ := time.Parse(time.DateOnly, rule.ExpirationDate)
	if !out.ExpirationDate.Equal(wantDate) {
		t.Errorf("ExpirationDate = %v, want %v", out.ExpirationDate, wantDate)
	}
	if !out.ExpiredObjectDeleteMarker {
		t.Error("expected ExpiredObjectDeleteMarker=true")
	}
	if out.NoncurrentVersionExpirationDays != rule.NoncurrentVersionExpirationDays {
		t.Errorf("NoncurrentVersionExpirationDays = %d, want %d", out.NoncurrentVersionExpirationDays, rule.NoncurrentVersionExpirationDays)
	}
	if out.NewerNoncurrentVersions != rule.NewerNoncurrentVersions {
		t.Errorf("NewerNoncurrentVersions = %d, want %d", out.NewerNoncurrentVersions, rule.NewerNoncurrentVersions)
	}
	if out.AbortMPUDaysAfterInitiation != rule.AbortMultipartDays {
		t.Errorf("AbortMPUDaysAfterInitiation = %d, want %d", out.AbortMPUDaysAfterInitiation, rule.AbortMultipartDays)
	}
}

func TestFromBucketLifecycleRule_EmptyExpirationDate(t *testing.T) {
	rule := minimalLifecycleRule()
	rule.ExpirationDate = ""

	out, err := fromBucketLifecycleRule(rule)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !out.ExpirationDate.IsZero() {
		t.Errorf("expected zero ExpirationDate, got %v", out.ExpirationDate)
	}
}

func TestFromBucketLifecycleRule_InvalidExpirationDate(t *testing.T) {
	rule := minimalLifecycleRule()
	rule.ExpirationDate = "not-a-date"

	if _, err := fromBucketLifecycleRule(rule); err == nil {
		t.Fatal("expected error for a malformed expiration date")
	}
}
