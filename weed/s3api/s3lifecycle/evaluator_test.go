package s3lifecycle

import (
	"testing"
	"time"
)

var now = time.Date(2026, 3, 27, 12, 0, 0, 0, time.UTC)

func TestEvaluate_ExpirationDays(t *testing.T) {
	rules := []Rule{{
		ID: "expire-30d", Status: "Enabled",
		ExpirationDays: 30,
	}}

	t.Run("object_older_than_days_is_expired", func(t *testing.T) {
		obj := ObjectInfo{
			Key: "data/file.txt", IsLatest: true,
			ModTime: now.Add(-31 * 24 * time.Hour),
		}
		result := Evaluate(rules, obj, now)
		assertAction(t, ActionDeleteObject, result.Action)
		assertEqual(t, "expire-30d", result.RuleID)
	})

	t.Run("object_younger_than_days_is_not_expired", func(t *testing.T) {
		obj := ObjectInfo{
			Key: "data/file.txt", IsLatest: true,
			ModTime: now.Add(-10 * 24 * time.Hour),
		}
		result := Evaluate(rules, obj, now)
		assertAction(t, ActionNone, result.Action)
	})

	t.Run("non_latest_version_not_affected_by_expiration_days", func(t *testing.T) {
		obj := ObjectInfo{
			Key: "data/file.txt", IsLatest: false,
			ModTime: now.Add(-60 * 24 * time.Hour),
		}
		result := Evaluate(rules, obj, now)
		assertAction(t, ActionNone, result.Action)
	})

	t.Run("delete_marker_not_affected_by_expiration_days", func(t *testing.T) {
		obj := ObjectInfo{
			Key: "data/file.txt", IsLatest: true, IsDeleteMarker: true,
			ModTime: now.Add(-60 * 24 * time.Hour), NumVersions: 3,
		}
		result := Evaluate(rules, obj, now)
		assertAction(t, ActionNone, result.Action)
	})
}

func TestEvaluate_ExpirationDate(t *testing.T) {
	expirationDate := time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC)
	rules := []Rule{{
		ID: "expire-date", Status: "Enabled",
		ExpirationDate: expirationDate,
	}}

	t.Run("object_expired_after_date", func(t *testing.T) {
		obj := ObjectInfo{
			Key: "file.txt", IsLatest: true,
			ModTime: now.Add(-60 * 24 * time.Hour),
		}
		result := Evaluate(rules, obj, now)
		assertAction(t, ActionDeleteObject, result.Action)
	})

	t.Run("object_not_expired_before_date", func(t *testing.T) {
		obj := ObjectInfo{
			Key: "file.txt", IsLatest: true,
			ModTime: now.Add(-1 * time.Hour),
		}
		beforeDate := time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)
		result := Evaluate(rules, obj, beforeDate)
		assertAction(t, ActionNone, result.Action)
	})
}

func TestEvaluate_ExpiredObjectDeleteMarker(t *testing.T) {
	rules := []Rule{{
		ID: "cleanup-markers", Status: "Enabled",
		ExpiredObjectDeleteMarker: true,
	}}

	t.Run("sole_delete_marker_is_expired", func(t *testing.T) {
		obj := ObjectInfo{
			Key: "file.txt", IsLatest: true, IsDeleteMarker: true,
			NumVersions: 1,
		}
		result := Evaluate(rules, obj, now)
		assertAction(t, ActionExpireDeleteMarker, result.Action)
	})

	t.Run("delete_marker_with_other_versions_not_expired", func(t *testing.T) {
		obj := ObjectInfo{
			Key: "file.txt", IsLatest: true, IsDeleteMarker: true,
			NumVersions: 3,
		}
		result := Evaluate(rules, obj, now)
		assertAction(t, ActionNone, result.Action)
	})

	t.Run("non_latest_delete_marker_not_expired", func(t *testing.T) {
		obj := ObjectInfo{
			Key: "file.txt", IsLatest: false, IsDeleteMarker: true,
			NumVersions: 1,
		}
		result := Evaluate(rules, obj, now)
		assertAction(t, ActionNone, result.Action)
	})

	t.Run("non_delete_marker_not_affected", func(t *testing.T) {
		obj := ObjectInfo{
			Key: "file.txt", IsLatest: true, IsDeleteMarker: false,
			NumVersions: 1,
		}
		result := Evaluate(rules, obj, now)
		assertAction(t, ActionNone, result.Action)
	})
}

func TestEvaluate_NoncurrentVersionExpiration(t *testing.T) {
	rules := []Rule{{
		ID: "expire-noncurrent", Status: "Enabled",
		NoncurrentVersionExpirationDays: 30,
	}}

	t.Run("old_noncurrent_version_is_expired", func(t *testing.T) {
		obj := ObjectInfo{
			Key: "file.txt", IsLatest: false,
			SuccessorModTime: now.Add(-45 * 24 * time.Hour),
		}
		result := Evaluate(rules, obj, now)
		assertAction(t, ActionDeleteVersion, result.Action)
	})

	t.Run("recent_noncurrent_version_is_not_expired", func(t *testing.T) {
		obj := ObjectInfo{
			Key: "file.txt", IsLatest: false,
			SuccessorModTime: now.Add(-10 * 24 * time.Hour),
		}
		result := Evaluate(rules, obj, now)
		assertAction(t, ActionNone, result.Action)
	})

	t.Run("latest_version_not_affected", func(t *testing.T) {
		obj := ObjectInfo{
			Key: "file.txt", IsLatest: true,
			ModTime: now.Add(-60 * 24 * time.Hour),
		}
		result := Evaluate(rules, obj, now)
		assertAction(t, ActionNone, result.Action)
	})
}

func TestShouldExpireNoncurrentVersion(t *testing.T) {
	rule := Rule{
		ID: "noncurrent-rule", Status: "Enabled",
		NoncurrentVersionExpirationDays: 30,
		NewerNoncurrentVersions:         2,
	}

	t.Run("old_version_beyond_count_is_expired", func(t *testing.T) {
		obj := ObjectInfo{
			Key: "file.txt", IsLatest: false,
			SuccessorModTime: now.Add(-45 * 24 * time.Hour),
		}
		// noncurrentIndex=2 means this is the 3rd noncurrent version (0-indexed)
		// With NewerNoncurrentVersions=2, indices 0 and 1 are kept.
		if !ShouldExpireNoncurrentVersion(rule, obj, 2, now) {
			t.Error("expected version at index 2 to be expired")
		}
	})

	t.Run("old_version_within_count_is_kept", func(t *testing.T) {
		obj := ObjectInfo{
			Key: "file.txt", IsLatest: false,
			SuccessorModTime: now.Add(-45 * 24 * time.Hour),
		}
		// noncurrentIndex=1 is within the keep threshold (NewerNoncurrentVersions=2).
		if ShouldExpireNoncurrentVersion(rule, obj, 1, now) {
			t.Error("expected version at index 1 to be kept")
		}
	})

	t.Run("recent_version_beyond_count_is_kept", func(t *testing.T) {
		obj := ObjectInfo{
			Key: "file.txt", IsLatest: false,
			SuccessorModTime: now.Add(-5 * 24 * time.Hour),
		}
		// Even at index 5 (beyond count), if too young, it's kept.
		if ShouldExpireNoncurrentVersion(rule, obj, 5, now) {
			t.Error("expected recent version to be kept regardless of index")
		}
	})

	t.Run("disabled_rule_never_expires", func(t *testing.T) {
		disabled := Rule{
			ID: "disabled", Status: "Disabled",
			NoncurrentVersionExpirationDays: 1,
		}
		obj := ObjectInfo{
			Key: "file.txt", IsLatest: false,
			SuccessorModTime: now.Add(-365 * 24 * time.Hour),
		}
		if ShouldExpireNoncurrentVersion(disabled, obj, 10, now) {
			t.Error("disabled rule should never expire")
		}
	})
}

func TestEvaluate_PrefixFilter(t *testing.T) {
	rules := []Rule{{
		ID: "logs-only", Status: "Enabled",
		Prefix:         "logs/",
		ExpirationDays: 7,
	}}

	t.Run("matching_prefix", func(t *testing.T) {
		obj := ObjectInfo{
			Key: "logs/app.log", IsLatest: true,
			ModTime: now.Add(-10 * 24 * time.Hour),
		}
		result := Evaluate(rules, obj, now)
		assertAction(t, ActionDeleteObject, result.Action)
	})

	t.Run("non_matching_prefix", func(t *testing.T) {
		obj := ObjectInfo{
			Key: "data/file.txt", IsLatest: true,
			ModTime: now.Add(-10 * 24 * time.Hour),
		}
		result := Evaluate(rules, obj, now)
		assertAction(t, ActionNone, result.Action)
	})
}

func TestEvaluate_TagFilter(t *testing.T) {
	rules := []Rule{{
		ID: "temp-only", Status: "Enabled",
		ExpirationDays: 1,
		FilterTags:     map[string]string{"env": "temp"},
	}}

	t.Run("matching_tags", func(t *testing.T) {
		obj := ObjectInfo{
			Key: "file.txt", IsLatest: true,
			ModTime: now.Add(-5 * 24 * time.Hour),
			Tags:    map[string]string{"env": "temp", "project": "foo"},
		}
		result := Evaluate(rules, obj, now)
		assertAction(t, ActionDeleteObject, result.Action)
	})

	t.Run("missing_tag", func(t *testing.T) {
		obj := ObjectInfo{
			Key: "file.txt", IsLatest: true,
			ModTime: now.Add(-5 * 24 * time.Hour),
			Tags:    map[string]string{"project": "foo"},
		}
		result := Evaluate(rules, obj, now)
		assertAction(t, ActionNone, result.Action)
	})

	t.Run("wrong_tag_value", func(t *testing.T) {
		obj := ObjectInfo{
			Key: "file.txt", IsLatest: true,
			ModTime: now.Add(-5 * 24 * time.Hour),
			Tags:    map[string]string{"env": "prod"},
		}
		result := Evaluate(rules, obj, now)
		assertAction(t, ActionNone, result.Action)
	})

	t.Run("nil_object_tags", func(t *testing.T) {
		obj := ObjectInfo{
			Key: "file.txt", IsLatest: true,
			ModTime: now.Add(-5 * 24 * time.Hour),
		}
		result := Evaluate(rules, obj, now)
		assertAction(t, ActionNone, result.Action)
	})
}

func TestEvaluate_SizeFilter(t *testing.T) {
	rules := []Rule{{
		ID: "large-files", Status: "Enabled",
		ExpirationDays:        7,
		FilterSizeGreaterThan: 1024 * 1024,       // > 1 MB
		FilterSizeLessThan:    100 * 1024 * 1024,  // < 100 MB
	}}

	t.Run("matching_size", func(t *testing.T) {
		obj := ObjectInfo{
			Key: "file.bin", IsLatest: true,
			ModTime: now.Add(-10 * 24 * time.Hour),
			Size:    10 * 1024 * 1024, // 10 MB
		}
		result := Evaluate(rules, obj, now)
		assertAction(t, ActionDeleteObject, result.Action)
	})

	t.Run("too_small", func(t *testing.T) {
		obj := ObjectInfo{
			Key: "file.bin", IsLatest: true,
			ModTime: now.Add(-10 * 24 * time.Hour),
			Size:    512, // 512 bytes
		}
		result := Evaluate(rules, obj, now)
		assertAction(t, ActionNone, result.Action)
	})

	t.Run("too_large", func(t *testing.T) {
		obj := ObjectInfo{
			Key: "file.bin", IsLatest: true,
			ModTime: now.Add(-10 * 24 * time.Hour),
			Size:    200 * 1024 * 1024, // 200 MB
		}
		result := Evaluate(rules, obj, now)
		assertAction(t, ActionNone, result.Action)
	})
}

func TestEvaluate_CombinedFilters(t *testing.T) {
	rules := []Rule{{
		ID: "combined", Status: "Enabled",
		Prefix:                "logs/",
		ExpirationDays:        7,
		FilterTags:            map[string]string{"env": "dev"},
		FilterSizeGreaterThan: 100,
	}}

	t.Run("all_filters_match", func(t *testing.T) {
		obj := ObjectInfo{
			Key: "logs/app.log", IsLatest: true,
			ModTime: now.Add(-10 * 24 * time.Hour),
			Size:    1024,
			Tags:    map[string]string{"env": "dev"},
		}
		result := Evaluate(rules, obj, now)
		assertAction(t, ActionDeleteObject, result.Action)
	})

	t.Run("prefix_doesnt_match", func(t *testing.T) {
		obj := ObjectInfo{
			Key: "data/app.log", IsLatest: true,
			ModTime: now.Add(-10 * 24 * time.Hour),
			Size:    1024,
			Tags:    map[string]string{"env": "dev"},
		}
		result := Evaluate(rules, obj, now)
		assertAction(t, ActionNone, result.Action)
	})

	t.Run("tag_doesnt_match", func(t *testing.T) {
		obj := ObjectInfo{
			Key: "logs/app.log", IsLatest: true,
			ModTime: now.Add(-10 * 24 * time.Hour),
			Size:    1024,
			Tags:    map[string]string{"env": "prod"},
		}
		result := Evaluate(rules, obj, now)
		assertAction(t, ActionNone, result.Action)
	})

	t.Run("size_doesnt_match", func(t *testing.T) {
		obj := ObjectInfo{
			Key: "logs/app.log", IsLatest: true,
			ModTime: now.Add(-10 * 24 * time.Hour),
			Size:    50, // too small
			Tags:    map[string]string{"env": "dev"},
		}
		result := Evaluate(rules, obj, now)
		assertAction(t, ActionNone, result.Action)
	})
}

func TestEvaluate_DisabledRule(t *testing.T) {
	rules := []Rule{{
		ID: "disabled", Status: "Disabled",
		ExpirationDays: 1,
	}}
	obj := ObjectInfo{
		Key: "file.txt", IsLatest: true,
		ModTime: now.Add(-365 * 24 * time.Hour),
	}
	result := Evaluate(rules, obj, now)
	assertAction(t, ActionNone, result.Action)
}

func TestEvaluate_MultipleRules_Priority(t *testing.T) {
	t.Run("delete_marker_takes_priority_over_expiration", func(t *testing.T) {
		rules := []Rule{
			{ID: "expire", Status: "Enabled", ExpirationDays: 1},
			{ID: "marker", Status: "Enabled", ExpiredObjectDeleteMarker: true},
		}
		obj := ObjectInfo{
			Key: "file.txt", IsLatest: true, IsDeleteMarker: true,
			NumVersions: 1, ModTime: now.Add(-10 * 24 * time.Hour),
		}
		result := Evaluate(rules, obj, now)
		assertAction(t, ActionExpireDeleteMarker, result.Action)
		assertEqual(t, "marker", result.RuleID)
	})

	t.Run("first_matching_expiration_rule_wins", func(t *testing.T) {
		rules := []Rule{
			{ID: "rule1", Status: "Enabled", ExpirationDays: 30, Prefix: "logs/"},
			{ID: "rule2", Status: "Enabled", ExpirationDays: 7},
		}
		obj := ObjectInfo{
			Key: "logs/app.log", IsLatest: true,
			ModTime: now.Add(-31 * 24 * time.Hour),
		}
		result := Evaluate(rules, obj, now)
		assertAction(t, ActionDeleteObject, result.Action)
		assertEqual(t, "rule1", result.RuleID)
	})
}

func TestEvaluate_EmptyPrefix(t *testing.T) {
	rules := []Rule{{
		ID: "all", Status: "Enabled",
		ExpirationDays: 30,
	}}
	obj := ObjectInfo{
		Key: "any/path/file.txt", IsLatest: true,
		ModTime: now.Add(-31 * 24 * time.Hour),
	}
	result := Evaluate(rules, obj, now)
	assertAction(t, ActionDeleteObject, result.Action)
}

func TestEvaluateMPUAbort(t *testing.T) {
	rules := []Rule{{
		ID: "abort-mpu", Status: "Enabled",
		AbortMPUDaysAfterInitiation: 7,
	}}

	t.Run("old_upload_is_aborted", func(t *testing.T) {
		result := EvaluateMPUAbort(rules, "uploads/file.bin", now.Add(-10*24*time.Hour), now)
		assertAction(t, ActionAbortMultipartUpload, result.Action)
	})

	t.Run("recent_upload_is_not_aborted", func(t *testing.T) {
		result := EvaluateMPUAbort(rules, "uploads/file.bin", now.Add(-3*24*time.Hour), now)
		assertAction(t, ActionNone, result.Action)
	})

	t.Run("prefix_scoped_abort", func(t *testing.T) {
		prefixRules := []Rule{{
			ID: "abort-logs", Status: "Enabled",
			Prefix:                      "logs/",
			AbortMPUDaysAfterInitiation: 1,
		}}
		result := EvaluateMPUAbort(prefixRules, "data/file.bin", now.Add(-5*24*time.Hour), now)
		assertAction(t, ActionNone, result.Action)
	})
}

func TestExpectedExpiryTime(t *testing.T) {
	ref := time.Date(2026, 3, 1, 15, 30, 0, 0, time.UTC)

	t.Run("30_days", func(t *testing.T) {
		// S3 spec: expires at midnight UTC of day 32 (ref + 31 days, truncated).
		expiry := expectedExpiryTime(ref, 30)
		expected := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
		if !expiry.Equal(expected) {
			t.Errorf("expected %v, got %v", expected, expiry)
		}
	})

	t.Run("zero_days_returns_ref", func(t *testing.T) {
		expiry := expectedExpiryTime(ref, 0)
		if !expiry.Equal(ref) {
			t.Errorf("expected %v, got %v", ref, expiry)
		}
	})
}

func assertAction(t *testing.T, expected, actual Action) {
	t.Helper()
	if expected != actual {
		t.Errorf("expected action %d, got %d", expected, actual)
	}
}

func assertEqual(t *testing.T, expected, actual string) {
	t.Helper()
	if expected != actual {
		t.Errorf("expected %q, got %q", expected, actual)
	}
}
