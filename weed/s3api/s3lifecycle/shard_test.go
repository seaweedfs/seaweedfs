package s3lifecycle

import "testing"

func TestShardIDInRange(t *testing.T) {
	cases := []struct{ bucket, key string }{
		{"", ""},
		{"a", "b"},
		{"my-bucket", "path/to/object.txt"},
		{"日本語", "キー"},
		{"bucket-with-dash", ""},
	}
	for _, c := range cases {
		got := ShardID(c.bucket, c.key)
		if got < 0 || got >= ShardCount {
			t.Fatalf("ShardID(%q,%q)=%d outside [0,%d)", c.bucket, c.key, got, ShardCount)
		}
	}
}

func TestShardIDDeterministic(t *testing.T) {
	a := ShardID("bucket", "key")
	b := ShardID("bucket", "key")
	if a != b {
		t.Fatalf("non-deterministic: %d vs %d", a, b)
	}
}

func TestShardIDDistinctFromBucket(t *testing.T) {
	// Same key, different buckets must be free to land on different shards.
	// (Statistical: with 16 shards, picking 16 distinct buckets should hit
	// at least 4 different shards.)
	seen := map[int]struct{}{}
	for i := 0; i < 16; i++ {
		buckets := []string{
			"alpha", "bravo", "charlie", "delta",
			"echo", "foxtrot", "golf", "hotel",
			"india", "juliet", "kilo", "lima",
			"mike", "november", "oscar", "papa",
		}
		seen[ShardID(buckets[i], "samekey")] = struct{}{}
	}
	if len(seen) < 4 {
		t.Fatalf("expected >=4 distinct shards across 16 buckets, got %d", len(seen))
	}
}
