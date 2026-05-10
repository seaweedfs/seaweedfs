package s3lifecycle

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Final unit-test cleanup ahead of the integration suite. Pins the
// remaining 0%-and-default-branch helpers in the s3lifecycle package
// and the lifecycletest builder so a regression doesn't slip in
// during the integration work that follows.

func TestActionKind_StringUnspecifiedDefault(t *testing.T) {
	// String's default branch (covers ActionKindUnspecified and any
	// future enum value not listed) must render "unspecified" rather
	// than empty or panic. Operators read this via the metrics labels
	// (see S3LifecycleDispatchCounter "kind").
	assert.Equal(t, "unspecified", ActionKindUnspecified.String())
	assert.Equal(t, "unspecified", ActionKind(99).String())
}

func TestHashExtended_DirectFromLifecyclePackage(t *testing.T) {
	// HashExtended is exercised from the s3api package's identity
	// tests; pin it here so the s3lifecycle package's own coverage
	// reflects the call. A nil/empty map produces no bytes, so the
	// CAS witness collapses to "no Extended" rather than a synthetic
	// hash that would mismatch on the server.
	assert.Empty(t, HashExtended(nil))
	assert.Empty(t, HashExtended(map[string][]byte{}))
	got := HashExtended(map[string][]byte{"a": []byte("1")})
	assert.NotEmpty(t, got)
	// Same content, different literal/insertion order across multiple
	// keys: hash must be stable. A single-key check can't catch an
	// iteration-order regression — multiple keys force the helper's
	// sort path to actually do work.
	first := HashExtended(map[string][]byte{
		"a": []byte("1"),
		"b": []byte("2"),
		"c": []byte("3"),
	})
	second := HashExtended(map[string][]byte{
		"c": []byte("3"),
		"a": []byte("1"),
		"b": []byte("2"),
	})
	assert.Equal(t, first, second, "hash must be insensitive to map iteration order")
}
