package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// HasPrefix is a thin wrapper around strings.HasPrefix exported by the
// bootstrap package so call sites can avoid pulling strings just to do
// a prefix check. Trivial but at 0% coverage — pin it.

func TestHasPrefix(t *testing.T) {
	assert.True(t, HasPrefix("logs/2026/01/01", "logs/"))
	assert.True(t, HasPrefix("logs/", "logs/"))
	assert.False(t, HasPrefix("metrics/x", "logs/"))
	assert.False(t, HasPrefix("logs", "logs/")) // shorter than prefix
	assert.True(t, HasPrefix("anything", ""))   // empty prefix matches all
	assert.True(t, HasPrefix("", ""))
	assert.False(t, HasPrefix("", "x"))
}
