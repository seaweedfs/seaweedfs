package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// HasPrefix is a thin wrapper around strings.HasPrefix exported by the
// bootstrap package so call sites can avoid pulling strings just to do
// a prefix check. Trivial but at 0% coverage — pin it.

func TestHasPrefix(t *testing.T) {
	cases := []struct {
		name   string
		path   string
		prefix string
		want   bool
	}{
		{"matching prefix", "logs/2026/01/01", "logs/", true},
		{"exact match", "logs/", "logs/", true},
		{"non-matching prefix", "metrics/x", "logs/", false},
		{"shorter than prefix", "logs", "logs/", false},
		{"empty prefix matches all", "anything", "", true},
		{"both empty", "", "", true},
		{"empty input non-empty prefix", "", "x", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, HasPrefix(c.path, c.prefix))
		})
	}
}
