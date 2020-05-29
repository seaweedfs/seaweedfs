package util

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHumanReadableInts(t *testing.T) {
	assert.Equal(t, "1-3", HumanReadableInts(1, 2, 3))
	assert.Equal(t, "1 3", HumanReadableInts(1, 3))
	assert.Equal(t, "1 3 5", HumanReadableInts(5, 1, 3))
	assert.Equal(t, "1-3 5", HumanReadableInts(1, 2, 3, 5))
	assert.Equal(t, "1-3 5 7-9", HumanReadableInts(7, 9, 8, 1, 2, 3, 5))
}
