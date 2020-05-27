package topology

import (
	assert2 "github.com/stretchr/testify/assert"
	"testing"
)

func TestJoinInts(t *testing.T) {
	assert2.Equal(t, "1-3", JoinInts(1, 2, 3))
	assert2.Equal(t, "1 3", JoinInts(1, 3))
	assert2.Equal(t, "1 3 5", JoinInts(5, 1, 3))
	assert2.Equal(t, "1-3 5", JoinInts(1, 2, 3, 5))
	assert2.Equal(t, "1-3 5 7-9", JoinInts(7, 9, 8, 1, 2, 3, 5))
}
