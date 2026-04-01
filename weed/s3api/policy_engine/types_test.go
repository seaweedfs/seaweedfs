package policy_engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCloneStringOrStringSliceCopiesBackingSlice(t *testing.T) {
	original := NewStringOrStringSlice("s3:GetObject", "s3:PutObject")

	cloned := CloneStringOrStringSlice(original)
	cloned.values[0] = "s3:DeleteObject"

	assert.Equal(t, []string{"s3:GetObject", "s3:PutObject"}, original.Strings())
	assert.Equal(t, []string{"s3:DeleteObject", "s3:PutObject"}, cloned.Strings())
}
