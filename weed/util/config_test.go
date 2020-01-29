package util

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAllKeysWithEnv(t *testing.T) {

	v := GetViper()
	v.BindEnv("id")
	v.BindEnv("foo", "foo")

	// bind and define environment variables (including a nested one)
	os.Setenv("WEED_ID", "13")
	os.Setenv("WEED_FOO_BAR", "baz")

	sub := v.Sub("foo")

	assert.Equal(t, "13", v.GetString("id"))
	assert.Equal(t, "baz", sub.GetString("bar"))
}
