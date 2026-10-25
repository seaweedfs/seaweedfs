//go:build !windows

package localsink

import "testing"

func TestSanitizeFsKeyKeepsColons(t *testing.T) {
	key := "/backup/a:b/c:d.txt"
	if got := sanitizeFsKey(key); got != key {
		t.Errorf("sanitizeFsKey(%q) = %q, want identity", key, got)
	}
}
