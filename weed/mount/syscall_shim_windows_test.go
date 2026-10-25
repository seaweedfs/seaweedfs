package mount

import "testing"

// The Windows adapter forwards cgofuse's xattr flags to SetXAttr untouched,
// which only holds while both sides number them the same. cgofuse's side is
// pinned in weed/mount/winfsp; this pins ours, so editing either alone fails
// rather than silently turning a create into a replace.
func TestXattrFlagValues(t *testing.T) {
	if xattr_CREATE != 1 {
		t.Errorf("xattr_CREATE = %d, want 1", xattr_CREATE)
	}
	if xattr_REPLACE != 2 {
		t.Errorf("xattr_REPLACE = %d, want 2", xattr_REPLACE)
	}
}
