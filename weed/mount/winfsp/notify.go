package winfsp

import (
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// relativeToMount rebases a filer path onto the mount root. Metadata events
// cover the whole subscription, so a path outside this mount is not ours to
// report, and a sibling whose name merely starts with the root is not inside
// it either.
func relativeToMount(root, path util.FullPath) (string, bool) {
	full, prefix := string(path), string(root)
	if prefix == "" || prefix == "/" {
		return full, full != ""
	}
	if full == prefix {
		return "/", true
	}
	if !strings.HasPrefix(full, prefix+"/") {
		return "", false
	}
	return full[len(prefix):], true
}
