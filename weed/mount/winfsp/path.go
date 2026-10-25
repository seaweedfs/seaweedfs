package winfsp

import "strings"

// splitPath breaks a WinFsp path into the components to walk. WinFsp normally
// hands out forward slashes, but callers reach the mount with either separator,
// and empty or "." components have to drop out rather than become a lookup for
// a name that does not exist.
func splitPath(path string) []string {
	trimmed := strings.Trim(strings.ReplaceAll(path, `\`, "/"), "/")
	if trimmed == "" {
		return nil
	}
	parts := make([]string, 0, strings.Count(trimmed, "/")+1)
	for _, name := range strings.Split(trimmed, "/") {
		if name == "" || name == "." {
			continue
		}
		parts = append(parts, name)
	}
	return parts
}

// splitParent separates the components leading up to the final name, which the
// create and delete operations need apart. ok is false for the root, which has
// no parent to operate in.
func splitParent(path string) (parent []string, name string, ok bool) {
	parts := splitPath(path)
	if len(parts) == 0 {
		return nil, "", false
	}
	return parts[:len(parts)-1], parts[len(parts)-1], true
}
