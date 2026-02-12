package util

import "strings"

// ExtractBucketPath returns the bucket path under basePath that contains target.
// If requireChild is true, the target must include additional segments beyond the bucket itself.
func ExtractBucketPath(basePath, target string, requireChild bool) (string, bool) {
	cleanBase := strings.TrimSuffix(basePath, "/")
	if cleanBase == "" {
		return "", false
	}

	prefix := cleanBase + "/"
	if !strings.HasPrefix(target, prefix) {
		return "", false
	}

	rest := strings.TrimPrefix(target, prefix)
	if rest == "" {
		return "", false
	}

	bucketName, _, found := strings.Cut(rest, "/")
	if bucketName == "" {
		return "", false
	}

	if requireChild && !found {
		return "", false
	}

	return prefix + bucketName, true
}
