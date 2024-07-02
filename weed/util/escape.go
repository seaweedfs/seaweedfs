package util

import (
	"fmt"
	"net/url"
	"strings"
)

// UrlPathEscape escapes path excluding slashes
func UrlPathEscape(object string) string {
	var escapedParts []string
	for _, part := range strings.Split(object, "/") {
		escapedParts = append(escapedParts, url.PathEscape(part))
	}
	return strings.Join(escapedParts, "/")
}

// UrlPathUnescape unescapes path excluding slashes
func UrlPathUnescape(object string) (string, error) {
	var unescapedParts []string
	for _, part := range strings.Split(object, "/") {
		unescapedPart, err := url.PathUnescape(part)
		if err != nil {
			return "", fmt.Errorf("cannot unescape path: %w", err)
		}
		unescapedParts = append(unescapedParts, unescapedPart)
	}
	return strings.Join(unescapedParts, "/"), nil
}
