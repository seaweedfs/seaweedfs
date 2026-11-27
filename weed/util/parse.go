package util

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

func ParseInt(text string, defaultValue int) int {
	count, parseError := strconv.ParseInt(text, 10, 64)
	if parseError != nil {
		if len(text) > 0 {
			return 0
		}
		return defaultValue
	}
	return int(count)
}
func ParseUint64(text string, defaultValue uint64) uint64 {
	count, parseError := strconv.ParseUint(text, 10, 64)
	if parseError != nil {
		if len(text) > 0 {
			return 0
		}
		return defaultValue
	}
	return count
}

func ParseBool(s string, defaultValue bool) bool {
	value, err := strconv.ParseBool(s)
	if err != nil {
		return defaultValue
	}
	return value
}

func BoolToString(b bool) string {
	return strconv.FormatBool(b)
}

func ParseFilerUrl(entryPath string) (filerServer string, filerPort int64, path string, err error) {
	if !strings.HasPrefix(entryPath, "http://") && !strings.HasPrefix(entryPath, "https://") {
		entryPath = "http://" + entryPath
	}

	var u *url.URL
	u, err = url.Parse(entryPath)
	if err != nil {
		return
	}
	filerServer = u.Hostname()
	portString := u.Port()
	if portString != "" {
		filerPort, err = strconv.ParseInt(portString, 10, 32)
	}
	path = u.Path
	return
}

func ParseHostPort(hostPort string) (filerServer string, filerPort int64, err error) {
	host, port, err := net.SplitHostPort(hostPort)
	if err != nil {
		err = fmt.Errorf("failed to parse %s", hostPort)
		return
	}

	filerPort, err = strconv.ParseInt(port, 10, 64)
	if err == nil {
		filerServer = host
	}

	return
}

func CanonicalizeETag(etag string) string {
	canonicalETag := strings.TrimPrefix(etag, "\"")
	return strings.TrimSuffix(canonicalETag, "\"")
}

// ETagListMatches reports whether an If-Match or If-None-Match value, "*" or a
// list of entity tags, matches etag (RFC 9110 13.1.1, 13.1.2). If-Match uses the
// strong comparison, so a weak tag never matches; If-None-Match the weak one.
func ETagListMatches(header, etag string, weak bool) bool {
	if strings.TrimSpace(header) == "*" {
		return true
	}
	for _, tag := range SplitETagList(header) {
		tag = strings.TrimSpace(tag)
		if strings.HasPrefix(tag, "W/") {
			if !weak {
				continue
			}
			tag = tag[len("W/"):]
		}
		// Empty list elements are ignored, so padding like a trailing
		// comma cannot match an empty opaque tag.
		if tag == "" {
			continue
		}
		if CanonicalizeETag(tag) == CanonicalizeETag(etag) {
			return true
		}
	}
	return false
}

// SplitETagList splits an entity-tag list on the commas between tags; a comma
// inside a quoted opaque tag does not split.
func SplitETagList(header string) []string {
	var tags []string
	start, quoted := 0, false
	for i := 0; i < len(header); i++ {
		switch header[i] {
		case '"':
			quoted = !quoted
		case ',':
			if !quoted {
				tags = append(tags, header[start:i])
				start = i + 1
			}
		}
	}
	return append(tags, header[start:])
}
