package util

import (
	"path"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

type FullPath string

// invalidUTF8Replacement is the single-byte replacement used everywhere a name
// or path from an untrusted source (kernel FUSE input, external clients, store
// imports) may contain bytes that are not valid UTF-8. Proto3 `string` fields
// require valid UTF-8, so any such bytes must be substituted before the value
// enters a gRPC request; otherwise marshaling fails for the whole RPC.
//
// '_' is URL-safe: these sanitized strings also flow into HTTP URLs
// (volume-server uploads, filer HTTP API, S3/WebDAV gateways). Using '?'
// would cause it to be interpreted as the query-string delimiter the first
// time the name lands in a URL and split the path.
const invalidUTF8Replacement = "_"

// SanitizeUTF8Name replaces every invalid-UTF-8 byte in s with
// invalidUTF8Replacement. For the common, valid-UTF-8 case the input is
// returned unchanged with no allocation. Use this for any byte sequence
// that will be assigned to a proto string field (names, paths) from an
// untrusted source; centralising the replacement keeps the chosen character
// consistent across the codebase.
func SanitizeUTF8Name(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	return strings.ToValidUTF8(s, invalidUTF8Replacement)
}

func NewFullPath(dir, name string) FullPath {
	name = strings.TrimSuffix(name, "/")
	return FullPath(dir).Child(name)
}

func (fp FullPath) DirAndName() (string, string) {
	dir, name := filepath.Split(string(fp))
	name = SanitizeUTF8Name(name)
	if dir == "/" {
		return dir, name
	}
	if len(dir) < 1 {
		return "/", ""
	}
	return dir[:len(dir)-1], name
}

// Name returns the last path component, with any invalid-UTF-8 bytes replaced
// via SanitizeUTF8Name so the result is always safe to place in a proto
// string field or HTTP URL.
func (fp FullPath) Name() string {
	_, name := filepath.Split(string(fp))
	return SanitizeUTF8Name(name)
}

// Sanitized returns the full path with every invalid-UTF-8 byte — in any
// component, not just the last — replaced via SanitizeUTF8Name. Use this
// before assigning the path to a proto string field (e.g. Directory,
// AssignVolumeRequest.Path) when the path may have been produced from
// sources that do not enforce UTF-8 (cache populated from an external
// store, legacy metadata, shell traversals of existing filer entries).
func (fp FullPath) Sanitized() string {
	return SanitizeUTF8Name(string(fp))
}

func (fp FullPath) IsLongerFileName(maxFilenameLength uint32) bool {
	if maxFilenameLength == 0 {
		return false
	}
	return uint32(len([]byte(fp.Name()))) > maxFilenameLength
}

func (fp FullPath) Child(name string) FullPath {
	dir := string(fp)
	noPrefix := name
	if strings.HasPrefix(name, "/") {
		noPrefix = name[1:]
	}
	if strings.HasSuffix(dir, "/") {
		return FullPath(dir + noPrefix)
	}
	return FullPath(dir + "/" + noPrefix)
}

// AsInode an in-memory only inode representation
func (fp FullPath) AsInode(unixTime int64) uint64 {
	inode := uint64(HashStringToLong(string(fp)))
	inode = inode + uint64(unixTime)*37
	return inode
}

// split, but skipping the root
func (fp FullPath) Split() []string {
	if fp == "" || fp == "/" {
		return []string{}
	}
	return strings.Split(string(fp)[1:], "/")
}

func Join(names ...string) string {
	return filepath.ToSlash(filepath.Join(names...))
}

func JoinPath(names ...string) FullPath {
	return FullPath(Join(names...))
}

func (fp FullPath) IsUnder(other FullPath) bool {
	if other == "/" {
		return true
	}
	return strings.HasPrefix(string(fp), string(other)+"/")
}

// IsEqualOrUnder reports whether candidate is equal to or a descendant of
// other using proper directory boundaries (not a plain string prefix check).
// Empty strings always return false.
func IsEqualOrUnder(candidate, other string) bool {
	candidatePath := NormalizePath(candidate)
	otherPath := NormalizePath(other)
	if candidatePath == "" || otherPath == "" {
		return false
	}
	return candidatePath == otherPath || candidatePath.IsUnder(otherPath)
}

// NormalizePath trims a trailing slash and returns a FullPath.
// Empty input returns "" (callers should treat this as "no path").
func NormalizePath(p string) FullPath {
	if p == "" {
		return ""
	}
	trimmed := strings.TrimSuffix(p, "/")
	if trimmed == "" {
		return "/"
	}
	return FullPath(trimmed)
}

func StringSplit(separatedValues string, sep string) []string {
	if separatedValues == "" {
		return nil
	}
	return strings.Split(separatedValues, sep)
}

// CleanWindowsPath normalizes Windows-style backslashes to forward slashes.
// This handles paths from Windows clients where paths use backslashes.
func CleanWindowsPath(p string) string {
	return strings.ReplaceAll(p, "\\", "/")
}

// CleanWindowsPathBase normalizes Windows-style backslashes to forward slashes
// and returns the base name of the path.
func CleanWindowsPathBase(p string) string {
	return path.Base(strings.ReplaceAll(p, "\\", "/"))
}
