package filer

import (
	"errors"
	"strings"
	"time"
)

type AtimeMode string

const (
	AtimeModeOff      AtimeMode = "off"
	AtimeModeRelatime AtimeMode = "relatime"
	AtimeModeStrict   AtimeMode = "strict"

	DefaultRelatimeThreshold = 24 * time.Hour
)

var ErrInvalidAtimeMode = errors.New("invalid atime mode: expected one of off, relatime, strict")

type AtimePolicy struct {
	Mode              AtimeMode
	RelatimeThreshold time.Duration
	// PathPrefixes restricts atime tracking to entries whose full path
	// begins with one of the listed prefixes. An empty list means atime
	// applies to every path the chosen mode would otherwise update.
	PathPrefixes []string
}

// NewAtimePolicy builds a policy from a mode string, relatime threshold and
// optional path-prefix list. A non-positive threshold falls back to
// DefaultRelatimeThreshold.
func NewAtimePolicy(mode string, threshold time.Duration, pathPrefixes []string) (*AtimePolicy, error) {
	parsed, err := ParseAtimeMode(mode)
	if err != nil {
		return nil, err
	}
	if threshold <= 0 {
		threshold = DefaultRelatimeThreshold
	}
	return &AtimePolicy{
		Mode:              parsed,
		RelatimeThreshold: threshold,
		PathPrefixes:      normalisePathPrefixes(pathPrefixes),
	}, nil
}

// ParseAtimeMode normalises a mode string (case- and whitespace-insensitive),
// treating the empty string as AtimeModeOff and rejecting anything unknown.
func ParseAtimeMode(s string) (AtimeMode, error) {
	switch AtimeMode(strings.ToLower(strings.TrimSpace(s))) {
	case AtimeModeOff, "":
		return AtimeModeOff, nil
	case AtimeModeRelatime:
		return AtimeModeRelatime, nil
	case AtimeModeStrict:
		return AtimeModeStrict, nil
	}
	return "", ErrInvalidAtimeMode
}

// ParsePathPrefixes splits a comma-separated prefix list, trimming whitespace
// and dropping empty entries. Convenience for command-line plumbing.
func ParsePathPrefixes(raw string) []string {
	if raw == "" {
		return nil
	}
	return normalisePathPrefixes(strings.Split(raw, ","))
}

func normalisePathPrefixes(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	for _, p := range in {
		p = strings.TrimSpace(p)
		// Treat "/a/b" and "/a/b/" as the same subtree so a prefix also
		// matches the directory itself. A bare "/" collapses to "" and is
		// dropped, which correctly means "no restriction".
		p = strings.TrimRight(p, "/")
		if p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// AppliesToPath reports whether the policy is active for the given full path,
// combining mode and prefix restrictions. Prefixes are matched at path
// component boundaries so that "/buckets/cache" matches "/buckets/cache/x" but
// not "/buckets/cache_backup/x".
func (p *AtimePolicy) AppliesToPath(fullpath string) bool {
	if p == nil || p.Mode == AtimeModeOff {
		return false
	}
	if len(p.PathPrefixes) == 0 {
		return true
	}
	for _, prefix := range p.PathPrefixes {
		if pathHasPrefix(fullpath, prefix) {
			return true
		}
	}
	return false
}

func pathHasPrefix(fullpath, prefix string) bool {
	if prefix == "" {
		return true
	}
	if !strings.HasPrefix(fullpath, prefix) {
		return false
	}
	if len(fullpath) == len(prefix) {
		return true
	}
	if prefix[len(prefix)-1] == '/' {
		return true
	}
	return fullpath[len(prefix)] == '/'
}

// ShouldUpdate reports whether a candidate access time should be persisted for
// an entry under this policy. The relatime threshold is normalised here too, so
// a policy built via a struct literal with a non-positive threshold still
// debounces using DefaultRelatimeThreshold rather than degrading to strict.
func (p *AtimePolicy) ShouldUpdate(existing Attr, candidate time.Time) bool {
	if p == nil || p.Mode == AtimeModeOff {
		return false
	}
	if candidate.IsZero() {
		return false
	}
	if existing.Atime.IsZero() {
		return true
	}
	if !candidate.After(existing.Atime) {
		return false
	}
	if p.Mode == AtimeModeStrict {
		return true
	}
	if !existing.Mtime.IsZero() && existing.Atime.Before(existing.Mtime) {
		return true
	}
	if !existing.Ctime.IsZero() && existing.Atime.Before(existing.Ctime) {
		return true
	}
	threshold := p.RelatimeThreshold
	if threshold <= 0 {
		threshold = DefaultRelatimeThreshold
	}
	return candidate.Sub(existing.Atime) >= threshold
}
