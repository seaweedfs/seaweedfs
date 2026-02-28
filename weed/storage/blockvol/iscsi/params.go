package iscsi

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// iSCSI key-value text parameters (RFC 7143, Section 6).
// Parameters are encoded as "Key=Value\0" in the data segment.

var (
	ErrMalformedParam = errors.New("iscsi: malformed parameter (missing '=')")
	ErrEmptyKey       = errors.New("iscsi: empty parameter key")
	ErrDuplicateKey   = errors.New("iscsi: duplicate parameter key")
)

// Params is an ordered list of iSCSI key-value parameters.
// Order matters for negotiation, so we use a slice rather than a map.
type Params struct {
	items []paramItem
}

type paramItem struct {
	key   string
	value string
}

// ParseParams decodes "Key=Value\0Key=Value\0..." from raw bytes.
// Empty trailing segments (from a trailing \0) are ignored.
// Returns ErrDuplicateKey if the same key appears more than once.
func ParseParams(data []byte) (*Params, error) {
	p := &Params{}
	if len(data) == 0 {
		return p, nil
	}

	seen := make(map[string]bool)
	s := string(data)

	// Split by null separator
	parts := strings.Split(s, "\x00")
	for _, part := range parts {
		if part == "" {
			continue // trailing null or empty
		}
		idx := strings.IndexByte(part, '=')
		if idx < 0 {
			return nil, fmt.Errorf("%w: %q", ErrMalformedParam, part)
		}
		key := part[:idx]
		if key == "" {
			return nil, ErrEmptyKey
		}
		value := part[idx+1:]

		if seen[key] {
			return nil, fmt.Errorf("%w: %q", ErrDuplicateKey, key)
		}
		seen[key] = true
		p.items = append(p.items, paramItem{key: key, value: value})
	}

	return p, nil
}

// Encode serializes parameters to "Key=Value\0" format.
func (p *Params) Encode() []byte {
	if len(p.items) == 0 {
		return nil
	}
	var b strings.Builder
	for _, item := range p.items {
		b.WriteString(item.key)
		b.WriteByte('=')
		b.WriteString(item.value)
		b.WriteByte(0)
	}
	return []byte(b.String())
}

// Get returns the value for a key, or ("", false) if not present.
func (p *Params) Get(key string) (string, bool) {
	for _, item := range p.items {
		if item.key == key {
			return item.value, true
		}
	}
	return "", false
}

// Set adds or replaces a parameter. If the key already exists, its value
// is updated in place. Otherwise, the key is appended.
func (p *Params) Set(key, value string) {
	for i, item := range p.items {
		if item.key == key {
			p.items[i].value = value
			return
		}
	}
	p.items = append(p.items, paramItem{key: key, value: value})
}

// Del removes a key if present.
func (p *Params) Del(key string) {
	for i, item := range p.items {
		if item.key == key {
			p.items = append(p.items[:i], p.items[i+1:]...)
			return
		}
	}
}

// Keys returns all keys in order.
func (p *Params) Keys() []string {
	keys := make([]string, len(p.items))
	for i, item := range p.items {
		keys[i] = item.key
	}
	return keys
}

// Len returns the number of parameters.
func (p *Params) Len() int { return len(p.items) }

// Each iterates over all key-value pairs in order.
func (p *Params) Each(fn func(key, value string)) {
	for _, item := range p.items {
		fn(item.key, item.value)
	}
}

// --- Negotiation helpers ---

// NegotiateNumber applies the iSCSI numeric negotiation rule:
// for "min" semantics (e.g., MaxRecvDataSegmentLength), return min(offer, ours).
// For "max" semantics (e.g., MaxBurstLength), return min(offer, ours).
// Both directions clamp to the smaller value, so min() is the general rule.
func NegotiateNumber(offered string, ours int, min, max int) (int, error) {
	v, err := strconv.Atoi(offered)
	if err != nil {
		return 0, fmt.Errorf("iscsi: invalid numeric value %q: %w", offered, err)
	}
	// Clamp to valid range
	if v < min {
		v = min
	}
	if v > max {
		v = max
	}
	// Standard negotiation: result is min(offer, ours)
	if ours < v {
		return ours, nil
	}
	return v, nil
}

// NegotiateBool applies boolean negotiation (AND semantics per RFC 7143).
// Result is true only if both sides agree to true.
func NegotiateBool(offered string, ours bool) (bool, error) {
	switch offered {
	case "Yes":
		return ours, nil
	case "No":
		return false, nil
	default:
		return false, fmt.Errorf("iscsi: invalid boolean value %q", offered)
	}
}

// BoolStr returns "Yes" or "No" for a boolean value.
func BoolStr(v bool) string {
	if v {
		return "Yes"
	}
	return "No"
}

// NewParams creates a new empty Params.
func NewParams() *Params {
	return &Params{}
}
