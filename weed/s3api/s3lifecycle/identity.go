package s3lifecycle

import (
	"crypto/sha256"
	"sort"
	"strconv"
)

// HashExtended is the canonical fingerprint of an entry's Extended map for
// use in lifecycle identity-CAS. Length-prefixed so a forged tag value
// can't collide with a legitimate multi-tag map. Returns nil for empty.
//
// Both sides of the LifecycleDelete CAS — the worker that captures the
// schedule-time identity and the server that re-fetches the live entry —
// must call this same function so the bytes match exactly.
func HashExtended(ext map[string][]byte) []byte {
	if len(ext) == 0 {
		return nil
	}
	keys := make([]string, 0, len(ext))
	for k := range ext {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, k := range keys {
		h.Write([]byte(strconv.Itoa(len(k))))
		h.Write([]byte{':'})
		h.Write([]byte(k))
		v := ext[k]
		h.Write([]byte(strconv.Itoa(len(v))))
		h.Write([]byte{':'})
		h.Write(v)
	}
	return h.Sum(nil)
}
