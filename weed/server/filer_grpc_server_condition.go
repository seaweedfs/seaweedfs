package weed_server

import (
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// conditionIsSet reports whether a condition asks for any check at all.
func conditionIsSet(cond *filer_pb.WriteCondition) bool {
	return cond != nil && len(cond.Clauses) > 0
}

// writeConditionSatisfied reports whether the precondition holds against the
// current entry (nil if absent), evaluated under the path lock. Every clause
// must hold (logical AND).
func writeConditionSatisfied(cond *filer_pb.WriteCondition, current *filer.Entry) bool {
	for _, c := range cond.Clauses {
		if !clauseSatisfied(c, current) {
			return false
		}
	}
	return true
}

// clauseSatisfied evaluates one primitive against the current entry. For the
// ETag kinds, etags is a set: IF_ETAG_MATCH holds when the current ETag equals
// any member, IF_ETAG_NOT_MATCH when it equals none. The IF_EXTENDED_* kinds are
// generic guards on an extended attribute used to enforce object-lock (legal
// hold and retention) without S3 knowledge in the filer.
func clauseSatisfied(c *filer_pb.WriteCondition_Clause, current *filer.Entry) bool {
	exists := current != nil
	switch c.Kind {
	case filer_pb.WriteCondition_NONE:
		return true
	case filer_pb.WriteCondition_IF_NOT_EXISTS:
		return !exists
	case filer_pb.WriteCondition_IF_EXISTS:
		return exists
	case filer_pb.WriteCondition_IF_ETAG_MATCH:
		return exists && etagInSet(storedEntryETag(current), c.Etags, c.AllowWeak)
	case filer_pb.WriteCondition_IF_ETAG_NOT_MATCH:
		return !exists || !etagInSet(storedEntryETag(current), c.Etags, c.AllowWeak)
	case filer_pb.WriteCondition_IF_UNMODIFIED_SINCE:
		return !exists || current.Attr.Mtime.Unix() <= c.UnixTime
	case filer_pb.WriteCondition_IF_MODIFIED_SINCE:
		return !exists || current.Attr.Mtime.Unix() > c.UnixTime
	case filer_pb.WriteCondition_IF_EXTENDED_NOT_EQUAL:
		if !exists {
			return true
		}
		v, ok := current.Extended[c.ExtKey]
		return !ok || string(v) != c.ExtValue
	case filer_pb.WriteCondition_IF_EXTENDED_TIME_ELAPSED:
		if !exists {
			return true
		}
		// An optional gate scopes the guard: when gate_key is set, the time check
		// only applies if extended[gate_key] == gate_value. This lets the gateway
		// express governance bypass (enforce retention only for COMPLIANCE mode)
		// without reading the entry — the filer decides under the lock.
		if c.GateKey != "" {
			gv, gok := current.Extended[c.GateKey]
			if !gok || string(gv) != c.GateValue {
				return true
			}
		}
		v, ok := current.Extended[c.ExtKey]
		if !ok {
			return true
		}
		deadline, err := strconv.ParseInt(strings.TrimSpace(string(v)), 10, 64)
		if err != nil {
			// An unparseable retention deadline is treated as still in force, so
			// a malformed attribute fails safe (write blocked) rather than open.
			return false
		}
		return deadline <= time.Now().Unix()
	default:
		// An unrecognized clause kind (e.g. from a newer client) must not be
		// treated as satisfied, which would silently bypass the guard. Fail
		// closed so the write is blocked rather than slipping through.
		return false
	}
}

// etagInSet reports whether stored matches any candidate. A strong comparison
// (allowWeak false) treats a weak ETag as never equal; a weak comparison
// ignores the W/ marker on both sides.
func etagInSet(stored string, candidates []string, allowWeak bool) bool {
	for _, c := range candidates {
		if etagEqual(stored, c, allowWeak) {
			return true
		}
	}
	return false
}

func etagEqual(stored, expected string, allowWeak bool) bool {
	sv, sWeak := canonicalETag(stored)
	ev, eWeak := canonicalETag(expected)
	// RFC 7232 strong comparison: a weak ETag on either side never matches.
	if !allowWeak && (sWeak || eWeak) {
		return false
	}
	return sv == ev
}

// canonicalETag splits off the weak (W/) marker before stripping quotes, so a
// weak ETag like W/"abc" yields ("abc", true).
func canonicalETag(etag string) (value string, weak bool) {
	etag = strings.TrimSpace(etag)
	if strings.HasPrefix(etag, "W/") {
		return strings.Trim(etag[len("W/"):], `"`), true
	}
	return strings.Trim(etag, `"`), false
}

// storedEntryETag mirrors the S3 gateway's ETag precedence (the stored
// Seaweed ETag extended attribute, then the chunk/Md5 fallback) so conditional
// comparisons match what the gateway computes, without coupling the filer to
// S3 request handling.
func storedEntryETag(entry *filer.Entry) string {
	if v, ok := entry.Extended[s3_constants.ExtETagKey]; ok && len(v) > 0 {
		return normalizeETag(string(v))
	}
	return normalizeETag(filer.ETagEntry(entry))
}

func normalizeETag(etag string) string {
	return strings.Trim(etag, `"`)
}
