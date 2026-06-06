package s3api

import (
	"net/http"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

func reqWith(headers map[string]string) *http.Request {
	r, _ := http.NewRequest(http.MethodPut, "/b/o", nil)
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	return r
}

// oneClause returns the single clause of cond, failing if it does not hold
// exactly one.
func oneClause(t *testing.T, cond *filer_pb.WriteCondition) *filer_pb.WriteCondition_Clause {
	t.Helper()
	if cond == nil {
		t.Fatal("expected a condition, got nil")
	}
	if len(cond.Clauses) != 1 {
		t.Fatalf("expected 1 clause, got %d", len(cond.Clauses))
	}
	return cond.Clauses[0]
}

func TestBuildWriteCondition(t *testing.T) {
	t.Run("no headers is unconditional", func(t *testing.T) {
		cond, ok := buildWriteCondition(reqWith(nil))
		if !ok || cond != nil {
			t.Fatalf("want (nil, true), got (%v, %v)", cond, ok)
		}
	})
	t.Run("If-None-Match * to IF_NOT_EXISTS", func(t *testing.T) {
		cond, ok := buildWriteCondition(reqWith(map[string]string{s3_constants.IfNoneMatch: "*"}))
		if !ok {
			t.Fatal("want ok")
		}
		if c := oneClause(t, cond); c.Kind != filer_pb.WriteCondition_IF_NOT_EXISTS {
			t.Fatalf("kind = %v", c.Kind)
		}
	})
	t.Run("If-Match * to IF_EXISTS", func(t *testing.T) {
		cond, ok := buildWriteCondition(reqWith(map[string]string{s3_constants.IfMatch: "*"}))
		if !ok {
			t.Fatal("want ok")
		}
		if c := oneClause(t, cond); c.Kind != filer_pb.WriteCondition_IF_EXISTS {
			t.Fatalf("kind = %v", c.Kind)
		}
	})
	t.Run("If-Match strong etag to IF_ETAG_MATCH", func(t *testing.T) {
		cond, ok := buildWriteCondition(reqWith(map[string]string{s3_constants.IfMatch: `"abc123"`}))
		if !ok {
			t.Fatal("want ok")
		}
		c := oneClause(t, cond)
		if c.Kind != filer_pb.WriteCondition_IF_ETAG_MATCH || len(c.Etags) != 1 || c.Etags[0] != "abc123" {
			t.Fatalf("clause = %+v", c)
		}
	})
	t.Run("If-None-Match strong etag to IF_ETAG_NOT_MATCH", func(t *testing.T) {
		cond, ok := buildWriteCondition(reqWith(map[string]string{s3_constants.IfNoneMatch: `"abc123"`}))
		if !ok {
			t.Fatal("want ok")
		}
		c := oneClause(t, cond)
		if c.Kind != filer_pb.WriteCondition_IF_ETAG_NOT_MATCH || len(c.Etags) != 1 || c.Etags[0] != "abc123" {
			t.Fatalf("clause = %+v", c)
		}
	})
	t.Run("weak etag falls back", func(t *testing.T) {
		if _, ok := buildWriteCondition(reqWith(map[string]string{s3_constants.IfMatch: `W/"abc"`})); ok {
			t.Fatal("weak etag must not take the fast path")
		}
	})
	t.Run("etag list falls back", func(t *testing.T) {
		if _, ok := buildWriteCondition(reqWith(map[string]string{s3_constants.IfMatch: `"a","b"`})); ok {
			t.Fatal("etag list must not take the fast path")
		}
	})
	t.Run("both match and none-match falls back", func(t *testing.T) {
		if _, ok := buildWriteCondition(reqWith(map[string]string{
			s3_constants.IfMatch:     "*",
			s3_constants.IfNoneMatch: "*",
		})); ok {
			t.Fatal("ambiguous combination must not take the fast path")
		}
	})
	t.Run("time-based falls back", func(t *testing.T) {
		if _, ok := buildWriteCondition(reqWith(map[string]string{
			"If-Unmodified-Since": "Wed, 21 Oct 2015 07:28:00 GMT",
		})); ok {
			t.Fatal("time condition must not take the fast path")
		}
	})
}

func TestParseConditionalHeadersAcceptsHTTPDateFormats(t *testing.T) {
	testCases := []struct {
		name   string
		header string
		value  string
	}{
		{
			name:   "If-Modified-Since RFC850",
			header: s3_constants.IfModifiedSince,
			value:  "Sunday, 06-Nov-94 08:49:37 GMT",
		},
		{
			name:   "If-Unmodified-Since ANSIC",
			header: s3_constants.IfUnmodifiedSince,
			value:  "Sun Nov  6 08:49:37 1994",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			r := reqWith(map[string]string{testCase.header: testCase.value})

			headers, errCode := parseConditionalHeaders(r)
			if errCode != s3err.ErrNone {
				t.Fatalf("expected %s to be accepted, got %v", testCase.header, errCode)
			}
			if !headers.isSet {
				t.Fatal("expected conditional headers to be marked set")
			}
		})
	}
}

func TestBuildDeleteCondition(t *testing.T) {
	t.Run("no If-Match is unconditional", func(t *testing.T) {
		cond, ok := buildDeleteCondition(reqWith(nil))
		if !ok || cond != nil {
			t.Fatalf("want (nil, true), got (%v, %v)", cond, ok)
		}
	})
	t.Run("If-Match * to IF_EXISTS", func(t *testing.T) {
		cond, ok := buildDeleteCondition(reqWith(map[string]string{s3_constants.IfMatch: "*"}))
		if !ok {
			t.Fatal("want ok")
		}
		if c := oneClause(t, cond); c.Kind != filer_pb.WriteCondition_IF_EXISTS {
			t.Fatalf("kind = %v", c.Kind)
		}
	})
	t.Run("If-Match etag to IF_ETAG_MATCH", func(t *testing.T) {
		cond, ok := buildDeleteCondition(reqWith(map[string]string{s3_constants.IfMatch: `"e"`}))
		if !ok {
			t.Fatal("want ok")
		}
		if c := oneClause(t, cond); c.Kind != filer_pb.WriteCondition_IF_ETAG_MATCH || c.Etags[0] != "e" {
			t.Fatalf("clause = %+v", c)
		}
	})
	t.Run("weak etag falls back", func(t *testing.T) {
		if _, ok := buildDeleteCondition(reqWith(map[string]string{s3_constants.IfMatch: `W/"e"`})); ok {
			t.Fatal("weak etag must not take the fast path")
		}
	})
}

func TestSingleStrongETag(t *testing.T) {
	cases := []struct {
		in     string
		want   string
		single bool
	}{
		{`"abc"`, "abc", true},
		{` "abc" `, "abc", true},
		{`abc`, "abc", true},
		{`W/"abc"`, "", false},
		{`w/"abc"`, "", false},
		{`"a","b"`, "", false},
	}
	for _, c := range cases {
		got, single := singleStrongETag(c.in)
		if single != c.single || (single && got != c.want) {
			t.Errorf("singleStrongETag(%q) = (%q, %v), want (%q, %v)", c.in, got, single, c.want, c.single)
		}
	}
}

func TestRouteWriteCondition(t *testing.T) {
	// Unconditional routes either way.
	if c, ok := routeWriteCondition(reqWith(nil), false); !ok || c != nil {
		t.Fatalf("overwrite unconditional: got (%v,%v)", c, ok)
	}
	if c, ok := routeWriteCondition(reqWith(nil), true); !ok || c != nil {
		t.Fatalf("unique unconditional: got (%v,%v)", c, ok)
	}
	// An overwrite carries a reducible condition.
	if c, ok := routeWriteCondition(reqWith(map[string]string{s3_constants.IfMatch: `"e"`}), false); !ok || c == nil {
		t.Fatalf("overwrite conditional should route: got (%v,%v)", c, ok)
	}
	// A conditional unique (versioned) write bails to the lock path.
	if _, ok := routeWriteCondition(reqWith(map[string]string{s3_constants.IfMatch: `"e"`}), true); ok {
		t.Fatal("conditional unique write must not route")
	}
	// A non-reducible condition bails regardless.
	if _, ok := routeWriteCondition(reqWith(map[string]string{s3_constants.IfMatch: `W/"e"`}), false); ok {
		t.Fatal("weak etag must not route")
	}
}
