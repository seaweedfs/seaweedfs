package replication

import (
	"errors"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/replication/sink"
)

func TestCheckDigestsMatch(t *testing.T) {
	cases := []struct {
		a, b   string
		refuse bool
	}{
		{"md5", "md5", false},
		{"sha256", "sha256", false},
		{"sha256", "md5", true},
		{"md5", "sha256", true},
		{"", "sha256", false},
		{"md5", "", false},
	}
	for _, c := range cases {
		err := CheckDigestsMatch("a:8888", c.a, "b:8888", c.b)
		if (err != nil) != c.refuse || IsDigestMismatch(err) != c.refuse {
			t.Errorf("CheckDigestsMatch(%q, %q) = %v, want refused as a mismatch %v", c.a, c.b, err, c.refuse)
		}
	}
}

// fakeSink is a ReplicationSink that writes nowhere.
type fakeSink struct{ sink.ReplicationSink }

// fakeFilerSink is a sink writing into a filer that reports digest.
type fakeFilerSink struct {
	fakeSink
	digest string
	err    error
}

func (f fakeFilerSink) TargetFiler() string { return "target:8888" }

func (f fakeFilerSink) TargetDigest() (string, error) { return f.digest, f.err }

func TestCheckSinkDigest(t *testing.T) {
	source := func(digest string, err error) func() (string, error) {
		return func() (string, error) { return digest, err }
	}
	cases := []struct {
		name     string
		source   func() (string, error)
		sink     sink.ReplicationSink
		refuse   bool
		mismatch bool
	}{
		{"non-filer sink is not checked", source("", errors.New("unreachable")), fakeSink{}, false, false},
		{"same digest", source("sha256", nil), fakeFilerSink{digest: "sha256"}, false, false},
		{"different digests", source("sha256", nil), fakeFilerSink{digest: "md5"}, true, true},
		{"target predates the setting", source("md5", nil), fakeFilerSink{digest: ""}, false, false},
		{"source unreachable", source("", errors.New("unreachable")), fakeFilerSink{digest: "md5"}, true, false},
		{"target unreachable", source("md5", nil), fakeFilerSink{err: errors.New("unreachable")}, true, false},
	}
	for _, c := range cases {
		err := CheckSinkDigest("source:8888", c.source, c.sink)
		if (err != nil) != c.refuse || IsDigestMismatch(err) != c.mismatch {
			t.Errorf("%s: CheckSinkDigest = %v, want refused %v, as a mismatch %v", c.name, err, c.refuse, c.mismatch)
		}
	}
}
