package posixlock

import (
	"math"
	"reflect"
	"testing"
)

func TestMarshalEmptyIsNil(t *testing.T) {
	s := &Set{}
	if b := s.Marshal(); b != nil {
		t.Fatalf("empty set should marshal to nil, got %d bytes", len(b))
	}
	got, err := Unmarshal(nil)
	if err != nil {
		t.Fatalf("unmarshal nil: %v", err)
	}
	if !got.Empty() {
		t.Fatal("unmarshal nil should be an empty set")
	}
}

func TestMarshalRoundTrip(t *testing.T) {
	s := &Set{}
	// A spread of fields: EOF range, both namespaces, two sessions, distinct pids.
	mustAcquire(t, s, Range{Start: 0, End: 99, Type: Write, Sid: 1, Owner: 7, Pid: 10})
	mustAcquire(t, s, Range{Start: 200, End: math.MaxUint64, Type: Read, Sid: 2, Owner: 7, Pid: 20})
	mustAcquire(t, s, Range{Start: 0, End: math.MaxUint64, Type: Write, Sid: 2, Owner: 9, Pid: 30, IsFlock: true})

	b := s.Marshal()
	got, err := Unmarshal(b)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(got.Locks(), s.Locks()) {
		t.Fatalf("round-trip mismatch:\n got %+v\nwant %+v", got.Locks(), s.Locks())
	}
}

func TestUnmarshalRejectsBadVersion(t *testing.T) {
	b := (&Set{locks: []Range{{Start: 0, End: 1, Type: Write, Owner: 1}}}).Marshal()
	b[0] = 0xFF
	if _, err := Unmarshal(b); err == nil {
		t.Fatal("expected an error on unknown version")
	}
}

func TestUnmarshalRejectsLengthMismatch(t *testing.T) {
	b := (&Set{locks: []Range{{Start: 0, End: 1, Type: Write, Owner: 1}}}).Marshal()
	if _, err := Unmarshal(b[:len(b)-3]); err == nil {
		t.Fatal("expected an error on truncated record")
	}
}

// A decoded set must behave identically — conflict detection still fires.
func TestRoundTripPreservesConflict(t *testing.T) {
	s := &Set{}
	mustAcquire(t, s, Range{Start: 0, End: 99, Type: Write, Sid: 1, Owner: 1, Pid: 10})

	decoded, err := Unmarshal(s.Marshal())
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, granted := decoded.Acquire(Range{Start: 50, End: 149, Type: Write, Sid: 2, Owner: 1, Pid: 20}); granted {
		t.Fatal("decoded set should still report the conflict")
	}
	// Same owner across the wire is still the same owner.
	if _, granted := decoded.Acquire(Range{Start: 0, End: 99, Type: Read, Sid: 1, Owner: 1, Pid: 10}); !granted {
		t.Fatal("decoded set should still recognize the same (sid,owner)")
	}
}
