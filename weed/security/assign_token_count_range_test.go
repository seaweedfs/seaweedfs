package security

import (
	"strconv"
	"testing"
	"time"
)

func TestVerifyAssignJwtAcceptsOnlyCanonicalAllocatedCountSuffixes(t *testing.T) {
	key := SigningKey("assignment-count-key-0123456789")
	base := "3,0100000011"
	token := GenJwtForAssign(key, time.Hour, base, "session-1", 3, "")

	for _, tc := range []struct {
		name string
		fid  string
		want bool
	}{
		{name: "base", fid: base, want: true},
		{name: "first suffix", fid: base + "_1", want: true},
		{name: "last allocated suffix", fid: base + "_2", want: true},
		{name: "count suffix", fid: base + "_3"},
		{name: "zero suffix", fid: base + "_0"},
		{name: "leading zero", fid: base + "_01"},
		{name: "plus sign", fid: base + "_+1"},
		{name: "non-digit suffix", fid: base + "_x"},
		{name: "empty suffix", fid: base + "_"},
		{name: "int32 overflow", fid: base + "_2147483648"},
		{name: "different base", fid: "3,0100000012"},
		{name: "structured first allocation", fid: "3,0200000011", want: true},
		{name: "structured last allocation", fid: "3,0300000011", want: true},
		{name: "structured count boundary", fid: "3,0400000011"},
		{name: "different volume", fid: "4,0200000011"},
		{name: "noncanonical numeric id", fid: "3,000200000011"},
		{name: "uint64 suffix overflow", fid: base + "_18446744073709551616"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := VerifyAssignJwt(key, nil, token, tc.fid, "session-1", 0, ""); got != tc.want {
				t.Fatalf("VerifyAssignJwt(%q) = %v, want %v", tc.fid, got, tc.want)
			}
		})
	}
}

func TestVerifyAssignJwtRejectsInvalidAssignmentCounts(t *testing.T) {
	key := SigningKey("assignment-invalid-count-key-0123456789")
	base := "3,0100000011"
	for _, count := range []int32{0, -1} {
		t.Run(strconv.FormatInt(int64(count), 10), func(t *testing.T) {
			token := signAssignmentClaims(t, key, SeaweedAssignClaims{
				Fid:              base,
				Count:            count,
				RegisteredClaims: jwtRegisteredClaims("session-1", time.Now().Add(time.Hour)),
			})
			if VerifyAssignJwt(key, nil, token, base, "session-1", 0, "") {
				t.Fatal("assignment proof with invalid count was accepted")
			}
		})
	}
}

func TestVerifyAssignJwtAllocationCannotWrapNeedleID(t *testing.T) {
	key := SigningKey("assignment-overflow-test-key")
	const base = "3,ffffffffffffffff00000011"
	token := GenJwtForAssign(key, time.Hour, base, "session-1", 2, "")
	if !VerifyAssignJwt(key, nil, token, base, "session-1", 0, "") {
		t.Fatal("base allocation was refused")
	}
	for _, fid := range []string{base + "_1", "3,0000000011"} {
		if VerifyAssignJwt(key, nil, token, fid, "session-1", 0, "") {
			t.Fatalf("overflowed allocation %s was accepted", fid)
		}
	}
}
