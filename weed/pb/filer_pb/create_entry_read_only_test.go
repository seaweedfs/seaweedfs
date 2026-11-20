package filer_pb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type createEntryAnswer struct {
	SeaweedFilerClient
	resp *CreateEntryResponse
	err  error
}

func (c *createEntryAnswer) CreateEntry(context.Context, *CreateEntryRequest, ...grpc.CallOption) (*CreateEntryResponse, error) {
	return c.resp, c.err
}

// A read-only verdict must match ErrReadOnly whichever way the filer sends it.
// Callers decide on that whether to retry, and which errno to give back.
func TestCreateEntryWithResponseRecognizesReadOnly(t *testing.T) {
	const legacy = "read only: /buckets/b/ (e.g. bucket over quota)"
	cases := []struct {
		name     string
		answer   *createEntryAnswer
		readOnly bool
	}{
		{"coded response", &createEntryAnswer{resp: &CreateEntryResponse{Error: legacy, ErrorCode: FilerError_READ_ONLY}}, true},
		// Filers before the coded response failed the call; gRPC carries that as Unknown.
		{"legacy unknown status", &createEntryAnswer{err: status.Error(codes.Unknown, legacy)}, true},
		{"bare read only", &createEntryAnswer{err: status.Error(codes.Unknown, "read only")}, true},
		{"other unknown status", &createEntryAnswer{err: status.Error(codes.Unknown, "insert entry: store unavailable")}, false},
		{"read only text, not unknown", &createEntryAnswer{err: status.Error(codes.Unavailable, legacy)}, false},
		{"read only later in the text", &createEntryAnswer{err: status.Error(codes.Unknown, "open /data: read only file system")}, false},
		{"uncoded response", &createEntryAnswer{resp: &CreateEntryResponse{Error: "insert entry: store unavailable"}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := CreateEntryWithResponse(context.Background(), tc.answer, &CreateEntryRequest{
				Directory: "/buckets/b/dir",
				Entry:     &Entry{Name: "f"},
			})
			if err == nil {
				t.Fatal("err = nil, want the filer's refusal")
			}
			if got := errors.Is(err, ErrReadOnly); got != tc.readOnly {
				t.Fatalf("errors.Is(%v, ErrReadOnly) = %v, want %v", err, got, tc.readOnly)
			}
		})
	}
}

// The legacy text names the rule that matched, so the error keeps it.
func TestCreateEntryWithResponseKeepsLegacyReadOnlyText(t *testing.T) {
	const legacy = "read only: /buckets/b/ (e.g. bucket over quota)"
	_, err := CreateEntryWithResponse(context.Background(), &createEntryAnswer{err: status.Error(codes.Unknown, legacy)}, &CreateEntryRequest{
		Directory: "/buckets/b/dir",
		Entry:     &Entry{Name: "f"},
	})
	if want := "CreateEntry /buckets/b/dir/f: " + legacy; err == nil || err.Error() != want {
		t.Fatalf("err = %v, want %q", err, want)
	}
	if strings.Contains(err.Error(), "rpc error") {
		t.Fatalf("err = %v still carries the transport framing", err)
	}
}

// IsFilerVerdict tells the filer's answer apart from a failed call, so that
// retry and connection classifiers never read the path in its message.
func TestIsFilerVerdict(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"read only", fmt.Errorf("CreateEntry /buckets/transport/f: %w", ErrReadOnly), true},
		{"already exists", fmt.Errorf("CreateEntry /d/f: %w", ErrEntryAlreadyExists), true},
		{"precondition failed", fmt.Errorf("CreateEntry /d/f: %w", ErrPreconditionFailed), true},
		{"older filer read only", LegacyReadOnlyError("read only: /buckets/transport/ (e.g. bucket over quota)"), true},
		{"coded assign refusal", AssignVolumeResponseError(&AssignVolumeResponse{Error: "assign volume: read only: /b/", ErrorCode: FilerError_READ_ONLY}), true},
		{"uncoded assign failure", AssignVolumeResponseError(&AssignVolumeResponse{Error: "assign volume: no free volumes"}), false},
		{"transport failure", status.Error(codes.Unavailable, "transport is closing"), false},
		{"plain error", errors.New("read only: not a sentinel"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsFilerVerdict(tc.err); got != tc.want {
				t.Fatalf("IsFilerVerdict(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
