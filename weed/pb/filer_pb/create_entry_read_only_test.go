package filer_pb

import (
	"context"
	"errors"
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
