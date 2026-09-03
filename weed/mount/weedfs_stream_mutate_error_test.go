package mount

import (
	"errors"
	"syscall"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// The filer reports a failed create twice: a generic errno at the top level and
// a structured code in the nested CreateEntryResponse. doUnary must not consume
// the response on the generic one, or the sentinel never reaches Mkdir and a
// lost create race surfaces as EIO instead of EEXIST.
func TestHasCreateResponse(t *testing.T) {
	cases := []struct {
		name string
		resp *filer_pb.StreamMutateEntryResponse
		want bool
	}{
		{
			name: "create carrying a nested response",
			resp: &filer_pb.StreamMutateEntryResponse{
				Response: &filer_pb.StreamMutateEntryResponse_CreateResponse{
					CreateResponse: &filer_pb.CreateEntryResponse{
						Error:     "entry already exists",
						ErrorCode: filer_pb.FilerError_ENTRY_ALREADY_EXISTS,
					},
				},
			},
			want: true,
		},
		{
			name: "create wrapper with no nested response",
			resp: &filer_pb.StreamMutateEntryResponse{
				Response: &filer_pb.StreamMutateEntryResponse_CreateResponse{},
			},
			want: false,
		},
		{
			name: "a different mutation",
			resp: &filer_pb.StreamMutateEntryResponse{
				Response: &filer_pb.StreamMutateEntryResponse_DeleteResponse{
					DeleteResponse: &filer_pb.DeleteEntryResponse{Error: "boom"},
				},
			},
			want: false,
		},
		{
			name: "no response at all",
			resp: &filer_pb.StreamMutateEntryResponse{},
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := hasCreateResponse(tc.resp); got != tc.want {
				t.Fatalf("hasCreateResponse = %v, want %v", got, tc.want)
			}
		})
	}
}

// CreateEntry turns the nested code into the sentinel Mkdir matches on.
func TestStreamMutateCreateEntryUnwrapsAlreadyExists(t *testing.T) {
	resp := &filer_pb.StreamMutateEntryResponse{
		Response: &filer_pb.StreamMutateEntryResponse_CreateResponse{
			CreateResponse: &filer_pb.CreateEntryResponse{
				Error:     "/dir/name: entry already exists",
				ErrorCode: filer_pb.FilerError_ENTRY_ALREADY_EXISTS,
			},
		},
	}
	_, err := createEntryFromResponse(resp, &filer_pb.CreateEntryRequest{
		Directory: "/dir",
		Entry:     &filer_pb.Entry{Name: "name"},
	})
	if !errors.Is(err, filer_pb.ErrEntryAlreadyExists) {
		t.Fatalf("err = %v, want it to wrap ErrEntryAlreadyExists", err)
	}
}

// Any other create failure keeps the generic errno rather than inventing one.
func TestStreamMutateCreateEntryKeepsGenericFailure(t *testing.T) {
	resp := &filer_pb.StreamMutateEntryResponse{
		Response: &filer_pb.StreamMutateEntryResponse_CreateResponse{
			CreateResponse: &filer_pb.CreateEntryResponse{Error: "store unavailable"},
		},
	}
	_, err := createEntryFromResponse(resp, &filer_pb.CreateEntryRequest{
		Directory: "/dir",
		Entry:     &filer_pb.Entry{Name: "name"},
	})
	var sme *streamMutateError
	if !errors.As(err, &sme) || sme.Errno() != syscall.EIO {
		t.Fatalf("err = %v, want a streamMutateError with EIO", err)
	}
}
