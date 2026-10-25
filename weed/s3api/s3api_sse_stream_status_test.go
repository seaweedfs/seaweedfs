package s3api

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// the SSE mirror of the plain-path deferred commit: the 200 must not be
// committed until the first fetched-and-decrypted byte is written
func TestS3SSEStreamCommitsStatusOnFirstWrite(t *testing.T) {
	plaintext := []byte("0123456789")
	keyPair := GenerateTestSSECKey(1)

	newSSECRequest := func(rangeHeader string) *http.Request {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		t.Cleanup(cancel)
		r := httptest.NewRequest(http.MethodGet, "/mybucket/dir/obj.bin", nil).WithContext(ctx)
		SetupTestSSECHeaders(r, keyPair)
		if rangeHeader != "" {
			r.Header.Set("Range", rangeHeader)
		}
		return r
	}

	encrypt := func(t *testing.T, r *http.Request) (ciphertext, iv []byte) {
		customerKey, err := ParseSSECHeaders(r)
		require.NoError(t, err)
		encReader, iv, err := CreateSSECEncryptedReader(bytes.NewReader(plaintext), customerKey)
		require.NoError(t, err)
		ciphertext, err = io.ReadAll(encReader)
		require.NoError(t, err)
		return ciphertext, iv
	}

	t.Run("readable SSE-C object still streams a 200 with the plaintext", func(t *testing.T) {
		s3a := newLocalReadFallbackServer(t, startStreamThroughFiler(t, "faketest-sseok", nil))
		r := newSSECRequest("")
		ciphertext, iv := encrypt(t, r)
		entry := &filer_pb.Entry{
			Name:       "obj.bin",
			Attributes: &filer_pb.FuseAttributes{FileSize: uint64(len(plaintext))},
			Content:    ciphertext,
			Extended: map[string][]byte{
				s3_constants.AmzServerSideEncryptionCustomerAlgorithm: []byte("AES256"),
				s3_constants.AmzServerSideEncryptionCustomerKeyMD5:    []byte(keyPair.KeyMD5),
				s3_constants.SeaweedFSSSEIV:                           iv,
			},
		}
		w := httptest.NewRecorder()

		err := s3a.streamFromVolumeServersWithSSE(w, r, entry, s3_constants.SSETypeC, "mybucket", "dir/obj.bin", "")

		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, plaintext, w.Body.Bytes())
		assert.Equal(t, "AES256", w.Header().Get(s3_constants.AmzServerSideEncryptionCustomerAlgorithm))
	})

	unreadableEntry := func(t *testing.T, r *http.Request) *filer_pb.Entry {
		_, iv := encrypt(t, r)
		return &filer_pb.Entry{
			Name:       "obj.bin",
			Attributes: &filer_pb.FuseAttributes{FileSize: uint64(len(plaintext))},
			Chunks:     []*filer_pb.FileChunk{{FileId: "1,0123456789ab", Size: uint64(len(plaintext))}},
			Extended: map[string][]byte{
				s3_constants.AmzServerSideEncryptionCustomerKeyMD5: []byte(keyPair.KeyMD5),
				s3_constants.SeaweedFSSSEIV:                        iv,
			},
		}
	}

	assertNothingCommitted := func(t *testing.T, w *httptest.ResponseRecorder, err error) {
		require.Error(t, err)
		var streamErr *StreamError
		assert.False(t, errors.As(err, &streamErr) && streamErr.ResponseWritten,
			"the caller must still own the error response")
		assert.Zero(t, w.Body.Len(), "no body bytes may precede the failure")
	}

	t.Run("unreadable SSE-C object leaves the response uncommitted", func(t *testing.T) {
		s3a := newLocalReadFallbackServer(t, startStreamThroughFiler(t, "faketest-ssefail", nil))
		r := newSSECRequest("")
		w := httptest.NewRecorder()

		err := s3a.streamFromVolumeServersWithSSE(w, r, unreadableEntry(t, r), s3_constants.SSETypeC, "mybucket", "dir/obj.bin", "")

		assertNothingCommitted(t, w, err)
	})

	t.Run("unreadable SSE-C range leaves the response uncommitted", func(t *testing.T) {
		s3a := newLocalReadFallbackServer(t, startStreamThroughFiler(t, "faketest-sserange", nil))
		r := newSSECRequest("bytes=2-5")
		w := httptest.NewRecorder()

		err := s3a.streamFromVolumeServersWithSSE(w, r, unreadableEntry(t, r), s3_constants.SSETypeC, "mybucket", "dir/obj.bin", "")

		assertNothingCommitted(t, w, err)
	})
}
