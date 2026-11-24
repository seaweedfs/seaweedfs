package operation

import (
	"bytes"
	"context"
	"crypto/fips140"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"testing"
)

// TestUploadChunksUseTargetDigest checks that ChunkedUploadOption.Digest, not
// this process's digest mode, decides the content and chunk digests: a writer
// into a sha256 filer computes truncated SHA-256 even in an md5 process, and
// the chunk upload carries the digest name so no MD5 Content-MD5 is sent.
func TestUploadChunksUseTargetDigest(t *testing.T) {
	if fips140.Enforced() {
		t.Skip("GODEBUG=fips140=only refuses digest md5")
	}
	payload := []byte("chunk written into a filer of another digest")
	sha := sha256.Sum256(payload)
	md := md5.Sum(payload)
	for _, c := range []struct {
		digest string
		want   []byte
	}{
		{"sha256", sha[:md5.Size]},
		{"md5", md[:]},
	} {
		var uploadedDigest, uploadedMd5 string
		result, err := uploadReaderInChunks(context.Background(), bytes.NewReader(payload), &ChunkedUploadOption{
			ChunkSize: 65536,
			Digest:    c.digest,
			AssignFunc: func(context.Context, int, uint64) (*VolumeAssignRequest, *AssignResult, error) {
				return nil, &AssignResult{Fid: "3,01637037d6", Url: "unused"}, nil
			},
			UploadFunc: func(_ context.Context, data []byte, opt *UploadOption) (*UploadResult, error) {
				uploadedDigest, uploadedMd5 = opt.Digest, opt.Md5
				return &UploadResult{Size: uint32(len(data)), ContentMd5: opt.Md5}, nil
			},
		}, func([]byte, *UploadOption) (*preparedUpload, error) {
			t.Fatal("custom uploader must receive original bytes")
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		wantB64 := base64.StdEncoding.EncodeToString(c.want)
		if uploadedDigest != c.digest || uploadedMd5 != wantB64 {
			t.Errorf("digest %s: chunk upload carried digest %q and Md5 %s, want %q and %s", c.digest, uploadedDigest, uploadedMd5, c.digest, wantB64)
		}
		if got := result.Md5Hash.Sum(nil); !bytes.Equal(got, c.want) {
			t.Errorf("digest %s: content digest %x, want %x", c.digest, got, c.want)
		}
		if len(result.FileChunks) != 1 || result.FileChunks[0].ETag != wantB64 {
			t.Errorf("digest %s: chunk ETag %v, want %s", c.digest, result.FileChunks, wantB64)
		}
	}
}
