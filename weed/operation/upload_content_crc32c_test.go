package operation

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestUploadSendsContentCrc32c checks that each kind of chunk upload carries
// the checksum of the bytes the volume server verifies, and that the volume
// server's parser accepts it.
func TestUploadSendsContentCrc32c(t *testing.T) {
	random := make([]byte, 64*1024)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	text := bytes.Repeat([]byte("compressible text chunk "), 4096)
	pregzipped, err := util.GzipData(text)
	if err != nil {
		t.Fatal(err)
	}

	randomMd5 := md5.Sum(random)
	textCrc := needle.ContentCrc32c(text)

	// want "any" stands for the ciphertext's checksum, which only the
	// volume server's acceptance can confirm.
	cases := []struct {
		name   string
		data   []byte
		option UploadOption
		want   string
	}{
		{name: "binary", data: random, option: UploadOption{Filename: "a.bin"}, want: needle.ContentCrc32c(random)},
		{name: "gzipped by the uploader", data: text, option: UploadOption{Filename: "a.txt"}, want: textCrc},
		{name: "gzipped by the caller", data: pregzipped, option: UploadOption{Filename: "a.txt", IsInputCompressed: true}, want: textCrc},
		{name: "with Content-MD5", data: random, option: UploadOption{Filename: "a.bin", Md5: base64.StdEncoding.EncodeToString(randomMd5[:])}, want: needle.ContentCrc32c(random)},
		{name: "ciphered", data: text, option: UploadOption{Filename: "a.txt", Cipher: true}, want: "any"},
		{name: "replica forward", data: pregzipped, option: UploadOption{IsInputCompressed: true, IsReplication: true, ContentCrc32c: textCrc}, want: textCrc},
		{name: "replica forward without one", data: pregzipped, option: UploadOption{IsInputCompressed: true, IsReplication: true}, want: ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var gotCrc string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotCrc = r.Header.Get(needle.ContentCrc32cHeader)
				if _, _, _, err := needle.CreateNeedleFromRequest(r, false, 1<<20, &bytes.Buffer{}); err != nil {
					w.WriteHeader(http.StatusBadRequest)
					_, _ = io.WriteString(w, `{"error":"refused"}`)
					t.Errorf("volume server refused the upload: %v", err)
					return
				}
				w.WriteHeader(http.StatusCreated)
				_, _ = io.WriteString(w, `{}`)
			}))
			defer server.Close()
			option := c.option
			option.UploadUrl = server.URL + "/3,01637037d6"
			option.MaxAttempts = 1
			if _, err := newUploader(server.Client()).UploadData(context.Background(), c.data, &option); err != nil {
				t.Fatalf("UploadData: %v", err)
			}
			if c.want == "any" && gotCrc != "" {
				return
			}
			if gotCrc != c.want {
				t.Fatalf("%s = %q, want %q", needle.ContentCrc32cHeader, gotCrc, c.want)
			}
		})
	}
}
