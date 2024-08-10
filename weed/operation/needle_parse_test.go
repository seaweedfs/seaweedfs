package operation

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type MockClient struct {
	needleHandling func(n *needle.Needle, originalSize int, e error)
}

func (m *MockClient) Do(req *http.Request) (*http.Response, error) {
	n, originalSize, _, err := needle.CreateNeedleFromRequest(req, false, 1024*1024, &bytes.Buffer{})
	if m.needleHandling != nil {
		m.needleHandling(n, originalSize, err)
	}
	return &http.Response{
		StatusCode: http.StatusNoContent,
	}, io.EOF
}

/*

The mime type is always the value passed in.

Compress or not depends on the content detection, file name extension, and compression ratio.

If the content is already compressed, need to know the content size.

*/

func TestCreateNeedleFromRequest(t *testing.T) {
	mockClient := &MockClient{}
	uploader := newUploader(mockClient)

	{
		mockClient.needleHandling = func(n *needle.Needle, originalSize int, err error) {
			assert.Equal(t, nil, err, "upload: %v", err)
			assert.Equal(t, "", string(n.Mime), "mime detection failed: %v", string(n.Mime))
			assert.Equal(t, true, n.IsCompressed(), "this should be compressed")
			assert.Equal(t, true, util.IsGzippedContent(n.Data), "this should be gzip")
			fmt.Printf("needle: %v, originalSize: %d\n", n, originalSize)
		}
		uploadOption := &UploadOption{
			UploadUrl:         "http://localhost:8080/389,0f084d17353afda0",
			Filename:          "t.txt",
			Cipher:            false,
			IsInputCompressed: false,
			MimeType:          "",
			PairMap:           nil,
			Jwt:               "",
		}
		uploadResult, err, data := uploader.Upload(bytes.NewReader([]byte(textContent)), uploadOption)
		if len(data) != len(textContent) {
			t.Errorf("data actual %d expected %d", len(data), len(textContent))
		}
		if err != nil {
			fmt.Printf("err: %v\n", err)
		}
		fmt.Printf("uploadResult: %+v\n", uploadResult)
	}

	{
		mockClient.needleHandling = func(n *needle.Needle, originalSize int, err error) {
			assert.Equal(t, nil, err, "upload: %v", err)
			assert.Equal(t, "text/plain", string(n.Mime), "mime detection failed: %v", string(n.Mime))
			assert.Equal(t, true, n.IsCompressed(), "this should be compressed")
			assert.Equal(t, true, util.IsGzippedContent(n.Data), "this should be gzip")
			fmt.Printf("needle: %v, dataSize:%d originalSize:%d\n", n, len(n.Data), originalSize)
		}
		gzippedData, _ := util.GzipData([]byte(textContent))
		uploadOption := &UploadOption{
			UploadUrl:         "http://localhost:8080/389,0f084d17353afda0",
			Filename:          "t.txt",
			Cipher:            false,
			IsInputCompressed: true,
			MimeType:          "text/plain",
			PairMap:           nil,
			Jwt:               "",
		}
		uploader.Upload(bytes.NewReader(gzippedData), uploadOption)
	}

	/*
		{
			mc.needleHandling = func(n *needle.Needle, originalSize int, err error) {
				assert.Equal(t, nil, err, "upload: %v", err)
				assert.Equal(t, "text/plain", string(n.Mime), "mime detection failed: %v", string(n.Mime))
				assert.Equal(t, true, n.IsCompressed(), "this should be compressed")
				assert.Equal(t, true, util.IsZstdContent(n.Data), "this should be zstd")
				fmt.Printf("needle: %v, dataSize:%d originalSize:%d\n", n, len(n.Data), originalSize)
			}
			zstdData, _ := util.ZstdData([]byte(textContent))
			Upload("http://localhost:8080/389,0f084d17353afda0", "t.txt", false, bytes.NewReader(zstdData), true, "text/plain", nil, "")
		}

		{
			mc.needleHandling = func(n *needle.Needle, originalSize int, err error) {
				assert.Equal(t, nil, err, "upload: %v", err)
				assert.Equal(t, "application/zstd", string(n.Mime), "mime detection failed: %v", string(n.Mime))
				assert.Equal(t, false, n.IsCompressed(), "this should not be compressed")
				assert.Equal(t, true, util.IsZstdContent(n.Data), "this should still be zstd")
				fmt.Printf("needle: %v, dataSize:%d originalSize:%d\n", n, len(n.Data), originalSize)
			}
			zstdData, _ := util.ZstdData([]byte(textContent))
			Upload("http://localhost:8080/389,0f084d17353afda0", "t.txt", false, bytes.NewReader(zstdData), false, "application/zstd", nil, "")
		}
	*/

}

var textContent = `Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

   * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
   * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
   * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
`
