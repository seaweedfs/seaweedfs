package s3api

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
	"time"
)

type mimeType string

const (
	mimeNone mimeType = ""
	mimeJSON mimeType = "application/json"
	mimeXML  mimeType = "application/xml"
)

func setCommonHeaders(w http.ResponseWriter) {
	w.Header().Set("x-amz-request-id", fmt.Sprintf("%d", time.Now().UnixNano()))
	w.Header().Set("Accept-Ranges", "bytes")
}

// Encodes the response headers into XML format.
func encodeResponse(response interface{}) []byte {
	var bytesBuffer bytes.Buffer
	bytesBuffer.WriteString(xml.Header)
	e := xml.NewEncoder(&bytesBuffer)
	e.Encode(response)
	return bytesBuffer.Bytes()
}
