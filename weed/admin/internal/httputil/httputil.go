package httputil

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

const MaxJSONBodyBytes = 1 << 20

func NewJSONMaxReader(w http.ResponseWriter, r *http.Request) io.Reader {
	return http.MaxBytesReader(w, r.Body, MaxJSONBodyBytes)
}

func DecodeJSONBody(r io.Reader, v interface{}) error {
	decoder := json.NewDecoder(r)
	return decoder.Decode(v)
}

func WriteJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if payload == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		glog.Errorf("failed to encode JSON response (status=%d, payload=%T): %v", status, payload, err)
	}
}

func WriteJSONError(w http.ResponseWriter, status int, message string) {
	WriteJSON(w, status, map[string]string{"error": message})
}

func DefaultQuery(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
