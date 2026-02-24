package handlers

import (
	"encoding/json"
	"io"
	"net/http"
)

const maxJSONBodyBytes = 1 << 20

func writeJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if payload == nil {
		return
	}
	_ = json.NewEncoder(w).Encode(payload)
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func decodeJSONBody(r io.Reader, v interface{}) error {
	decoder := json.NewDecoder(r)
	return decoder.Decode(v)
}

func newJSONMaxReader(w http.ResponseWriter, r *http.Request) io.Reader {
	return http.MaxBytesReader(w, r.Body, maxJSONBodyBytes)
}

func defaultQuery(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
