package handlers

import (
	"io"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/admin/internal/httputil"
)

func writeJSON(w http.ResponseWriter, status int, payload interface{}) {
	httputil.WriteJSON(w, status, payload)
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	httputil.WriteJSONError(w, status, message)
}

func decodeJSONBody(r io.Reader, v interface{}) error {
	return httputil.DecodeJSONBody(r, v)
}

func newJSONMaxReader(w http.ResponseWriter, r *http.Request) io.Reader {
	return httputil.NewJSONMaxReader(w, r)
}

func defaultQuery(value, fallback string) string {
	return httputil.DefaultQuery(value, fallback)
}
