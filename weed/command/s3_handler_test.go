package command

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
)

// A raw ';' pair must not select a Queries() route that the handler, reading
// the escaped query, does not see.
func TestS3HandlerEscapesSemicolonsBeforeRouting(t *testing.T) {
	router := mux.NewRouter().SkipClean(true)
	var served string
	var hasUploads bool
	router.Methods(http.MethodGet).Path("/{bucket}").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served, hasUploads = "uploads", r.URL.Query().Has("uploads")
	}).Queries("uploads", "")
	router.Methods(http.MethodGet).Path("/{bucket}").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served, hasUploads = "list", r.URL.Query().Has("uploads")
	})

	for query, want := range map[string]string{"uploads": "uploads", "uploads;x=1": "list", "x=1;uploads": "list"} {
		s3Handler(router, nil).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/bkt?"+query, nil))
		assert.Equal(t, want, served, query)
		assert.Equal(t, want == "uploads", hasUploads, query)
	}
}
