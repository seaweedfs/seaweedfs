package iceberg

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
)

// The catch-all answers anything unrouted, so a path that reaches it is a
// route that does not exist.
func TestRegisteredRoutesReachAHandler(t *testing.T) {
	router := mux.NewRouter().SkipClean(true)
	(&Server{}).RegisterRoutes(router)

	cases := []struct {
		method string
		target string
	}{
		{http.MethodPost, "/v1/views/rename"},
		{http.MethodPost, "/v1/warehouse/views/rename"},
		{http.MethodPost, "/v1/namespaces/ns/tables/t/metrics"},
		{http.MethodPost, "/v1/warehouse/namespaces/ns/tables/t/metrics"},
	}

	for _, tc := range cases {
		var match mux.RouteMatch
		if !router.Match(httptest.NewRequest(tc.method, tc.target, nil), &match) {
			t.Errorf("%s %s matched no route", tc.method, tc.target)
			continue
		}
		if match.MatchErr != nil {
			t.Errorf("%s %s: %v", tc.method, tc.target, match.MatchErr)
			continue
		}
		if tmpl, err := match.Route.GetPathTemplate(); err == nil && tmpl == "/" {
			t.Errorf("%s %s fell through to the catch-all", tc.method, tc.target)
		}
	}
}
