package http

import (
	"net/http"
	"strings"
)

// EscapeSemicolonsInQuery re-encodes raw ';' in the query string. RFC 3986
// allows ';' in a query and AWS treats it as data, but Go's url.ParseQuery
// drops every key=value pair containing one. Presigned URLs carry
// X-Amz-SignedHeaders=content-type%3Bhost; clients that decode the %3B would
// otherwise lose the parameter entirely and fail signature verification.
func EscapeSemicolonsInQuery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.RawQuery, ";") {
			u := *r.URL
			u.RawQuery = strings.ReplaceAll(u.RawQuery, ";", "%3B")
			r2 := new(http.Request)
			*r2 = *r
			r2.URL = &u
			r = r2
		}
		next.ServeHTTP(w, r)
	})
}
