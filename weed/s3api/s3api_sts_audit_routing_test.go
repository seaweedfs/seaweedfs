package s3api

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every STS route must be wrapped by track(), which is the only thing that
// emits an audit entry for these handlers: the STS responses go out through
// WriteXMLResponse, which never calls PostLog itself. A route registered
// outside track() would therefore mint credentials with no audit trail at all,
// and nothing else in the suite would notice. STS has three routing layers
// (explicit query-param routes, the authenticated POST dispatcher, and the
// anonymous fallback), so a new action is easy to attach to the wrong one.
func TestSTSRoutesEmitAuditEntries(t *testing.T) {
	router := mux.NewRouter()
	s3a := setupRoutingTestServer(t)
	s3a.registerRouter(router)

	cases := []struct {
		name   string
		action string
		params url.Values
		signed bool
		inBody bool
	}{
		{name: "AssumeRole", action: "AssumeRole", signed: true, params: url.Values{
			"RoleArn":         {"arn:aws:iam::" + defaultAccountID + ":role/AuditRole"},
			"RoleSessionName": {"audit-session"},
		}},
		{name: "GetCallerIdentity", action: "GetCallerIdentity", signed: true},
		{name: "GetFederationToken", action: "GetFederationToken", signed: true, params: url.Values{
			"Name": {"audit-user"},
		}},
		{name: "AssumeRoleWithWebIdentity", action: "AssumeRoleWithWebIdentity", params: url.Values{
			"WebIdentityToken": {"not-a-real-token"},
			"RoleArn":          {"arn:aws:iam::" + defaultAccountID + ":role/AuditRole"},
			"RoleSessionName":  {"audit-session"},
		}},
		{name: "AssumeRoleWithLDAPIdentity", action: "AssumeRoleWithLDAPIdentity", params: url.Values{
			"RoleArn":         {"arn:aws:iam::" + defaultAccountID + ":role/AuditRole"},
			"RoleSessionName": {"audit-session"},
			"LDAPUsername":    {"audit-user"},
			"LDAPPassword":    {"audit-password"},
		}},
		// The authenticated POST dispatcher is a separate routing layer from the
		// explicit query-param routes above.
		{name: "AssumeRole via POST body", action: "AssumeRole", signed: true, inBody: true, params: url.Values{
			"RoleArn":         {"arn:aws:iam::" + defaultAccountID + ":role/AuditRole"},
			"RoleSessionName": {"audit-session"},
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			form := url.Values{"Action": {tc.action}, "Version": {"2011-06-15"}}
			for k, vs := range tc.params {
				form[k] = vs
			}

			var req *http.Request
			var body string
			if tc.inBody {
				body = form.Encode()
				req = httptest.NewRequest(http.MethodPost, "http://localhost/", strings.NewReader(body))
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			} else {
				req = httptest.NewRequest(http.MethodPost, "http://localhost/?"+form.Encode(), nil)
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			}
			if tc.signed {
				signRoutingTestRequest(t, req, body, "sts")
			}

			// track() installs both of these itself, but on a request copy this
			// test would never see. Installing them up front shares the
			// underlying pointers so the middleware's writes stay observable.
			req = s3err.EnsureAuditTracking(req)
			req = s3_constants.EnsureIdentityHolder(req)

			rr := httptest.NewRecorder()
			router.ServeHTTP(rr, req)

			require.NotEqual(t, http.StatusNotFound, rr.Code,
				"request did not match an STS route: %s", rr.Body.String())
			assert.True(t, s3err.AuditAlreadyLogged(req),
				"STS route emitted no audit entry - is the route wrapped in track()? status=%d body=%s",
				rr.Code, rr.Body.String())
		})
	}
}
