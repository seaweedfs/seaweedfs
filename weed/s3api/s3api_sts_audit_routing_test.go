package s3api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stsXMLNamespace appears on every STS response, success or error. IAM and S3
// responses carry a different namespace, so this is what separates "the request
// reached the STS handler" from "it reached some other handler that also
// happened to answer".
const stsXMLNamespace = "https://sts.amazonaws.com/doc/2011-06-15/"

// setupAuditRoutingTestServer builds a server whose STS handler is backed by a
// real STS service, so the routes under test actually execute instead of
// short-circuiting on an uninitialized service.
func setupAuditRoutingTestServer(t *testing.T) *S3ApiServer {
	t.Helper()
	ctx := context.Background()

	manager := newTestSTSIntegrationManager(t)
	require.NoError(t, manager.CreatePolicy(ctx, "", "AuditPolicy", &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{{
			Effect:   "Allow",
			Action:   []string{"s3:GetObject"},
			Resource: []string{"arn:aws:s3:::*/*"},
		}},
	}))
	require.NoError(t, manager.CreateRole(ctx, "", "AuditRole", &integration.RoleDefinition{
		RoleName: "AuditRole",
		TrustPolicy: &policy.PolicyDocument{
			Version:   "2012-10-17",
			Statement: []policy.Statement{{Effect: "Allow", Principal: "*", Action: []string{"sts:AssumeRole"}}},
		},
		AttachedPolicies: []string{"AuditPolicy"},
	}))

	opt := &S3ApiServerOption{EnableIam: true}
	iam := NewIdentityAccessManagementWithStore(opt, nil, "memory")
	iam.isAuthEnabled = true
	iam.iamIntegration = NewS3IAMIntegration(manager, "")

	if iam.credentialManager == nil {
		cm, err := credential.NewCredentialManager("memory", util.GetViper(), "")
		require.NoError(t, err)
		iam.credentialManager = cm
	}
	// Mirror the production wiring in s3api_server.go: without a user store the
	// manager cannot resolve caller policies and GetFederationToken fails closed.
	manager.SetUserStore(iam.credentialManager)

	testIdent := &Identity{
		Name:     routingTestUser,
		Actions:  []Action{s3_constants.ACTION_ADMIN},
		IsStatic: true,
		Credentials: []*Credential{{
			AccessKey: routingTestAccessKey,
			SecretKey: routingTestSecretKey,
		}},
	}
	iam.m.Lock()
	if iam.accessKeyIdent == nil {
		iam.accessKeyIdent = make(map[string]*Identity)
	}
	if iam.nameToIdentity == nil {
		iam.nameToIdentity = make(map[string]*Identity)
	}
	iam.identities = append(iam.identities, testIdent)
	iam.accessKeyIdent[routingTestAccessKey] = testIdent
	iam.nameToIdentity[routingTestUser] = testIdent
	iam.m.Unlock()

	return &S3ApiServer{
		option:            opt,
		iam:               iam,
		credentialManager: iam.credentialManager,
		embeddedIam:       NewEmbeddedIamApi(iam.credentialManager, iam, false),
		stsHandlers:       NewSTSHandlers(manager.GetSTSService(), iam),
	}
}

// Every STS route must be wrapped by track(), which is the only thing that
// emits an audit entry for these handlers: the STS responses go out through
// WriteXMLResponse, which never calls PostLog itself. A route registered
// outside track() would therefore mint credentials with no audit trail at all,
// and nothing else in the suite would notice. STS has three routing layers
// (explicit query-param routes, the authenticated POST dispatcher, and the
// anonymous fallback), so a new action is easy to attach to the wrong one.
func TestSTSRoutesEmitAuditEntries(t *testing.T) {
	router := mux.NewRouter()
	s3a := setupAuditRoutingTestServer(t)
	s3a.registerRouter(router)

	cases := []struct {
		name       string
		action     string
		params     url.Values
		signed     bool
		inBody     bool
		wantStatus int
	}{
		{name: "AssumeRole", action: "AssumeRole", signed: true, wantStatus: http.StatusOK, params: url.Values{
			"RoleArn":         {"arn:aws:iam::" + defaultAccountID + ":role/AuditRole"},
			"RoleSessionName": {"audit-session"},
		}},
		{name: "GetCallerIdentity", action: "GetCallerIdentity", signed: true, wantStatus: http.StatusOK},
		{name: "GetFederationToken", action: "GetFederationToken", signed: true, wantStatus: http.StatusOK, params: url.Values{
			"Name": {"audit-user"},
		}},
		// The anonymous actions still route to STS; they fail on the token or the
		// missing provider, which is itself STS-handler behaviour we want to have
		// reached rather than a 404 from somewhere else.
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
		// explicit query-param routes above, and it can also hand a request to
		// its IAM branch - the STS namespace is what proves it did not.
		{name: "AssumeRole via POST body", action: "AssumeRole", signed: true, inBody: true, wantStatus: http.StatusOK, params: url.Values{
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

			assert.Contains(t, rr.Body.String(), stsXMLNamespace,
				"request did not reach the STS handler: status=%d body=%s", rr.Code, rr.Body.String())
			if tc.wantStatus != 0 {
				assert.Equal(t, tc.wantStatus, rr.Code, rr.Body.String())
			}
			assert.True(t, s3err.AuditAlreadyLogged(req),
				"STS route emitted no audit entry - is the route wrapped in track()? status=%d body=%s",
				rr.Code, rr.Body.String())
		})
	}
}
