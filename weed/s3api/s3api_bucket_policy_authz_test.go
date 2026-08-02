package s3api

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"

	iamlib "github.com/seaweedfs/seaweedfs/weed/iam"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// The constants above only close the escalation if the routes are actually
// bound to them, so pin the binding itself. Checking the action in isolation
// passes just as happily when the route still says ACTION_WRITE.
func TestBucketPolicyRoutesUseTheirOwnActions(t *testing.T) {
	bindings := handlerActionBindings(t)

	for _, tc := range []struct{ handler, want string }{
		{"PutBucketPolicyHandler", "ACTION_PUT_BUCKET_POLICY"},
		{"DeleteBucketPolicyHandler", "ACTION_DELETE_BUCKET_POLICY"},
	} {
		got, ok := bindings[tc.handler]
		if !ok {
			t.Errorf("no route found for %s", tc.handler)
			continue
		}
		if got != tc.want {
			t.Errorf("%s is gated on %s, want %s -- %s is implied by object write and lets a writer rewrite the bucket's authorization",
				tc.handler, got, tc.want, got)
		}
	}
}

// handlerActionBindings maps each handler name to the action constant its route
// authorizes on, read out of the router source.
//
// Routes read `iam.Auth(cb.Limit(handler, ACTION))`, which is a multi-value
// pass-through: Limit returns (http.HandlerFunc, Action) and those become Auth's
// two parameters, so the action Auth authorizes on is Limit's second argument
// and the two cannot disagree -- `Auth(Limit(h, X), Y)` does not compile. Auth
// called directly with its own action is still recognised, so a route that
// bypasses Limit is reported against the action it really uses rather than as a
// missing route.
func handlerActionBindings(t *testing.T) map[string]string {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "s3api_server.go", nil, 0)
	if err != nil {
		t.Fatalf("parse s3api_server.go: %v", err)
	}

	bindings := make(map[string]string)
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) != 2 {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || (sel.Sel.Name != "Limit" && sel.Sel.Name != "Auth") {
			return true
		}
		handler, ok := call.Args[0].(*ast.SelectorExpr)
		if !ok || !strings.HasSuffix(handler.Sel.Name, "Handler") {
			return true
		}
		if action, ok := call.Args[1].(*ast.Ident); ok {
			bindings[handler.Sel.Name] = action.Name
		}
		return true
	})

	if len(bindings) < 20 {
		t.Fatalf("parsed only %d handler bindings, the router shape must have changed", len(bindings))
	}
	return bindings
}

// Writing a bucket policy is permissions management, not object writing: an
// explicit Allow in a bucket policy short-circuits the IAM check entirely
// (authRequestWithAuthType sets policyAllows and skips VerifyActionPermission).
// So an identity that can only write objects must not be able to author one --
// otherwise object-write escalates to arbitrary authorization on the bucket,
// including granting anonymous access.
func TestBucketPolicyWriteIsNotImpliedByObjectWrite(t *testing.T) {
	objectWriter := &Identity{
		Name:    "object-writer",
		Actions: []Action{Action(s3_constants.ACTION_WRITE + ":test-bucket")},
	}

	if !objectWriter.CanDo(s3_constants.ACTION_WRITE, "test-bucket", "some/key") {
		t.Fatal("precondition failed: the identity should be able to write objects")
	}

	for _, action := range []string{
		s3_constants.ACTION_PUT_BUCKET_POLICY,
		s3_constants.ACTION_DELETE_BUCKET_POLICY,
	} {
		if objectWriter.CanDo(Action(action), "test-bucket", "") {
			t.Errorf("an object writer was allowed to %s, which escalates to arbitrary bucket authorization", action)
		}
	}
}

// An admin identity keeps managing bucket policies; the tightening must not
// lock the operator out of the surface it protects.
func TestBucketPolicyWriteAllowedForAdmin(t *testing.T) {
	for _, tc := range []struct {
		name     string
		identity *Identity
	}{
		{"global admin", &Identity{Name: "admin", Actions: []Action{Action(s3_constants.ACTION_ADMIN)}}},
		{"bucket admin", &Identity{Name: "bucket-admin", Actions: []Action{Action(s3_constants.ACTION_ADMIN + ":test-bucket")}}},
		{"explicit delegation", &Identity{Name: "policy-manager", Actions: []Action{
			Action(s3_constants.ACTION_PUT_BUCKET_POLICY + ":test-bucket"),
			Action(s3_constants.ACTION_DELETE_BUCKET_POLICY + ":test-bucket"),
		}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, action := range []string{
				s3_constants.ACTION_PUT_BUCKET_POLICY,
				s3_constants.ACTION_DELETE_BUCKET_POLICY,
			} {
				if !tc.identity.CanDo(Action(action), "test-bucket", "") {
					t.Errorf("%s was denied %s", tc.name, action)
				}
			}
		})
	}
}

// The new actions are only reachable if an operator can actually grant them.
// An IAM policy naming s3:PutBucketPolicy is rejected outright when the action
// has no mapping ("not a valid action"), so the grant this change requires has
// to round-trip through the IAM helpers.
func TestBucketPolicyActionsAreGrantableViaIamPolicy(t *testing.T) {
	for _, tc := range []struct{ policyAction, wantIdentityAction string }{
		{"s3:PutBucketPolicy", s3_constants.ACTION_PUT_BUCKET_POLICY},
		{"PutBucketPolicy", s3_constants.ACTION_PUT_BUCKET_POLICY},
		{"s3:DeleteBucketPolicy", s3_constants.ACTION_DELETE_BUCKET_POLICY},
		{"DeleteBucketPolicy", s3_constants.ACTION_DELETE_BUCKET_POLICY},
		{"s3:GetBucketPolicy", s3_constants.ACTION_READ},
	} {
		if got := iamlib.MapToStatementAction(tc.policyAction); got != tc.wantIdentityAction {
			t.Errorf("MapToStatementAction(%q) = %q, want %q -- an unmapped action is rejected as invalid, so the permission cannot be granted",
				tc.policyAction, got, tc.wantIdentityAction)
		}
	}

	// Granting delete-policy must not hand back blanket admin, which is what
	// the previous mapping did.
	if got := iamlib.MapToStatementAction("s3:DeleteBucketPolicy"); got == s3_constants.ACTION_ADMIN {
		t.Error("s3:DeleteBucketPolicy still maps to Admin, so granting it grants everything")
	}

	// And the reverse direction, used to render an identity's actions back as
	// policy statements.
	for _, tc := range []struct{ identityAction, wantPolicyAction string }{
		{s3_constants.ACTION_PUT_BUCKET_POLICY, "PutBucketPolicy"},
		{s3_constants.ACTION_DELETE_BUCKET_POLICY, "DeleteBucketPolicy"},
	} {
		if got := iamlib.MapToIdentitiesAction(tc.identityAction); got != tc.wantPolicyAction {
			t.Errorf("MapToIdentitiesAction(%q) = %q, want %q -- an empty result renders as a bare \"s3:\"",
				tc.identityAction, got, tc.wantPolicyAction)
		}
	}
}

// For IAM-policy identities the router action becomes the S3 action name that
// gets matched, so it has to be the AWS one. Mapping to s3:* (what ACTION_ADMIN
// resolves to) would force a blanket grant instead of s3:PutBucketPolicy.
func TestBucketPolicyActionsMapToAwsActionNames(t *testing.T) {
	for _, tc := range []struct{ action, want string }{
		{s3_constants.ACTION_PUT_BUCKET_POLICY, s3_constants.S3_ACTION_PUT_BUCKET_POLICY},
		{s3_constants.ACTION_DELETE_BUCKET_POLICY, s3_constants.S3_ACTION_DELETE_BUCKET_POLICY},
	} {
		if got := mapBaseActionToS3Format(tc.action); got != tc.want {
			t.Errorf("mapBaseActionToS3Format(%q) = %q, want %q", tc.action, got, tc.want)
		}
	}

	// The escalation in AWS terms: s3:PutObject must not resolve to the same
	// action name the policy write is checked against.
	if mapBaseActionToS3Format(s3_constants.ACTION_WRITE) == s3_constants.S3_ACTION_PUT_BUCKET_POLICY {
		t.Fatal("object write and bucket-policy write resolve to the same S3 action")
	}
}
