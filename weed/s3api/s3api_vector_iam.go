package s3api

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

func (s3a *S3ApiServer) EvaluateIAMPolicy(r *http.Request, action, resource string, policyContext *s3tables.PolicyContext) (allowed, denied bool) {
	identity, _ := s3_constants.GetIdentityFromContext(r).(*Identity)
	if identity == nil || s3a.iam == nil {
		return false, false
	}
	conditions := make(map[string][]string)
	for key, value := range policyContext.ResourceTags {
		conditions["aws:ResourceTag/"+key] = []string{value}
	}
	for key, value := range policyContext.TableBucketTags {
		conditions["s3vectors:VectorBucketTag/"+key] = []string{value}
	}
	for key, value := range policyContext.RequestTags {
		conditions["aws:RequestTag/"+key] = []string{value}
	}
	if len(policyContext.TagKeys) > 0 {
		conditions["aws:TagKeys"] = policyContext.TagKeys
	}
	r = r.WithContext(context.WithValue(r.Context(), vectorIAMConditionsKey{}, conditions))

	result := s3a.iam.evaluateAttachedIAMPolicies(r, identity, Action(action), resource, "")
	denied = result == attachedIAMPolicyDeny || s3a.iam.isActionExplicitlyDeniedByIAM(r, identity, buildPrincipalARN(identity, r), action, resource)
	if denied {
		return false, true
	}
	if s3a.iam.authorizationRoute(r, identity) == authorizeViaIAMIntegration {
		allowed = s3a.iam.VerifyActionPermission(r, identity, Action(action), resource, "") == s3err.ErrNone
		return allowed, hasSessionToken(r) && !allowed
	}
	return result == attachedIAMPolicyAllow, false
}

type vectorIAMConditionsKey struct{}

func vectorIAMConditions(r *http.Request) map[string][]string {
	conditions, _ := r.Context().Value(vectorIAMConditionsKey{}).(map[string][]string)
	return conditions
}
