package s3vectors

import (
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

type IAMAuthorizer interface {
	EvaluateIAMPolicy(r *http.Request, action, resource string, policyContext *s3tables.PolicyContext) (allowed, denied bool)
}

func (s *Server) checkPermission(r *http.Request, action, principal, owner, policy, resource string, ctx *s3tables.PolicyContext) bool {
	if authorizer, ok := s.authenticator.(IAMAuthorizer); ok {
		allowed, denied := authorizer.EvaluateIAMPolicy(r, action, resource, ctx)
		if denied {
			return false
		}
		if allowed {
			copyContext := *ctx
			copyContext.IdentityActions = append(append([]string{}, ctx.IdentityActions...), action)
			ctx = &copyContext
		}
	}
	return s3tables.CheckPermissionWithContext(action, principal, owner, policy, resource, ctx)
}
