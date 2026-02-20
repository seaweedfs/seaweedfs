package s3tables

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// IAMAuthorizer allows s3tables handlers to evaluate IAM policies without importing s3api.
type IAMAuthorizer interface {
	IsActionAllowed(ctx context.Context, request *integration.ActionRequest) (bool, error)
}

// SetIAMAuthorizer injects the IAM authorizer for policy-based access checks.
func (h *S3TablesHandler) SetIAMAuthorizer(authorizer IAMAuthorizer) {
	h.iamAuthorizer = authorizer
}

func (h *S3TablesHandler) shouldUseIAM(r *http.Request, identityActions, identityPolicyNames []string) bool {
	if h.iamAuthorizer == nil || r == nil {
		return false
	}
	if s3_constants.GetIdentityFromContext(r) == nil {
		return false
	}
	// When default-allow is enabled, keep anonymous requests on the legacy path
	// to preserve zero-config behavior (IAM policies are not available for anonymous).
	if h.defaultAllow && isAnonymousIdentity(r) {
		return false
	}
	// An empty inline `identityActions` slice doesn't mean the identity has no
	// permissions—it just means authorization lives in IAM policies or session
	// tokens instead of static action lists. We therefore prefer the IAM path
	// whenever inline actions are absent and fall back to default policy names
	// or session tokens.
	if hasSessionToken(r) {
		return true
	}
	if len(identityActions) == 0 {
		return true
	}
	return len(identityPolicyNames) > 0
}

func isAnonymousIdentity(r *http.Request) bool {
	val, ok := getIdentityStructValue(r)
	if !ok {
		return false
	}
	if nameField := val.FieldByName("Name"); nameField.IsValid() && nameField.Kind() == reflect.String {
		if nameField.String() == s3_constants.AccountAnonymousId {
			return true
		}
	}
	accountField := val.FieldByName("Account")
	if accountField.IsValid() && !accountField.IsNil() {
		if accountField.Kind() == reflect.Ptr {
			accountField = accountField.Elem()
		}
		if accountField.Kind() == reflect.Struct {
			if idField := accountField.FieldByName("Id"); idField.IsValid() && idField.Kind() == reflect.String {
				if idField.String() == s3_constants.AccountAnonymousId {
					return true
				}
			}
		}
	}
	return false
}

func hasSessionToken(r *http.Request) bool {
	return extractSessionToken(r) != ""
}

func extractSessionToken(r *http.Request) string {
	if token := r.Header.Get("X-SeaweedFS-Session-Token"); token != "" {
		return token
	}
	if token := r.Header.Get("X-Amz-Security-Token"); token != "" {
		return token
	}
	return r.URL.Query().Get("X-Amz-Security-Token")
}

func (h *S3TablesHandler) authorizeIAMAction(r *http.Request, identityPolicyNames []string, action string, resources ...string) (bool, error) {
	if h.iamAuthorizer == nil {
		err := fmt.Errorf("nil iamAuthorizer in authorizeIAMAction")
		glog.V(2).Infof("S3Tables: %v", err)
		return false, err
	}
	principal := r.Header.Get("X-SeaweedFS-Principal")
	if principal == "" {
		principal = getIdentityPrincipalArn(r)
	}
	if principal == "" {
		return false, fmt.Errorf("missing principal for IAM authorization")
	}

	if !strings.Contains(action, ":") {
		action = "s3tables:" + action
	}

	sessionToken := extractSessionToken(r)

	requestContext := buildIAMRequestContext(r, getIdentityClaims(r))
	policyNames := identityPolicyNames
	if len(policyNames) == 0 {
		policyNames = getIdentityPolicyNames(r)
	}

	if len(resources) == 0 {
		return false, fmt.Errorf("no resources provided to authorizeIAMAction")
	}
	checkedResource := false
	for _, resource := range resources {
		if resource == "" {
			continue
		}
		checkedResource = true
		allowed, err := h.iamAuthorizer.IsActionAllowed(r.Context(), &integration.ActionRequest{
			Principal:      principal,
			Action:         action,
			Resource:       resource,
			SessionToken:   sessionToken,
			RequestContext: requestContext,
			PolicyNames:    policyNames,
		})
		if err != nil {
			glog.V(2).Infof("S3Tables: IAM authorization error action=%s resource=%s principal=%s: %v", action, resource, principal, err)
			return false, err
		}
		if !allowed {
			err := fmt.Errorf("access denied by IAM for resource %s", resource)
			return false, err
		}
	}
	if !checkedResource {
		return false, fmt.Errorf("no non-empty resources provided to authorizeIAMAction")
	}
	return true, nil
}

func getIdentityPrincipalArn(r *http.Request) string {
	val, ok := getIdentityStructValue(r)
	if !ok {
		return ""
	}
	field := val.FieldByName("PrincipalArn")
	if field.IsValid() && field.Kind() == reflect.String {
		return field.String()
	}
	return ""
}

func getIdentityPolicyNames(r *http.Request) []string {
	val, ok := getIdentityStructValue(r)
	if !ok {
		return nil
	}
	field := val.FieldByName("PolicyNames")
	if !field.IsValid() || field.Kind() != reflect.Slice {
		return nil
	}
	policies := make([]string, 0, field.Len())
	for i := 0; i < field.Len(); i++ {
		item := field.Index(i)
		if item.Kind() == reflect.String {
			policies = append(policies, item.String())
		} else if item.CanInterface() {
			policies = append(policies, fmt.Sprint(item.Interface()))
		}
	}
	if len(policies) == 0 {
		return nil
	}
	return policies
}

func getIdentityClaims(r *http.Request) map[string]interface{} {
	val, ok := getIdentityStructValue(r)
	if !ok {
		return nil
	}
	field := val.FieldByName("Claims")
	if !field.IsValid() || field.Kind() != reflect.Map || field.IsNil() {
		return nil
	}
	if field.Type().Key().Kind() != reflect.String {
		return nil
	}
	claims := make(map[string]interface{}, field.Len())
	for _, key := range field.MapKeys() {
		if key.Kind() != reflect.String {
			continue
		}
		val := field.MapIndex(key)
		if !val.IsValid() {
			continue
		}
		claims[key.String()] = val.Interface()
	}
	if len(claims) == 0 {
		return nil
	}
	return claims
}

func buildIAMRequestContext(r *http.Request, claims map[string]interface{}) map[string]interface{} {
	ctx := make(map[string]interface{})
	if ua := r.Header.Get("User-Agent"); ua != "" {
		ctx["userAgent"] = ua
	}
	if referer := r.Header.Get("Referer"); referer != "" {
		ctx["referer"] = referer
	}
	for k, v := range claims {
		if strings.HasPrefix(k, "jwt:") {
			if _, exists := ctx[k]; !exists {
				ctx[k] = v
			}
		}
	}
	for k, v := range claims {
		if strings.HasPrefix(k, "jwt:") {
			continue
		}
		if _, exists := ctx[k]; !exists {
			ctx[k] = v
		}
		jwtKey := "jwt:" + k
		if _, exists := ctx[jwtKey]; !exists {
			ctx[jwtKey] = v
		}
	}
	if len(ctx) == 0 {
		return nil
	}
	return ctx
}

// getIdentityStructValue fetches the identity struct held in the request context.
// The identity is expected to be a pointer to a struct with the fields used by
// the reflection helpers (PrincipalArn string, PolicyNames []string,
// Claims map[string]interface{}).
// This helper centralizes the nil-check and ptr-deref logic so callers focus on
// reading the specific fields they need.
func getIdentityStructValue(r *http.Request) (reflect.Value, bool) {
	identityRaw := s3_constants.GetIdentityFromContext(r)
	if identityRaw == nil {
		return reflect.Value{}, false
	}
	val := reflect.ValueOf(identityRaw)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return reflect.Value{}, false
	}
	return val, true
}
