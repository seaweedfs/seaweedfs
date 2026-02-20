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
	if hasSessionToken(r) {
		return true
	}
	if len(identityActions) == 0 {
		return true
	}
	return len(identityPolicyNames) > 0
}

func hasSessionToken(r *http.Request) bool {
	if r.Header.Get("X-SeaweedFS-Session-Token") != "" {
		return true
	}
	if r.Header.Get("X-Amz-Security-Token") != "" {
		return true
	}
	return r.URL.Query().Get("X-Amz-Security-Token") != ""
}

func (h *S3TablesHandler) authorizeIAMAction(r *http.Request, identityPolicyNames []string, action string, resources ...string) (bool, error) {
	if h.iamAuthorizer == nil {
		return false, nil
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

	sessionToken := r.Header.Get("X-SeaweedFS-Session-Token")
	if sessionToken == "" {
		sessionToken = r.Header.Get("X-Amz-Security-Token")
		if sessionToken == "" {
			sessionToken = r.URL.Query().Get("X-Amz-Security-Token")
		}
	}

	requestContext := buildIAMRequestContext(r, getIdentityClaims(r))
	policyNames := identityPolicyNames
	if len(policyNames) == 0 {
		policyNames = getIdentityPolicyNames(r)
	}

	var lastErr error
	for _, resource := range resources {
		if resource == "" {
			continue
		}
		allowed, err := h.iamAuthorizer.IsActionAllowed(r.Context(), &integration.ActionRequest{
			Principal:      principal,
			Action:         action,
			Resource:       resource,
			SessionToken:   sessionToken,
			RequestContext: requestContext,
			PolicyNames:    policyNames,
		})
		if err != nil {
			lastErr = err
			glog.V(2).Infof("S3Tables: IAM authorization error action=%s resource=%s principal=%s: %v", action, resource, principal, err)
			continue
		}
		if allowed {
			return true, nil
		}
	}
	return false, lastErr
}

func getIdentityPrincipalArn(r *http.Request) string {
	identityRaw := s3_constants.GetIdentityFromContext(r)
	if identityRaw == nil {
		return ""
	}
	val := reflect.ValueOf(identityRaw)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return ""
	}
	field := val.FieldByName("PrincipalArn")
	if field.IsValid() && field.Kind() == reflect.String {
		return field.String()
	}
	return ""
}

func getIdentityPolicyNames(r *http.Request) []string {
	identityRaw := s3_constants.GetIdentityFromContext(r)
	if identityRaw == nil {
		return nil
	}
	val := reflect.ValueOf(identityRaw)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
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
	identityRaw := s3_constants.GetIdentityFromContext(r)
	if identityRaw == nil {
		return nil
	}
	val := reflect.ValueOf(identityRaw)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
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
	if requestTime := r.Context().Value("requestTime"); requestTime != nil {
		ctx["requestTime"] = requestTime
	}
	for k, v := range claims {
		if _, exists := ctx[k]; !exists {
			ctx[k] = v
		}
		if !strings.Contains(k, ":") {
			jwtKey := "jwt:" + k
			if _, exists := ctx[jwtKey]; !exists {
				ctx[jwtKey] = v
			}
		}
	}
	if len(ctx) == 0 {
		return nil
	}
	return ctx
}
