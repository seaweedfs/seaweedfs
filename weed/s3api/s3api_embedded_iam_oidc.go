package s3api

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go/service/iam"
	iamlib "github.com/seaweedfs/seaweedfs/weed/iam"
	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
)

// OIDC provider IAM actions handled by this file. Mutating actions are
// reserved for Phase 2b; the read-only set lands now so existing static-config
// providers become discoverable through the AWS IAM API.
const (
	actionGetOpenIDConnectProvider   = "GetOpenIDConnectProvider"
	actionListOpenIDConnectProviders = "ListOpenIDConnectProviders"
)

// isOIDCProviderAction reports whether an action belongs to the OIDC provider
// family. Used by ExecuteAction to short-circuit the S3ApiConfiguration code
// path for actions that don't operate on it.
func isOIDCProviderAction(action string) bool {
	switch action {
	case actionGetOpenIDConnectProvider,
		actionListOpenIDConnectProviders:
		return true
	default:
		return false
	}
}

// dispatchOIDCProviderAction handles the OIDC provider IAM actions. Returns a
// response, the IAM error if any, and a boolean indicating whether the action
// was recognised (so the caller can fall through when false).
func (e *EmbeddedIamApi) dispatchOIDCProviderAction(ctx context.Context, values url.Values) (iamlib.RequestIDSetter, *iamError, bool) {
	if !isOIDCProviderAction(values.Get("Action")) {
		return nil, nil, false
	}

	mgr := e.oidcIAMManager()
	if mgr == nil {
		return nil, &iamError{
			Code:  iam.ErrCodeServiceFailureException,
			Error: errors.New("OIDC provider store not configured"),
		}, true
	}

	switch values.Get("Action") {
	case actionListOpenIDConnectProviders:
		resp, err := e.listOpenIDConnectProviders(ctx, mgr)
		return resp, err, true
	case actionGetOpenIDConnectProvider:
		resp, err := e.getOpenIDConnectProvider(ctx, mgr, values)
		return resp, err, true
	}
	return nil, nil, false
}

func (e *EmbeddedIamApi) oidcIAMManager() *integration.IAMManager {
	if e.iam == nil || e.iam.iamIntegration == nil {
		return nil
	}
	provider, ok := e.iam.iamIntegration.(IAMManagerProvider)
	if !ok {
		return nil
	}
	return provider.GetIAMManager()
}

func (e *EmbeddedIamApi) listOpenIDConnectProviders(ctx context.Context, mgr *integration.IAMManager) (*iamlib.ListOpenIDConnectProvidersResponse, *iamError) {
	records, err := mgr.ListOIDCProviders(ctx)
	if err != nil {
		return nil, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	resp := &iamlib.ListOpenIDConnectProvidersResponse{}
	resp.ListOpenIDConnectProvidersResult.OpenIDConnectProviderList = make([]*iamlib.OpenIDConnectProviderListEntry, 0, len(records))
	for _, rec := range records {
		resp.ListOpenIDConnectProvidersResult.OpenIDConnectProviderList = append(
			resp.ListOpenIDConnectProvidersResult.OpenIDConnectProviderList,
			&iamlib.OpenIDConnectProviderListEntry{Arn: rec.ARN},
		)
	}
	return resp, nil
}

func (e *EmbeddedIamApi) getOpenIDConnectProvider(ctx context.Context, mgr *integration.IAMManager, values url.Values) (*iamlib.GetOpenIDConnectProviderResponse, *iamError) {
	arn := strings.TrimSpace(values.Get("OpenIDConnectProviderArn"))
	if arn == "" {
		return nil, &iamError{
			Code:  iam.ErrCodeInvalidInputException,
			Error: fmt.Errorf("OpenIDConnectProviderArn is required"),
		}
	}
	rec, err := mgr.GetOIDCProvider(ctx, arn)
	if err != nil {
		return nil, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: err}
	}
	resp := &iamlib.GetOpenIDConnectProviderResponse{}
	resp.GetOpenIDConnectProviderResult.Url = rec.URL
	resp.GetOpenIDConnectProviderResult.ClientIDList = append([]string(nil), rec.ClientIDs...)
	resp.GetOpenIDConnectProviderResult.ThumbprintList = append([]string(nil), rec.Thumbprints...)
	if !rec.CreatedAt.IsZero() {
		// AWS uses ISO-8601; the IAM XML format accepts time.Time-string output.
		resp.GetOpenIDConnectProviderResult.CreateDate = rec.CreatedAt.UTC().Format("2006-01-02T15:04:05Z")
	}
	if len(rec.Tags) > 0 {
		tags := make([]*iamlib.IAMTag, 0, len(rec.Tags))
		for k, v := range rec.Tags {
			tags = append(tags, &iamlib.IAMTag{Key: k, Value: v})
		}
		resp.GetOpenIDConnectProviderResult.Tags = tags
	}
	return resp, nil
}
