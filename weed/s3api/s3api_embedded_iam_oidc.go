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

// OIDC provider IAM actions handled by this file.
const (
	actionGetOpenIDConnectProvider                  = "GetOpenIDConnectProvider"
	actionListOpenIDConnectProviders                = "ListOpenIDConnectProviders"
	actionCreateOpenIDConnectProvider               = "CreateOpenIDConnectProvider"
	actionDeleteOpenIDConnectProvider               = "DeleteOpenIDConnectProvider"
	actionAddClientIDToOpenIDConnectProvider        = "AddClientIDToOpenIDConnectProvider"
	actionRemoveClientIDFromOpenIDConnectProvider   = "RemoveClientIDFromOpenIDConnectProvider"
	actionUpdateOpenIDConnectProviderThumbprint     = "UpdateOpenIDConnectProviderThumbprint"
	actionTagOpenIDConnectProvider                  = "TagOpenIDConnectProvider"
	actionUntagOpenIDConnectProvider                = "UntagOpenIDConnectProvider"
)

// isOIDCProviderAction reports whether an action belongs to the OIDC provider
// family. Used by ExecuteAction to short-circuit the S3ApiConfiguration code
// path for actions that don't operate on it.
func isOIDCProviderAction(action string) bool {
	switch action {
	case actionGetOpenIDConnectProvider,
		actionListOpenIDConnectProviders,
		actionCreateOpenIDConnectProvider,
		actionDeleteOpenIDConnectProvider,
		actionAddClientIDToOpenIDConnectProvider,
		actionRemoveClientIDFromOpenIDConnectProvider,
		actionUpdateOpenIDConnectProviderThumbprint,
		actionTagOpenIDConnectProvider,
		actionUntagOpenIDConnectProvider:
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
	case actionCreateOpenIDConnectProvider:
		resp, err := e.createOpenIDConnectProvider(ctx, mgr, values)
		return resp, err, true
	case actionDeleteOpenIDConnectProvider:
		resp, err := e.deleteOpenIDConnectProvider(ctx, mgr, values)
		return resp, err, true
	case actionAddClientIDToOpenIDConnectProvider:
		resp, err := e.addClientIDToOpenIDConnectProvider(ctx, mgr, values)
		return resp, err, true
	case actionRemoveClientIDFromOpenIDConnectProvider:
		resp, err := e.removeClientIDFromOpenIDConnectProvider(ctx, mgr, values)
		return resp, err, true
	case actionUpdateOpenIDConnectProviderThumbprint:
		resp, err := e.updateOpenIDConnectProviderThumbprint(ctx, mgr, values)
		return resp, err, true
	case actionTagOpenIDConnectProvider:
		resp, err := e.tagOpenIDConnectProvider(ctx, mgr, values)
		return resp, err, true
	case actionUntagOpenIDConnectProvider:
		resp, err := e.untagOpenIDConnectProvider(ctx, mgr, values)
		return resp, err, true
	}
	return nil, nil, false
}

// extractMemberList collects all values from `Foo.member.<n>=...` form
// parameters in order of N. AWS query-string conventions encode list inputs
// this way, so List/Add/Update/Tag actions all share this helper.
func extractMemberList(values url.Values, prefix string) []string {
	out := []string{}
	// Members are indexed from 1; iterate until a missing index breaks the run.
	for i := 1; ; i++ {
		key := fmt.Sprintf("%s.member.%d", prefix, i)
		v := values.Get(key)
		if v == "" {
			break
		}
		out = append(out, v)
	}
	return out
}

func (e *EmbeddedIamApi) createOpenIDConnectProvider(ctx context.Context, mgr *integration.IAMManager, values url.Values) (*iamlib.CreateOpenIDConnectProviderResponse, *iamError) {
	urlStr := strings.TrimSpace(values.Get("Url"))
	if urlStr == "" {
		return nil, &iamError{Code: iam.ErrCodeInvalidInputException, Error: errors.New("Url is required")}
	}
	clientIDs := extractMemberList(values, "ClientIDList")
	if len(clientIDs) == 0 {
		return nil, &iamError{Code: iam.ErrCodeInvalidInputException, Error: errors.New("ClientIDList must contain at least one entry")}
	}
	thumbprints := extractMemberList(values, "ThumbprintList")
	tags := extractTags(values)

	accountID := mgr.GetSTSService().Config.AccountId
	arn, err := integration.DeriveOIDCProviderARN(accountID, urlStr)
	if err != nil {
		return nil, &iamError{Code: iam.ErrCodeInvalidInputException, Error: err}
	}

	rec := &integration.OIDCProviderRecord{
		AccountID:   accountID,
		ARN:         arn,
		URL:         urlStr,
		ClientIDs:   clientIDs,
		Thumbprints: thumbprints,
		Tags:        tags,
	}
	if err := mgr.CreateOIDCProvider(ctx, rec); err != nil {
		if errors.Is(err, integration.ErrOIDCProviderAlreadyExists) {
			return nil, &iamError{Code: iam.ErrCodeEntityAlreadyExistsException, Error: err}
		}
		return nil, &iamError{Code: iam.ErrCodeInvalidInputException, Error: err}
	}

	resp := &iamlib.CreateOpenIDConnectProviderResponse{}
	resp.CreateOpenIDConnectProviderResult.OpenIDConnectProviderArn = rec.ARN
	if len(rec.Tags) > 0 {
		out := make([]*iamlib.IAMTag, 0, len(rec.Tags))
		for k, v := range rec.Tags {
			out = append(out, &iamlib.IAMTag{Key: k, Value: v})
		}
		resp.CreateOpenIDConnectProviderResult.Tags = out
	}
	return resp, nil
}

func (e *EmbeddedIamApi) deleteOpenIDConnectProvider(ctx context.Context, mgr *integration.IAMManager, values url.Values) (*iamlib.DeleteOpenIDConnectProviderResponse, *iamError) {
	arn, iamErr := requireProviderArn(values)
	if iamErr != nil {
		return nil, iamErr
	}
	if err := mgr.DeleteOIDCProvider(ctx, arn); err != nil {
		return nil, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	return &iamlib.DeleteOpenIDConnectProviderResponse{}, nil
}

func (e *EmbeddedIamApi) addClientIDToOpenIDConnectProvider(ctx context.Context, mgr *integration.IAMManager, values url.Values) (*iamlib.AddClientIDToOpenIDConnectProviderResponse, *iamError) {
	arn, iamErr := requireProviderArn(values)
	if iamErr != nil {
		return nil, iamErr
	}
	clientID := strings.TrimSpace(values.Get("ClientID"))
	if clientID == "" {
		return nil, &iamError{Code: iam.ErrCodeInvalidInputException, Error: errors.New("ClientID is required")}
	}
	if err := mgr.AddClientIDToOIDCProvider(ctx, arn, clientID); err != nil {
		if errors.Is(err, integration.ErrOIDCProviderNotFound) {
			return nil, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: err}
		}
		return nil, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	return &iamlib.AddClientIDToOpenIDConnectProviderResponse{}, nil
}

func (e *EmbeddedIamApi) removeClientIDFromOpenIDConnectProvider(ctx context.Context, mgr *integration.IAMManager, values url.Values) (*iamlib.RemoveClientIDFromOpenIDConnectProviderResponse, *iamError) {
	arn, iamErr := requireProviderArn(values)
	if iamErr != nil {
		return nil, iamErr
	}
	clientID := strings.TrimSpace(values.Get("ClientID"))
	if clientID == "" {
		return nil, &iamError{Code: iam.ErrCodeInvalidInputException, Error: errors.New("ClientID is required")}
	}
	if err := mgr.RemoveClientIDFromOIDCProvider(ctx, arn, clientID); err != nil {
		if errors.Is(err, integration.ErrOIDCProviderNotFound) {
			return nil, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: err}
		}
		return nil, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	return &iamlib.RemoveClientIDFromOpenIDConnectProviderResponse{}, nil
}

func (e *EmbeddedIamApi) updateOpenIDConnectProviderThumbprint(ctx context.Context, mgr *integration.IAMManager, values url.Values) (*iamlib.UpdateOpenIDConnectProviderThumbprintResponse, *iamError) {
	arn, iamErr := requireProviderArn(values)
	if iamErr != nil {
		return nil, iamErr
	}
	thumbprints := extractMemberList(values, "ThumbprintList")
	if len(thumbprints) == 0 {
		return nil, &iamError{Code: iam.ErrCodeInvalidInputException, Error: errors.New("ThumbprintList must contain at least one entry")}
	}
	if err := mgr.UpdateOIDCProviderThumbprints(ctx, arn, thumbprints); err != nil {
		if errors.Is(err, integration.ErrOIDCProviderNotFound) {
			return nil, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: err}
		}
		return nil, &iamError{Code: iam.ErrCodeInvalidInputException, Error: err}
	}
	return &iamlib.UpdateOpenIDConnectProviderThumbprintResponse{}, nil
}

func (e *EmbeddedIamApi) tagOpenIDConnectProvider(ctx context.Context, mgr *integration.IAMManager, values url.Values) (*iamlib.TagOpenIDConnectProviderResponse, *iamError) {
	arn, iamErr := requireProviderArn(values)
	if iamErr != nil {
		return nil, iamErr
	}
	tags := extractTags(values)
	if len(tags) == 0 {
		return nil, &iamError{Code: iam.ErrCodeInvalidInputException, Error: errors.New("Tags must contain at least one Key/Value pair")}
	}
	if err := mgr.TagOIDCProvider(ctx, arn, tags); err != nil {
		if errors.Is(err, integration.ErrOIDCProviderNotFound) {
			return nil, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: err}
		}
		return nil, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	return &iamlib.TagOpenIDConnectProviderResponse{}, nil
}

func (e *EmbeddedIamApi) untagOpenIDConnectProvider(ctx context.Context, mgr *integration.IAMManager, values url.Values) (*iamlib.UntagOpenIDConnectProviderResponse, *iamError) {
	arn, iamErr := requireProviderArn(values)
	if iamErr != nil {
		return nil, iamErr
	}
	keys := extractMemberList(values, "TagKeys")
	if len(keys) == 0 {
		return nil, &iamError{Code: iam.ErrCodeInvalidInputException, Error: errors.New("TagKeys must contain at least one entry")}
	}
	if err := mgr.UntagOIDCProvider(ctx, arn, keys); err != nil {
		if errors.Is(err, integration.ErrOIDCProviderNotFound) {
			return nil, &iamError{Code: iam.ErrCodeNoSuchEntityException, Error: err}
		}
		return nil, &iamError{Code: iam.ErrCodeServiceFailureException, Error: err}
	}
	return &iamlib.UntagOpenIDConnectProviderResponse{}, nil
}

// extractTags walks the AWS IAM "Tags.member.N.Key / Tags.member.N.Value"
// query-string convention and returns the parsed map. Returns nil (not an
// empty map) when no tags are present so the caller can distinguish
// "untouched" from "empty".
func extractTags(values url.Values) map[string]string {
	var out map[string]string
	for i := 1; ; i++ {
		k := values.Get(fmt.Sprintf("Tags.member.%d.Key", i))
		if k == "" {
			break
		}
		if out == nil {
			out = make(map[string]string)
		}
		out[k] = values.Get(fmt.Sprintf("Tags.member.%d.Value", i))
	}
	return out
}

func requireProviderArn(values url.Values) (string, *iamError) {
	arn := strings.TrimSpace(values.Get("OpenIDConnectProviderArn"))
	if arn == "" {
		return "", &iamError{Code: iam.ErrCodeInvalidInputException, Error: errors.New("OpenIDConnectProviderArn is required")}
	}
	return arn, nil
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
