package s3api

import (
	"context"
	"net/url"
	"testing"
	"time"

	iamlib "github.com/seaweedfs/seaweedfs/weed/iam"
	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
)

// stubIntegration is the smallest IAMManagerProvider that lets the OIDC
// dispatcher reach an IAMManager. The other IAMIntegration methods are
// unused by these tests and panic if invoked, which is what we want — any
// unexpected call signals a routing bug.
type stubIntegration struct {
	IAMIntegration
	mgr *integration.IAMManager
}

func (s *stubIntegration) GetIAMManager() *integration.IAMManager { return s.mgr }

func newOIDCTestAPI(t *testing.T) (*EmbeddedIamApiForTest, *integration.IAMManager) {
	t.Helper()
	mgr := integration.NewIAMManager()
	cfg := &integration.IAMConfig{
		STS: &sts.STSConfig{
			TokenDuration:    sts.FlexibleDuration{Duration: time.Hour},
			MaxSessionLength: sts.FlexibleDuration{Duration: 12 * time.Hour},
			Issuer:           "test-sts",
			SigningKey:       []byte("test-signing-key-32-characters-long"),
			AccountId:        "111122223333",
			Providers: []*sts.ProviderConfig{
				{
					Name:    "google",
					Type:    sts.ProviderTypeOIDC,
					Enabled: true,
					Config: map[string]interface{}{
						"issuer":   "https://accounts.google.com",
						"clientId": "client-google",
					},
				},
				{
					Name:    "github",
					Type:    sts.ProviderTypeOIDC,
					Enabled: true,
					Config: map[string]interface{}{
						"issuer":   "https://token.actions.githubusercontent.com",
						"clientId": "sts.amazonaws.com",
					},
				},
			},
		},
		Policy: &policy.PolicyEngineConfig{DefaultEffect: "Deny", StoreType: "memory"},
		Roles:  &integration.RoleStoreConfig{StoreType: "memory"},
	}
	if err := mgr.Initialize(cfg, func() string { return "localhost:8888" }); err != nil {
		t.Fatalf("Initialize IAM manager: %v", err)
	}

	api := NewEmbeddedIamApiForTest()
	api.iam.iamIntegration = &stubIntegration{mgr: mgr}
	return api, mgr
}

func TestListOpenIDConnectProviders(t *testing.T) {
	api, _ := newOIDCTestAPI(t)
	values := url.Values{}
	values.Set("Action", actionListOpenIDConnectProviders)

	resp, iamErr := api.ExecuteAction(context.Background(), values, true, "test-req-1")
	if iamErr != nil {
		t.Fatalf("ExecuteAction: code=%s err=%v", iamErr.Code, iamErr.Error)
	}
	listResp, ok := resp.(*iamlib.ListOpenIDConnectProvidersResponse)
	if !ok {
		t.Fatalf("unexpected response type %T", resp)
	}
	got := listResp.ListOpenIDConnectProvidersResult.OpenIDConnectProviderList
	if len(got) != 2 {
		t.Fatalf("expected 2 providers, got %d", len(got))
	}
}

func TestGetOpenIDConnectProvider(t *testing.T) {
	api, _ := newOIDCTestAPI(t)
	arn := "arn:aws:iam::111122223333:oidc-provider/accounts.google.com"

	values := url.Values{}
	values.Set("Action", actionGetOpenIDConnectProvider)
	values.Set("OpenIDConnectProviderArn", arn)

	resp, iamErr := api.ExecuteAction(context.Background(), values, true, "test-req-2")
	if iamErr != nil {
		t.Fatalf("ExecuteAction: code=%s err=%v", iamErr.Code, iamErr.Error)
	}
	getResp, ok := resp.(*iamlib.GetOpenIDConnectProviderResponse)
	if !ok {
		t.Fatalf("unexpected response type %T", resp)
	}
	if getResp.GetOpenIDConnectProviderResult.Url != "https://accounts.google.com" {
		t.Fatalf("URL mismatch: %s", getResp.GetOpenIDConnectProviderResult.Url)
	}
	if len(getResp.GetOpenIDConnectProviderResult.ClientIDList) != 1 ||
		getResp.GetOpenIDConnectProviderResult.ClientIDList[0] != "client-google" {
		t.Fatalf("ClientIDList wrong: %v", getResp.GetOpenIDConnectProviderResult.ClientIDList)
	}
}

func TestGetOpenIDConnectProviderMissing(t *testing.T) {
	api, _ := newOIDCTestAPI(t)
	values := url.Values{}
	values.Set("Action", actionGetOpenIDConnectProvider)
	values.Set("OpenIDConnectProviderArn", "arn:aws:iam::111122223333:oidc-provider/nope.example")

	_, iamErr := api.ExecuteAction(context.Background(), values, true, "test-req-3")
	if iamErr == nil {
		t.Fatal("expected NoSuchEntity error")
	}
	if iamErr.Code != "NoSuchEntity" {
		t.Fatalf("expected NoSuchEntity code, got %s", iamErr.Code)
	}
}

func TestGetOpenIDConnectProviderRequiresArn(t *testing.T) {
	api, _ := newOIDCTestAPI(t)
	values := url.Values{}
	values.Set("Action", actionGetOpenIDConnectProvider)

	_, iamErr := api.ExecuteAction(context.Background(), values, true, "test-req-4")
	if iamErr == nil {
		t.Fatal("expected error for missing ARN")
	}
	if iamErr.Code != "InvalidInput" {
		t.Fatalf("expected InvalidInput code, got %s", iamErr.Code)
	}
}

func TestReadOnlyAllowsOIDCList(t *testing.T) {
	api, _ := newOIDCTestAPI(t)
	api.readOnly = true
	values := url.Values{}
	values.Set("Action", actionListOpenIDConnectProviders)

	if _, iamErr := api.ExecuteAction(context.Background(), values, true, "ro-1"); iamErr != nil {
		t.Fatalf("read-only mode should allow ListOpenIDConnectProviders: %v", iamErr.Error)
	}
}
