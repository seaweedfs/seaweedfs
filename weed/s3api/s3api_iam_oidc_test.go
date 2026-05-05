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

func TestReadOnlyDeniesOIDCMutations(t *testing.T) {
	api, _ := newOIDCTestAPI(t)
	api.readOnly = true
	mutations := []string{
		actionCreateOpenIDConnectProvider,
		actionDeleteOpenIDConnectProvider,
		actionAddClientIDToOpenIDConnectProvider,
		actionRemoveClientIDFromOpenIDConnectProvider,
		actionUpdateOpenIDConnectProviderThumbprint,
		actionTagOpenIDConnectProvider,
		actionUntagOpenIDConnectProvider,
	}
	for _, action := range mutations {
		t.Run(action, func(t *testing.T) {
			values := url.Values{}
			values.Set("Action", action)
			_, iamErr := api.ExecuteAction(context.Background(), values, true, "ro-deny")
			if iamErr == nil {
				t.Fatalf("expected denial for %s in read-only mode", action)
			}
		})
	}
}

func TestCreateOpenIDConnectProvider(t *testing.T) {
	api, _ := newOIDCTestAPI(t)
	values := url.Values{}
	values.Set("Action", actionCreateOpenIDConnectProvider)
	values.Set("Url", "https://auth.example.com")
	values.Set("ClientIDList.member.1", "alpha")
	values.Set("ClientIDList.member.2", "beta")
	values.Set("ThumbprintList.member.1", "9e99a48a9960b14926bb7f3b02e22da2b0ab7280")
	values.Set("Tags.member.1.Key", "team")
	values.Set("Tags.member.1.Value", "infra")

	resp, iamErr := api.ExecuteAction(context.Background(), values, true, "create-1")
	if iamErr != nil {
		t.Fatalf("ExecuteAction: %v", iamErr.Error)
	}
	createResp, ok := resp.(*iamlib.CreateOpenIDConnectProviderResponse)
	if !ok {
		t.Fatalf("unexpected response type %T", resp)
	}
	wantArn := "arn:aws:iam::111122223333:oidc-provider/auth.example.com"
	if createResp.CreateOpenIDConnectProviderResult.OpenIDConnectProviderArn != wantArn {
		t.Fatalf("ARN mismatch: got=%s want=%s",
			createResp.CreateOpenIDConnectProviderResult.OpenIDConnectProviderArn, wantArn)
	}

	// Get the new provider back to confirm persistence and clientId list.
	getValues := url.Values{}
	getValues.Set("Action", actionGetOpenIDConnectProvider)
	getValues.Set("OpenIDConnectProviderArn", wantArn)
	getResp, iamErr := api.ExecuteAction(context.Background(), getValues, true, "get-after-create")
	if iamErr != nil {
		t.Fatalf("Get after create: %v", iamErr.Error)
	}
	gr := getResp.(*iamlib.GetOpenIDConnectProviderResponse).GetOpenIDConnectProviderResult
	if len(gr.ClientIDList) != 2 || gr.ClientIDList[0] != "alpha" || gr.ClientIDList[1] != "beta" {
		t.Fatalf("ClientIDList persisted incorrectly: %v", gr.ClientIDList)
	}
	if len(gr.ThumbprintList) != 1 || gr.ThumbprintList[0] != "9e99a48a9960b14926bb7f3b02e22da2b0ab7280" {
		t.Fatalf("ThumbprintList persisted incorrectly: %v", gr.ThumbprintList)
	}
	if len(gr.Tags) != 1 || gr.Tags[0].Key != "team" || gr.Tags[0].Value != "infra" {
		t.Fatalf("Tags persisted incorrectly: %v", gr.Tags)
	}
}

func TestCreateOpenIDConnectProviderRejectsBadInput(t *testing.T) {
	api, _ := newOIDCTestAPI(t)

	t.Run("missing Url", func(t *testing.T) {
		values := url.Values{}
		values.Set("Action", actionCreateOpenIDConnectProvider)
		values.Set("ClientIDList.member.1", "x")
		_, iamErr := api.ExecuteAction(context.Background(), values, true, "")
		if iamErr == nil || iamErr.Code != "InvalidInput" {
			t.Fatalf("expected InvalidInput, got %v", iamErr)
		}
	})

	t.Run("missing ClientIDList", func(t *testing.T) {
		values := url.Values{}
		values.Set("Action", actionCreateOpenIDConnectProvider)
		values.Set("Url", "https://auth.example.com")
		_, iamErr := api.ExecuteAction(context.Background(), values, true, "")
		if iamErr == nil || iamErr.Code != "InvalidInput" {
			t.Fatalf("expected InvalidInput, got %v", iamErr)
		}
	})

	t.Run("invalid thumbprint", func(t *testing.T) {
		values := url.Values{}
		values.Set("Action", actionCreateOpenIDConnectProvider)
		values.Set("Url", "https://auth.example.com")
		values.Set("ClientIDList.member.1", "x")
		values.Set("ThumbprintList.member.1", "not-a-sha1")
		_, iamErr := api.ExecuteAction(context.Background(), values, true, "")
		if iamErr == nil || iamErr.Code != "InvalidInput" {
			t.Fatalf("expected InvalidInput, got %v", iamErr)
		}
	})
}

func TestCreateOpenIDConnectProviderConflict(t *testing.T) {
	api, _ := newOIDCTestAPI(t)
	values := url.Values{}
	values.Set("Action", actionCreateOpenIDConnectProvider)
	values.Set("Url", "https://accounts.google.com") // already mirrored from static config
	values.Set("ClientIDList.member.1", "x")

	_, iamErr := api.ExecuteAction(context.Background(), values, true, "")
	if iamErr == nil {
		t.Fatal("expected conflict for duplicate provider")
	}
	if iamErr.Code != "EntityAlreadyExists" {
		t.Fatalf("expected EntityAlreadyExists, got %s", iamErr.Code)
	}
}

func TestDeleteOpenIDConnectProvider(t *testing.T) {
	api, _ := newOIDCTestAPI(t)
	arn := "arn:aws:iam::111122223333:oidc-provider/accounts.google.com"

	values := url.Values{}
	values.Set("Action", actionDeleteOpenIDConnectProvider)
	values.Set("OpenIDConnectProviderArn", arn)

	if _, iamErr := api.ExecuteAction(context.Background(), values, true, ""); iamErr != nil {
		t.Fatalf("delete: %v", iamErr.Error)
	}
	// Verify gone.
	getValues := url.Values{}
	getValues.Set("Action", actionGetOpenIDConnectProvider)
	getValues.Set("OpenIDConnectProviderArn", arn)
	if _, iamErr := api.ExecuteAction(context.Background(), getValues, true, ""); iamErr == nil {
		t.Fatal("expected not-found after delete")
	}
	// Idempotent.
	if _, iamErr := api.ExecuteAction(context.Background(), values, true, ""); iamErr != nil {
		t.Fatalf("idempotent delete failed: %v", iamErr.Error)
	}
}

func TestAddRemoveClientID(t *testing.T) {
	api, _ := newOIDCTestAPI(t)
	arn := "arn:aws:iam::111122223333:oidc-provider/accounts.google.com"

	add := url.Values{}
	add.Set("Action", actionAddClientIDToOpenIDConnectProvider)
	add.Set("OpenIDConnectProviderArn", arn)
	add.Set("ClientID", "extra-client")
	if _, iamErr := api.ExecuteAction(context.Background(), add, true, ""); iamErr != nil {
		t.Fatalf("add: %v", iamErr.Error)
	}

	getValues := url.Values{}
	getValues.Set("Action", actionGetOpenIDConnectProvider)
	getValues.Set("OpenIDConnectProviderArn", arn)
	resp, iamErr := api.ExecuteAction(context.Background(), getValues, true, "")
	if iamErr != nil {
		t.Fatalf("get: %v", iamErr.Error)
	}
	got := resp.(*iamlib.GetOpenIDConnectProviderResponse).GetOpenIDConnectProviderResult.ClientIDList
	if len(got) != 2 {
		t.Fatalf("expected 2 clients after add, got %v", got)
	}

	// Idempotent add.
	if _, iamErr := api.ExecuteAction(context.Background(), add, true, ""); iamErr != nil {
		t.Fatalf("idempotent add: %v", iamErr.Error)
	}

	// Remove.
	rm := url.Values{}
	rm.Set("Action", actionRemoveClientIDFromOpenIDConnectProvider)
	rm.Set("OpenIDConnectProviderArn", arn)
	rm.Set("ClientID", "extra-client")
	if _, iamErr := api.ExecuteAction(context.Background(), rm, true, ""); iamErr != nil {
		t.Fatalf("remove: %v", iamErr.Error)
	}
	resp, _ = api.ExecuteAction(context.Background(), getValues, true, "")
	got = resp.(*iamlib.GetOpenIDConnectProviderResponse).GetOpenIDConnectProviderResult.ClientIDList
	if len(got) != 1 || got[0] != "client-google" {
		t.Fatalf("unexpected clients after remove: %v", got)
	}

	// Removing a missing client is a no-op.
	if _, iamErr := api.ExecuteAction(context.Background(), rm, true, ""); iamErr != nil {
		t.Fatalf("idempotent remove: %v", iamErr.Error)
	}
}

func TestUpdateThumbprintAndTags(t *testing.T) {
	api, _ := newOIDCTestAPI(t)
	arn := "arn:aws:iam::111122223333:oidc-provider/accounts.google.com"

	upd := url.Values{}
	upd.Set("Action", actionUpdateOpenIDConnectProviderThumbprint)
	upd.Set("OpenIDConnectProviderArn", arn)
	upd.Set("ThumbprintList.member.1", "0000000000000000000000000000000000000000")
	upd.Set("ThumbprintList.member.2", "1111111111111111111111111111111111111111")
	if _, iamErr := api.ExecuteAction(context.Background(), upd, true, ""); iamErr != nil {
		t.Fatalf("update thumbprints: %v", iamErr.Error)
	}

	tag := url.Values{}
	tag.Set("Action", actionTagOpenIDConnectProvider)
	tag.Set("OpenIDConnectProviderArn", arn)
	tag.Set("Tags.member.1.Key", "owner")
	tag.Set("Tags.member.1.Value", "platform")
	if _, iamErr := api.ExecuteAction(context.Background(), tag, true, ""); iamErr != nil {
		t.Fatalf("tag: %v", iamErr.Error)
	}

	get := url.Values{}
	get.Set("Action", actionGetOpenIDConnectProvider)
	get.Set("OpenIDConnectProviderArn", arn)
	resp, iamErr := api.ExecuteAction(context.Background(), get, true, "")
	if iamErr != nil {
		t.Fatalf("get: %v", iamErr.Error)
	}
	gr := resp.(*iamlib.GetOpenIDConnectProviderResponse).GetOpenIDConnectProviderResult
	if len(gr.ThumbprintList) != 2 {
		t.Fatalf("ThumbprintList persisted incorrectly: %v", gr.ThumbprintList)
	}
	if len(gr.Tags) != 1 || gr.Tags[0].Key != "owner" {
		t.Fatalf("Tags persisted incorrectly: %v", gr.Tags)
	}

	untag := url.Values{}
	untag.Set("Action", actionUntagOpenIDConnectProvider)
	untag.Set("OpenIDConnectProviderArn", arn)
	untag.Set("TagKeys.member.1", "owner")
	if _, iamErr := api.ExecuteAction(context.Background(), untag, true, ""); iamErr != nil {
		t.Fatalf("untag: %v", iamErr.Error)
	}
	resp, _ = api.ExecuteAction(context.Background(), get, true, "")
	gr = resp.(*iamlib.GetOpenIDConnectProviderResponse).GetOpenIDConnectProviderResult
	if len(gr.Tags) != 0 {
		t.Fatalf("Tags should be empty after untag, got: %v", gr.Tags)
	}
}
