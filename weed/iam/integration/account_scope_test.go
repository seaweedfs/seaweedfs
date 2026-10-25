package integration

import (
	"context"
	"testing"
)

func TestGetProviderByIssuerAndAccount(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryOIDCProviderStore()

	// Two providers for the same issuer, scoped to different accounts.
	must := func(rec *OIDCProviderRecord) {
		if err := store.StoreProvider(ctx, "", rec); err != nil {
			t.Fatalf("store: %v", err)
		}
	}
	must(&OIDCProviderRecord{
		AccountID: "111",
		ARN:       "arn:aws:iam::111:oidc-provider/idp.example",
		URL:       "https://idp.example",
		ClientIDs: []string{"a"},
	})
	must(&OIDCProviderRecord{
		AccountID: "222",
		ARN:       "arn:aws:iam::222:oidc-provider/idp.example",
		URL:       "https://idp.example",
		ClientIDs: []string{"b"},
	})

	t.Run("matching account returns scoped record", func(t *testing.T) {
		rec, err := store.GetProviderByIssuerAndAccount(ctx, "", "https://idp.example", "111")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if rec.AccountID != "111" {
			t.Fatalf("unexpected record: %+v", rec)
		}
	})

	t.Run("unknown account is rejected", func(t *testing.T) {
		if _, err := store.GetProviderByIssuerAndAccount(ctx, "", "https://idp.example", "999"); err == nil {
			t.Fatal("expected error for cross-account use")
		}
	})

	t.Run("global provider satisfies any account", func(t *testing.T) {
		must(&OIDCProviderRecord{
			AccountID: "",
			ARN:       "arn:aws:iam:::oidc-provider/global.example",
			URL:       "https://global.example",
			ClientIDs: []string{"x"},
		})
		rec, err := store.GetProviderByIssuerAndAccount(ctx, "", "https://global.example", "anything")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if rec.AccountID != "" {
			t.Fatalf("expected global record, got AccountID=%q", rec.AccountID)
		}
	})

	t.Run("empty caller account accepts any record", func(t *testing.T) {
		rec, err := store.GetProviderByIssuerAndAccount(ctx, "", "https://idp.example", "")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if rec == nil {
			t.Fatal("expected a record")
		}
	})
}
