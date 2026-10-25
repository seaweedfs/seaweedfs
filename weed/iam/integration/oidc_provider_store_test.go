package integration

import (
	"context"
	"strings"
	"testing"
	"time"
)

func newRecord(arn, url string) *OIDCProviderRecord {
	now := time.Now()
	return &OIDCProviderRecord{
		ARN:       arn,
		URL:       url,
		ClientIDs: []string{"sts.amazonaws.com"},
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func TestMemoryStoreCRUD(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryOIDCProviderStore()

	rec := newRecord(
		"arn:aws:iam::123:oidc-provider/token.actions.githubusercontent.com",
		"https://token.actions.githubusercontent.com",
	)

	// Store + Get round-trip preserves the record.
	if err := store.StoreProvider(ctx, "", rec); err != nil {
		t.Fatalf("StoreProvider: %v", err)
	}
	got, err := store.GetProviderByARN(ctx, "", rec.ARN)
	if err != nil {
		t.Fatalf("GetProviderByARN: %v", err)
	}
	if got.URL != rec.URL {
		t.Fatalf("URL mismatch: got=%s want=%s", got.URL, rec.URL)
	}

	// Mutate the returned copy and verify the store wasn't affected.
	got.ClientIDs[0] = "tampered"
	again, _ := store.GetProviderByARN(ctx, "", rec.ARN)
	if again.ClientIDs[0] == "tampered" {
		t.Fatal("store handed out a shared slice; mutations leaked back")
	}

	// List returns the entry.
	all, err := store.ListProviders(ctx, "")
	if err != nil {
		t.Fatalf("ListProviders: %v", err)
	}
	if len(all) != 1 || all[0].ARN != rec.ARN {
		t.Fatalf("ListProviders unexpected result: %+v", all)
	}

	// Delete -> Get returns not found.
	if err := store.DeleteProvider(ctx, "", rec.ARN); err != nil {
		t.Fatalf("DeleteProvider: %v", err)
	}
	if _, err := store.GetProviderByARN(ctx, "", rec.ARN); err == nil {
		t.Fatal("expected not-found after delete")
	}
	// Delete is idempotent.
	if err := store.DeleteProvider(ctx, "", rec.ARN); err != nil {
		t.Fatalf("idempotent delete should succeed: %v", err)
	}
}

func TestMemoryStoreGetByIssuerNormalizesHost(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryOIDCProviderStore()

	rec := newRecord(
		"arn:aws:iam::123:oidc-provider/token.actions.githubusercontent.com",
		"https://Token.Actions.GithubUserContent.com/", // mixed case + trailing slash
	)
	if err := store.StoreProvider(ctx, "", rec); err != nil {
		t.Fatalf("StoreProvider: %v", err)
	}

	cases := []string{
		"https://token.actions.githubusercontent.com",
		"https://token.actions.githubusercontent.com/",
		"https://TOKEN.actions.GITHUBUSERCONTENT.com",
	}
	for _, want := range cases {
		got, err := store.GetProviderByIssuer(ctx, "", want)
		if err != nil {
			t.Errorf("issuer %q: GetProviderByIssuer: %v", want, err)
			continue
		}
		if got.ARN != rec.ARN {
			t.Errorf("issuer %q: ARN mismatch: got=%s", want, got.ARN)
		}
	}
}

func TestMemoryStoreGetByIssuerMissing(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryOIDCProviderStore()
	_, err := store.GetProviderByIssuer(ctx, "", "https://other.example/")
	if err == nil {
		t.Fatal("expected error for unregistered issuer")
	}
}

func TestDeriveOIDCProviderARN(t *testing.T) {
	cases := []struct {
		name      string
		accountID string
		issuer    string
		want      string
	}{
		{
			name:      "google",
			accountID: "111122223333",
			issuer:    "https://accounts.google.com",
			want:      "arn:aws:iam::111122223333:oidc-provider/accounts.google.com",
		},
		{
			name:      "EKS with path",
			accountID: "999999999999",
			issuer:    "https://oidc.eks.us-west-2.amazonaws.com/id/EXAMPLED",
			want:      "arn:aws:iam::999999999999:oidc-provider/oidc.eks.us-west-2.amazonaws.com/id/EXAMPLED",
		},
		{
			name:      "uppercase host normalized",
			accountID: "111122223333",
			issuer:    "https://Accounts.Google.com",
			want:      "arn:aws:iam::111122223333:oidc-provider/accounts.google.com",
		},
		{
			name:      "trailing slash trimmed",
			accountID: "111122223333",
			issuer:    "https://accounts.google.com/",
			want:      "arn:aws:iam::111122223333:oidc-provider/accounts.google.com",
		},
		{
			name:      "empty account allowed",
			accountID: "",
			issuer:    "https://accounts.google.com",
			want:      "arn:aws:iam:::oidc-provider/accounts.google.com",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DeriveOIDCProviderARN(tc.accountID, tc.issuer)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got=%s want=%s", got, tc.want)
			}
		})
	}
}

func TestDeriveOIDCProviderARNRejectsBadInput(t *testing.T) {
	cases := []string{"", "not a url", "http://"}
	for _, in := range cases {
		if _, err := DeriveOIDCProviderARN("123", in); err == nil {
			t.Errorf("expected error for input %q", in)
		}
	}
}

func TestStoreRejectsEmptyARN(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryOIDCProviderStore()
	rec := newRecord("", "https://issuer/")
	if err := store.StoreProvider(ctx, "", rec); err == nil {
		t.Fatal("expected error storing record with empty ARN")
	}
}

func TestStoreRejectsNil(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryOIDCProviderStore()
	if err := store.StoreProvider(ctx, "", nil); err == nil {
		t.Fatal("expected error storing nil record")
	}
}

func TestNormalizeIssuerRejectsEmpty(t *testing.T) {
	if got := normalizeIssuer(""); got != "" {
		t.Fatalf("empty issuer should normalize to empty, got %q", got)
	}
}

func TestNormalizeIssuerHandlesNonURL(t *testing.T) {
	// Defense in depth: even when issuer is junk, normalize doesn't panic and
	// at least lowercases the input.
	got := normalizeIssuer("Some Random String/")
	if !strings.Contains(got, "some random") {
		t.Fatalf("normalize should lowercase non-URL input, got %q", got)
	}
}
