package wdclient

import (
	"context"
	"reflect"
	"testing"
)

type authoritativeLookupProvider struct {
	calls  int
	result map[string][]Location
	err    error
}

func (p *authoritativeLookupProvider) LookupVolumeIds(_ context.Context, volumeIDs []string) (map[string][]Location, error) {
	p.calls++
	return p.result, p.err
}

func TestAuthoritativeVolumeLookupDoesNotTrustCachedLocations(t *testing.T) {
	provider := &authoritativeLookupProvider{result: map[string][]Location{}}
	client := newVidMapClient(provider, "", DefaultVidMapCacheSize)
	client.addLocation(17, Location{Url: "retired-volume:8080"})

	locations, err := client.LookupVolumeIdsAuthoritative(context.Background(), []string{"17"})
	if err != nil {
		t.Fatalf("authoritative lookup: %v", err)
	}
	if provider.calls != 1 {
		t.Fatalf("provider calls = %d, want 1", provider.calls)
	}
	if len(locations) != 0 {
		t.Fatalf("authoritative lookup returned stale cache: %#v", locations)
	}
}

func TestAuthoritativeVolumeLookupReturnsProviderResult(t *testing.T) {
	want := map[string][]Location{"17": {{Url: "current-volume:8080"}}}
	provider := &authoritativeLookupProvider{result: want}
	client := newVidMapClient(provider, "", DefaultVidMapCacheSize)

	got, err := client.LookupVolumeIdsAuthoritative(context.Background(), []string{"17"})
	if err != nil {
		t.Fatalf("authoritative lookup: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("locations = %#v, want %#v", got, want)
	}
}
