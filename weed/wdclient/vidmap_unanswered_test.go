package wdclient

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// A lookup the provider did not answer and an answer that does not list the
// volume both fail with "volume N not found". Only the second says the volume
// is gone, and readers that drop data for a missing volume, such as the
// filer's metadata log replay, must be able to tell them apart.
func TestLookupWithoutAnAnswerIsMarkedUnanswered(t *testing.T) {
	vc := newVidMapClient(&stubProvider{err: errNoAnswer}, "", DefaultVidMapCacheSize)

	_, err := vc.LookupFileIdWithFallback(context.Background(), "7,0123456789")
	if err == nil {
		t.Fatal("lookup of a volume the provider did not answer for succeeded")
	}
	if !errors.Is(err, ErrLookupUnanswered) {
		t.Fatalf("error %q is not marked unanswered", err)
	}
	if !errors.Is(err, errNoAnswer) {
		t.Fatalf("error %q lost the provider's cause", err)
	}
	// Callers that match the text keep working.
	if !strings.Contains(err.Error(), "volume 7 not found") {
		t.Fatalf("error %q no longer says the volume was not found", err)
	}
}

func TestLookupAnsweredWithoutTheVolumeIsNotMarkedUnanswered(t *testing.T) {
	for name, provider := range map[string]*stubProvider{
		"no locations": {locations: map[string][]Location{}},
		// What the master provider returns for a volume it does not know.
		"error for the volume": {answer: func(int) (map[string][]Location, error) {
			return map[string][]Location{}, errors.New("master volume lookup errors: volume 7: volume id 7 not found")
		}},
	} {
		t.Run(name, func(t *testing.T) {
			vc := newVidMapClient(provider, "", DefaultVidMapCacheSize)
			_, err := vc.LookupFileIdWithFallback(context.Background(), "7,0123456789")
			if err == nil {
				t.Fatal("lookup of a volume the provider does not list succeeded")
			}
			if errors.Is(err, ErrLookupUnanswered) {
				t.Fatalf("an answer without the volume was marked unanswered: %q", err)
			}
		})
	}
}

// Without an answer, locations a failed read set aside are still served; the
// lookup then succeeds and nothing is reported.
func TestLookupWithoutAnAnswerServesSetAsideLocations(t *testing.T) {
	provider := &stubProvider{err: errNoAnswer}
	vc := newVidMapClient(provider, "", DefaultVidMapCacheSize)
	vc.addLocation(7, Location{Url: "a:8080", PublicUrl: "a:8080"})
	vc.InvalidateCache("7,0123456789")

	urls, err := vc.LookupFileIdWithFallback(context.Background(), "7,0123456789")
	if err != nil || len(urls) != 1 || urls[0] != "http://a:8080/7,0123456789" {
		t.Fatalf("lookup without an answer over set-aside locations: %v, %v", urls, err)
	}
}
