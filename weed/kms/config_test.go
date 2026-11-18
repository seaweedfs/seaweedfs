package kms

import (
	"errors"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// closeTracker is a KMSProvider that records whether it was closed.
type closeTracker struct {
	KMSProvider
	closed bool
}

func (c *closeTracker) Close() error {
	c.closed = true
	return nil
}

// TestAddKMSProviderReplacesTheInstance checks a configuration reload: the provider gets a
// new instance of its own and the old one is closed, while a reload that fails keeps the
// current instance open and in use.
func TestAddKMSProviderReplacesTheInstance(t *testing.T) {
	var created []*closeTracker
	fail := false
	RegisterProvider("replace-test", func(util.Configuration) (KMSProvider, error) {
		if fail {
			return nil, errors.New("bad configuration")
		}
		p := &closeTracker{KMSProvider: newCountingKMSProvider()}
		created = append(created, p)
		return p, nil
	})
	mgr := &KMSManager{
		providers: make(map[string]KMSProvider),
		configs:   make(map[string]*KMSConfig),
		bucketKMS: make(map[string]string),
	}
	add := func() error { return mgr.AddKMSProvider("p", &KMSConfig{Provider: "replace-test"}) }
	current := func() KMSProvider {
		p, err := mgr.GetKMSProviderByName("p")
		if err != nil {
			t.Fatalf("GetKMSProviderByName: %v", err)
		}
		return p
	}

	if err := add(); err != nil {
		t.Fatalf("AddKMSProvider: %v", err)
	}
	if err := add(); err != nil {
		t.Fatalf("AddKMSProvider again: %v", err)
	}
	if len(created) != 2 || !created[0].closed || created[1].closed || current() != created[1] {
		t.Fatalf("after a reload: %d instances, first closed %v, second closed %v; want a new open instance in use",
			len(created), created[0].closed, created[len(created)-1].closed)
	}

	fail = true
	if err := add(); err == nil {
		t.Fatal("AddKMSProvider with a failing configuration succeeded")
	}
	if created[1].closed || current() != created[1] {
		t.Fatal("a failed reload closed or replaced the provider in use")
	}
}
