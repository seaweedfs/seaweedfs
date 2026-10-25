package auth

import "testing"

// acceptAllStore records whether the store was consulted and accepts any password.
type acceptAllStore struct {
	stubStore
	called bool
}

func (s *acceptAllStore) ValidatePassword(string, []byte) bool {
	s.called = true
	return true
}

// An empty password must be rejected before the store is consulted, so a store
// without its own empty-secret check cannot authenticate on zero-length input.
func TestPasswordAuthenticatorRejectsEmptyPassword(t *testing.T) {
	store := &acceptAllStore{}
	a := NewPasswordAuthenticator(store, true)

	for _, password := range [][]byte{nil, {}} {
		if _, err := a.Authenticate(&fakeConnMetadata{user: "alice"}, password); err == nil {
			t.Fatalf("expected empty password %v to be rejected", password)
		}
	}
	if store.called {
		t.Fatal("store must not be consulted for an empty password")
	}
}

func TestPasswordAuthenticatorConsultsStore(t *testing.T) {
	store := &acceptAllStore{}
	a := NewPasswordAuthenticator(store, true)

	perms, err := a.Authenticate(&fakeConnMetadata{user: "alice"}, []byte("s3cret"))
	if err != nil {
		t.Fatalf("expected non-empty password to reach the store: %v", err)
	}
	if perms.Extensions["username"] != "alice" {
		t.Fatalf("expected username extension alice, got %q", perms.Extensions["username"])
	}
}
