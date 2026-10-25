package user

import "testing"

func newTestStore(users ...*User) *FileStore {
	s := &FileStore{users: make(map[string]*User)}
	for _, u := range users {
		s.users[u.Username] = u
	}
	return s
}

func TestValidatePasswordRejectsEmpty(t *testing.T) {
	s := newTestStore(
		&User{Username: "keyonly", Password: "", PublicKeys: []string{"ssh-ed25519 AAAA"}},
		&User{Username: "haspass", Password: "s3cret"},
	)

	cases := []struct {
		username string
		password string
		want     bool
	}{
		{"keyonly", "", false}, // public-key-only user must not accept an empty password
		{"keyonly", "wrong", false},
		{"haspass", "", false}, // a real password is never matched by an empty one
		{"haspass", "s3cret", true},
		{"haspass", "wrong", false},
		{"missing", "", false},
	}
	for _, c := range cases {
		if got := s.ValidatePassword(c.username, []byte(c.password)); got != c.want {
			t.Errorf("ValidatePassword(%q, %q) = %v, want %v", c.username, c.password, got, c.want)
		}
	}
}
