package security

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
)

// BasicAuth checks HTTP Basic credentials against one configured user and
// password. Digests are compared so the check takes the same time whatever
// the lengths of the configured and supplied values.
type BasicAuth struct {
	user     [sha256.Size]byte
	password [sha256.Size]byte
}

// NewBasicAuth returns nil, meaning Basic Auth is off, when password is empty.
func NewBasicAuth(user, password string) *BasicAuth {
	if password == "" {
		return nil
	}
	return &BasicAuth{
		user:     sha256.Sum256([]byte(user)),
		password: sha256.Sum256([]byte(password)),
	}
}

// Allows reports whether r carries the configured credentials.
func (a *BasicAuth) Allows(r *http.Request) bool {
	user, password, ok := r.BasicAuth()
	if !ok {
		return false
	}
	u := sha256.Sum256([]byte(user))
	p := sha256.Sum256([]byte(password))
	return subtle.ConstantTimeCompare(u[:], a.user[:])&subtle.ConstantTimeCompare(p[:], a.password[:]) == 1
}
