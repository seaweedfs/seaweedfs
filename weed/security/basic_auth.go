package security

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
)

// BasicAuth checks HTTP Basic credentials against one configured user and
// password. Values are compared as MACs under a random per-process key, so
// the check takes the same time whatever the lengths of the configured and
// supplied values, and no plain digest of the password is kept.
type BasicAuth struct {
	key      []byte
	user     []byte
	password []byte
}

// NewBasicAuth returns nil, meaning Basic Auth is off, when password is empty.
func NewBasicAuth(user, password string) *BasicAuth {
	if password == "" {
		return nil
	}
	a := &BasicAuth{key: make([]byte, sha256.Size)}
	rand.Read(a.key)
	a.user = a.mac(user)
	a.password = a.mac(password)
	return a
}

// Allows reports whether r carries the configured credentials.
func (a *BasicAuth) Allows(r *http.Request) bool {
	user, password, ok := r.BasicAuth()
	if !ok {
		return false
	}
	return subtle.ConstantTimeCompare(a.mac(user), a.user)&subtle.ConstantTimeCompare(a.mac(password), a.password) == 1
}

func (a *BasicAuth) mac(s string) []byte {
	h := hmac.New(sha256.New, a.key)
	h.Write([]byte(s))
	return h.Sum(nil)
}
