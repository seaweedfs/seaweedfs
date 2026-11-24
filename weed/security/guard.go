package security

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	ErrUnauthorized = errors.New("unauthorized token")
)

/*
Guard is to ensure data access security.
There are 2 ways to check access:
 1. white list. It's checking request ip address.
 2. JSON Web Token(JWT) generated from secretKey.
    The jwt can come from:
 1. url parameter jwt=...
 2. request header "Authorization"
 3. cookie with the name "jwt"

The white list is checked first because it is easy.
Then the JWT is checked.

The Guard will also check these claims if provided:
1. "exp" Expiration Time
2. "nbf" Not Before

Generating JWT:
 1. use HS256 to sign
 2. optionally set "exp", "nbf" fields, in Unix time,
    the number of seconds elapsed since January 1, 1970 UTC.

Referenced:
https://github.com/pkieltyka/jwtauth/blob/master/jwtauth.go
*/
// guardState is the immutable snapshot of all hot-reloadable Guard state. The
// Update* methods build a new snapshot from the current one and swap it in
// atomically, so request-path readers (WhiteList, IsWhiteListed, the SigningKey
// accessors) always observe a consistent set of keys and whitelist — never a
// torn slice header or a mix of old and new state across a SIGHUP.
type guardState struct {
	signingKey          SigningKey
	expiresAfterSec     int
	readSigningKey      SigningKey
	readExpiresAfterSec int
	// previous keys are accepted when verifying, never used to sign
	previousSigningKey     SigningKey
	previousReadSigningKey SigningKey

	whiteListIp      map[string]struct{}
	whiteListCIDR    map[string]*net.IPNet
	isWriteActive    bool
	isEmptyWhiteList bool
}

type Guard struct {
	// state is swapped atomically by the Update* methods. Read it via Load.
	state atomic.Pointer[guardState]
	// updateMu serializes the read-modify-write inside the Update* methods so
	// concurrent reloads don't clobber each other; readers stay lock-free.
	updateMu sync.Mutex
}

func NewGuard(whiteList []string, signingKey string, expiresAfterSec int, readSigningKey string, readExpiresAfterSec int, previousSigningKey string, previousReadSigningKey string) *Guard {
	g := &Guard{}
	g.state.Store(&guardState{
		signingKey:             SigningKey(signingKey),
		expiresAfterSec:        expiresAfterSec,
		readSigningKey:         SigningKey(readSigningKey),
		readExpiresAfterSec:    readExpiresAfterSec,
		previousSigningKey:     SigningKey(previousSigningKey),
		previousReadSigningKey: SigningKey(previousReadSigningKey),
	})
	g.UpdateWhiteList(whiteList)
	return g
}

func (g *Guard) SigningKey() SigningKey     { return g.state.Load().signingKey }
func (g *Guard) ExpiresAfterSec() int       { return g.state.Load().expiresAfterSec }
func (g *Guard) ReadSigningKey() SigningKey { return g.state.Load().readSigningKey }
func (g *Guard) ReadExpiresAfterSec() int   { return g.state.Load().readExpiresAfterSec }

// VerificationKeys returns the write (or read) signing key and the previous
// key still accepted for it, from one snapshot.
func (g *Guard) VerificationKeys(isWrite bool) (signingKey, previousSigningKey SigningKey) {
	st := g.state.Load()
	if isWrite {
		return st.signingKey, st.previousSigningKey
	}
	return st.readSigningKey, st.previousReadSigningKey
}

func (g *Guard) WhiteList(f http.HandlerFunc) http.HandlerFunc {
	if !g.state.Load().isWriteActive {
		//if no security needed, just skip all checking
		return f
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if err := g.checkWhiteList(w, r); err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		f(w, r)
	}
}

func GetActualRemoteHost(r *http.Request) string {
	// For security reasons, only use RemoteAddr to determine the client's IP address.
	// Do not trust headers like X-Forwarded-For, as they can be easily spoofed by clients.
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}

	// If SplitHostPort fails, it may be because of a missing port.
	// We try to parse RemoteAddr as a raw host (IP or hostname).
	host = strings.TrimSpace(r.RemoteAddr)
	// It might be an IPv6 address without a port, but with brackets.
	// e.g. "[::1]"
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = host[1 : len(host)-1]
	}

	// Return the host (can be IP or hostname, just like headers)
	return host
}

func (g *Guard) checkWhiteList(w http.ResponseWriter, r *http.Request) error {
	host := GetActualRemoteHost(r)
	if g.IsWhiteListed(host) {
		return nil
	}
	glog.V(0).Infof("Not in whitelist: %s (original RemoteAddr: %s)", host, r.RemoteAddr)
	return fmt.Errorf("Not in whitelist: %s", host)
}

// IsWhiteListed returns true if the given host IP is allowed by the guard.
// When no whitelist is configured (security inactive), all hosts are allowed.
func (g *Guard) IsWhiteListed(host string) bool {
	st := g.state.Load()
	if !st.isWriteActive {
		return true
	}
	if st.isEmptyWhiteList {
		return true
	}
	if _, ok := st.whiteListIp[host]; ok {
		return true
	}
	remote := net.ParseIP(host)
	if remote != nil {
		for _, cidrnet := range st.whiteListCIDR {
			if cidrnet.Contains(remote) {
				return true
			}
		}
	}
	return false
}

// UpdateSigningKeys refreshes the JWT signing keys and their expirations so
// operators can rotate keys (e.g. via SIGHUP) without restarting the process.
// It swaps in a new snapshot carrying the existing whitelist, so a concurrent
// reader sees either the old keys or the new ones, never a torn slice header.
// Current keys too short for GODEBUG=fips140=only keep the keys in effect, as
// signing with them would panic; it then returns false.
func (g *Guard) UpdateSigningKeys(signingKey string, expiresAfterSec int, readSigningKey string, readExpiresAfterSec int, previousSigningKey string, previousReadSigningKey string) bool {
	if err := util.CheckFips140HmacKeys([]string{"signing key", "read signing key"}, []string{signingKey, readSigningKey}); err != nil {
		glog.Errorf("JWT reload: keeping the keys in effect, because %v", err)
		return false
	}
	previousSigningKey = usablePreviousKey("previous signing key", previousSigningKey)
	previousReadSigningKey = usablePreviousKey("previous read signing key", previousReadSigningKey)
	g.updateMu.Lock()
	defer g.updateMu.Unlock()
	next := *g.state.Load()
	next.signingKey = SigningKey(signingKey)
	next.expiresAfterSec = expiresAfterSec
	next.readSigningKey = SigningKey(readSigningKey)
	next.readExpiresAfterSec = readExpiresAfterSec
	next.previousSigningKey = SigningKey(previousSigningKey)
	next.previousReadSigningKey = SigningKey(previousReadSigningKey)
	next.isWriteActive = !next.isEmptyWhiteList || len(next.signingKey) != 0
	g.state.Store(&next)
	return true
}

// ReloadSigningKeys applies the [section] keys after util.LoadConfiguration
// merged security.toml again. The merge keeps a key the file no longer sets,
// so the keys are read from the file anew: a removed previous key stops
// verifying, and a swapped pair comes from one read. A file that cannot be
// read keeps the keys in effect. It reports whether the new keys are in effect.
func (g *Guard) ReloadSigningKeys(section string) bool {
	fresh, used := util.ReadConfiguration("security", "")
	if used == "" {
		glog.Warningf("reload %s: security.toml cannot be read, keeping the keys in effect", section)
		return false
	}
	v := util.GetViper()
	return g.UpdateSigningKeys(
		freshKey(fresh, section+".key"),
		v.GetInt(section+".expires_after_seconds"),
		freshKey(fresh, section+".read.key"),
		v.GetInt(section+".read.expires_after_seconds"),
		fresh.GetString(section+".previous_key"),
		fresh.GetString(section+".read.previous_key"),
	)
}

// usablePreviousKey returns key, or "" with an error log when it is too short
// for HMAC under GODEBUG=fips140=only. Nothing in that mode can sign with such a
// key, so dropping it rejects no token; keeping the new current keys keeps a
// reload's outcome the same on servers that pass previous keys and on those
// that do not, such as the filer's volume signer.
func usablePreviousKey(name, key string) string {
	if err := util.CheckFips140HmacKey(name, []byte(key)); err != nil {
		glog.Errorf("JWT reload: ignoring the %s, which is shorter than the %d characters FIPS 140 needs for HMAC keys; the new keys are in effect", name, util.Fips140MinHmacKeyBytes)
		return ""
	}
	return key
}

// freshKey returns name as the fresh read sets it, else its merged value, so
// removing a current key from the file does not turn verification off.
func freshKey(fresh *util.ViperProxy, name string) string {
	if fresh.IsSet(name) {
		return fresh.GetString(name)
	}
	return util.GetViper().GetString(name)
}

func (g *Guard) UpdateWhiteList(whiteList []string) {
	whiteListIp := make(map[string]struct{})
	whiteListCIDR := make(map[string]*net.IPNet)
	for _, ip := range whiteList {
		if strings.Contains(ip, "/") {
			_, cidrnet, err := net.ParseCIDR(ip)
			if err != nil {
				glog.Errorf("Parse CIDR %s in whitelist failed: %v", ip, err)
				continue
			}
			whiteListCIDR[ip] = cidrnet
		} else {
			whiteListIp[ip] = struct{}{}
		}
	}
	g.updateMu.Lock()
	defer g.updateMu.Unlock()
	next := *g.state.Load()
	next.isEmptyWhiteList = len(whiteListIp) == 0 && len(whiteListCIDR) == 0
	next.isWriteActive = !next.isEmptyWhiteList || len(next.signingKey) != 0
	next.whiteListIp = whiteListIp
	next.whiteListCIDR = whiteListCIDR
	g.state.Store(&next)
}
