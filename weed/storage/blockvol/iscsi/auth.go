// auth.go implements CHAP authentication for iSCSI (RFC 7143 S12.1).
// Only unidirectional (target authenticates initiator) with MD5 (algorithm 5).
package iscsi

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

var ErrCHAPSecretEmpty = errors.New("iscsi: CHAP enabled but secret is empty")

// CHAPConfig holds CHAP authentication settings.
type CHAPConfig struct {
	Enabled  bool
	Username string // expected initiator username (empty = accept any)
	Secret   string // shared secret
}

type chapState int

const (
	chapIdle          chapState = iota
	chapChallengeSent
	chapDone
)

// CHAPAuthenticator drives the target side of a CHAP exchange.
type CHAPAuthenticator struct {
	config    CHAPConfig
	state     chapState
	id        uint8  // challenge identifier (0-255)
	challenge []byte // 16 random bytes
}

// ValidateCHAPConfig checks that a CHAPConfig is well-formed.
// Must be called at startup before passing the config to NewLoginNegotiator.
func ValidateCHAPConfig(c CHAPConfig) error {
	if c.Enabled && c.Secret == "" {
		return ErrCHAPSecretEmpty
	}
	return nil
}

// NewCHAPAuthenticator creates a CHAP authenticator for one login session.
// The config must have been validated with ValidateCHAPConfig at startup.
func NewCHAPAuthenticator(config CHAPConfig) *CHAPAuthenticator {
	return &CHAPAuthenticator{config: config, state: chapIdle}
}

// IsEnabled returns whether CHAP authentication is required.
func (a *CHAPAuthenticator) IsEnabled() bool {
	return a.config.Enabled
}

// GenerateChallenge produces the CHAP_A, CHAP_I, CHAP_C parameters for the
// first security negotiation response. Must be called exactly once.
func (a *CHAPAuthenticator) GenerateChallenge() (map[string]string, error) {
	// Generate random id byte.
	var idBuf [1]byte
	if _, err := rand.Read(idBuf[:]); err != nil {
		return nil, fmt.Errorf("chap: generate id: %w", err)
	}
	a.id = idBuf[0]

	// Generate 16-byte random challenge.
	a.challenge = make([]byte, 16)
	if _, err := rand.Read(a.challenge); err != nil {
		return nil, fmt.Errorf("chap: generate challenge: %w", err)
	}

	a.state = chapChallengeSent

	return map[string]string{
		"CHAP_A": "5",                                     // MD5
		"CHAP_I": fmt.Sprintf("%d", a.id),                 // decimal
		"CHAP_C": "0x" + hex.EncodeToString(a.challenge),  // hex with 0x prefix
	}, nil
}

// Verify checks the initiator's CHAP_N (username) and CHAP_R (response).
// Returns true if authentication succeeds.
func (a *CHAPAuthenticator) Verify(chapN, chapR string) bool {
	if a.state != chapChallengeSent {
		return false
	}
	a.state = chapDone

	// Check username if configured.
	if a.config.Username != "" && chapN != a.config.Username {
		return false
	}

	// Compute expected response: MD5(id_byte || secret_bytes || challenge_bytes).
	h := md5.New()
	h.Write([]byte{a.id})
	h.Write([]byte(a.config.Secret))
	h.Write(a.challenge)
	expected := hex.EncodeToString(h.Sum(nil))

	// Normalize initiator response: strip "0x" prefix if present.
	got := strings.TrimPrefix(chapR, "0x")
	got = strings.TrimPrefix(got, "0X")

	return strings.EqualFold(expected, got)
}
