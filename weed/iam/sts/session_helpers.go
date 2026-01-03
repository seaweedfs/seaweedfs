package sts

import "time"

// IsExpired returns true if the credentials have expired
func (c *Credentials) IsExpired() bool {
	if c == nil {
		return true
	}
	// Treat zero-time expiration as expired (uninitialized credentials)
	// This prevents treating uninitialized credentials as valid
	if c.Expiration.IsZero() {
		return true
	}
	return time.Now().After(c.Expiration)
}

// IsExpired returns true if the session has expired
func (s *SessionInfo) IsExpired() bool {
	// If SessionInfo is nil, consider it expired
	if s == nil {
		return true
	}
	// Treat zero-time expiration as expired (uninitialized session)
	if s.ExpiresAt.IsZero() {
		return true
	}
	return time.Now().After(s.ExpiresAt)
}
