package sts

import "time"

// IsExpired returns true if the credentials have expired
func (c *Credentials) IsExpired() bool {
	if c == nil {
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
	return time.Now().After(s.ExpiresAt)
}
