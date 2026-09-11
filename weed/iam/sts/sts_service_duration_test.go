package sts

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func newSTSServiceWithMaxSession(t *testing.T, maxSession time.Duration) *STSService {
	t.Helper()
	s := NewSTSService()
	assert.NoError(t, s.Initialize(&STSConfig{
		Issuer:           "test-issuer",
		SigningKey:       []byte("test-signing-key-at-least-32-bytes-long"),
		TokenDuration:    FlexibleDuration{Duration: time.Hour},
		MaxSessionLength: FlexibleDuration{Duration: maxSession},
	}))
	return s
}

func secondsPtr(v int64) *int64 { return &v }

func TestValidateSessionDurationSeconds_RespectsConfiguredMaxSessionLength(t *testing.T) {
	s := newSTSServiceWithMaxSession(t, 168*time.Hour)
	assert.NoError(t, s.validateSessionDurationSeconds(secondsPtr(604800)))
}

func TestValidateSessionDurationSeconds_RejectsAboveConfiguredMaxSessionLength(t *testing.T) {
	s := newSTSServiceWithMaxSession(t, 24*time.Hour)
	err := s.validateSessionDurationSeconds(secondsPtr(90000))
	assert.Error(t, err)
}

func TestValidateSessionDurationSeconds_FallsBackToDefaultWhenUnset(t *testing.T) {
	s := &STSService{}
	assert.NoError(t, s.validateSessionDurationSeconds(secondsPtr(43200)))
	err := s.validateSessionDurationSeconds(secondsPtr(43201))
	assert.Error(t, err)
}

func TestValidateSessionDurationSeconds_EnforcesMinimum(t *testing.T) {
	s := newSTSServiceWithMaxSession(t, 168*time.Hour)
	assert.Error(t, s.validateSessionDurationSeconds(secondsPtr(899)))
}

func TestValidateSessionDurationSeconds_NilReturnsNil(t *testing.T) {
	s := newSTSServiceWithMaxSession(t, 168*time.Hour)
	assert.NoError(t, s.validateSessionDurationSeconds(nil))
}
