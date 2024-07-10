package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInitSentryNoDSN(t *testing.T) {
	assert.Nil(t, initSentry())
}

func TestInitSentryEmptyDSN(t *testing.T) {
	t.Setenv("SENTRY_DSN", "")
	assert.Nil(t, initSentry())
}

func TestInitSentryTestDSN(t *testing.T) {
	t.Setenv("SENTRY_DSN", "https://example@example.com/123")
	assert.Nil(t, initSentry())
}

func TestInitSentryInvalidDSN(t *testing.T) {
	t.Setenv("SENTRY_DSN", "https://example.com")
	assert.NotNil(t, initSentry())
}
