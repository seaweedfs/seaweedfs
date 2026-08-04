package nfs

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientAuthorizerResolvesHostnameEntries(t *testing.T) {
	ips, err := net.LookupIP("localhost")
	require.NoError(t, err)
	require.NotEmpty(t, ips)

	authorizer, err := newClientAuthorizer([]string{"localhost"})
	require.NoError(t, err)

	matched := false
	for _, ip := range ips {
		if authorizer.isAllowedAddr(&net.TCPAddr{IP: ip, Port: 2049}) {
			matched = true
			break
		}
	}

	assert.True(t, matched)
	assert.False(t, authorizer.isAllowedAddr(&net.TCPAddr{IP: net.ParseIP("192.0.2.10"), Port: 2049}))
}
