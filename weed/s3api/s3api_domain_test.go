package s3api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestClassifyDomainNames tests the domain classification logic for mixed virtual-host and path-style S3 access
// This test validates the fix for issue #7356
func TestClassifyDomainNames(t *testing.T) {
	tests := []struct {
		name                string
		domainNames         []string
		expectedPathStyle   []string
		expectedVirtualHost []string
		description         string
	}{
		{
			name:                "Mixed path-style and virtual-host with single parent",
			domainNames:         []string{"s3.mydomain.com", "develop.s3.mydomain.com"},
			expectedPathStyle:   []string{"develop.s3.mydomain.com"},
			expectedVirtualHost: []string{"s3.mydomain.com"},
			description:         "develop.s3.mydomain.com is path-style because s3.mydomain.com is in the list",
		},
		{
			name:                "Multiple subdomains with same parent",
			domainNames:         []string{"s3.mydomain.com", "develop.s3.mydomain.com", "staging.s3.mydomain.com"},
			expectedPathStyle:   []string{"develop.s3.mydomain.com", "staging.s3.mydomain.com"},
			expectedVirtualHost: []string{"s3.mydomain.com"},
			description:         "Multiple subdomains can be path-style when parent is in the list",
		},
		{
			name:                "Subdomain without parent in list",
			domainNames:         []string{"develop.s3.mydomain.com"},
			expectedPathStyle:   []string{},
			expectedVirtualHost: []string{"develop.s3.mydomain.com"},
			description:         "Subdomain becomes virtual-host when parent is not in the list",
		},
		{
			name:                "Only top-level domain",
			domainNames:         []string{"s3.mydomain.com"},
			expectedPathStyle:   []string{},
			expectedVirtualHost: []string{"s3.mydomain.com"},
			description:         "Top-level domain is always virtual-host style",
		},
		{
			name:                "Multiple independent domains",
			domainNames:         []string{"s3.domain1.com", "s3.domain2.com"},
			expectedPathStyle:   []string{},
			expectedVirtualHost: []string{"s3.domain1.com", "s3.domain2.com"},
			description:         "Independent domains without parent relationships are all virtual-host",
		},
		{
			name:                "Mixed with nested levels",
			domainNames:         []string{"example.com", "s3.example.com", "api.s3.example.com"},
			expectedPathStyle:   []string{"s3.example.com", "api.s3.example.com"},
			expectedVirtualHost: []string{"example.com"},
			description:         "Both s3.example.com and api.s3.example.com are path-style because their immediate parents are in the list",
		},
		{
			name:                "Domain without dot",
			domainNames:         []string{"localhost"},
			expectedPathStyle:   []string{},
			expectedVirtualHost: []string{"localhost"},
			description:         "Domain without dot (no subdomain) is virtual-host style",
		},
		{
			name:                "Empty list",
			domainNames:         []string{},
			expectedPathStyle:   []string{},
			expectedVirtualHost: []string{},
			description:         "Empty domain list returns empty results",
		},
		{
			name:                "Mixed localhost and domain",
			domainNames:         []string{"localhost", "s3.localhost"},
			expectedPathStyle:   []string{"s3.localhost"},
			expectedVirtualHost: []string{"localhost"},
			description:         "s3.localhost is path-style when localhost is in the list",
		},
		{
			name:                "Three-level subdomain hierarchy",
			domainNames:         []string{"example.com", "s3.example.com", "dev.s3.example.com", "api.dev.s3.example.com"},
			expectedPathStyle:   []string{"s3.example.com", "dev.s3.example.com", "api.dev.s3.example.com"},
			expectedVirtualHost: []string{"example.com"},
			description:         "Each level that has its parent in the list becomes path-style",
		},
		{
			name:                "Real-world example from issue #7356",
			domainNames:         []string{"s3.mydomain.com", "develop.s3.mydomain.com", "staging.s3.mydomain.com", "prod.s3.mydomain.com"},
			expectedPathStyle:   []string{"develop.s3.mydomain.com", "staging.s3.mydomain.com", "prod.s3.mydomain.com"},
			expectedVirtualHost: []string{"s3.mydomain.com"},
			description:         "Real-world scenario with multiple environment subdomains",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pathStyle, virtualHost := classifyDomainNames(tt.domainNames)

			assert.ElementsMatch(t, tt.expectedPathStyle, pathStyle,
				"Path-style domains mismatch: %s", tt.description)
			assert.ElementsMatch(t, tt.expectedVirtualHost, virtualHost,
				"Virtual-host domains mismatch: %s", tt.description)
		})
	}
}

// TestClassifyDomainNamesOrder tests that the function maintains consistent behavior regardless of input order
func TestClassifyDomainNamesOrder(t *testing.T) {
	tests := []struct {
		name        string
		domainNames []string
		description string
	}{
		{
			name:        "Parent before child",
			domainNames: []string{"s3.mydomain.com", "develop.s3.mydomain.com"},
			description: "Parent domain listed before child",
		},
		{
			name:        "Child before parent",
			domainNames: []string{"develop.s3.mydomain.com", "s3.mydomain.com"},
			description: "Child domain listed before parent",
		},
		{
			name:        "Mixed order with multiple children",
			domainNames: []string{"staging.s3.mydomain.com", "s3.mydomain.com", "develop.s3.mydomain.com"},
			description: "Children and parent in mixed order",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pathStyle, virtualHost := classifyDomainNames(tt.domainNames)

			// Regardless of order, the result should be consistent
			// Parent should be virtual-host
			assert.Contains(t, virtualHost, "s3.mydomain.com",
				"Parent should always be virtual-host: %s", tt.description)

			// Children should be path-style
			if len(tt.domainNames) > 1 {
				assert.Greater(t, len(pathStyle), 0,
					"Should have at least one path-style domain: %s", tt.description)
			}
		})
	}
}

// TestClassifyDomainNamesEdgeCases tests edge cases and special scenarios
func TestClassifyDomainNamesEdgeCases(t *testing.T) {
	t.Run("Duplicate domains", func(t *testing.T) {
		domainNames := []string{"s3.example.com", "s3.example.com", "api.s3.example.com"}
		pathStyle, virtualHost := classifyDomainNames(domainNames)

		// Even with duplicates, classification should work
		assert.Contains(t, pathStyle, "api.s3.example.com")
		assert.Contains(t, virtualHost, "s3.example.com")
	})

	t.Run("Very long domain name", func(t *testing.T) {
		domainNames := []string{"very.long.subdomain.hierarchy.example.com", "long.subdomain.hierarchy.example.com"}
		pathStyle, virtualHost := classifyDomainNames(domainNames)

		// Should handle long domains correctly
		assert.Contains(t, pathStyle, "very.long.subdomain.hierarchy.example.com")
		assert.Contains(t, virtualHost, "long.subdomain.hierarchy.example.com")
	})

	t.Run("Similar but different domains", func(t *testing.T) {
		domainNames := []string{"s3.example.com", "s3.examples.com", "api.s3.example.com"}
		pathStyle, virtualHost := classifyDomainNames(domainNames)

		// api.s3.example.com should be path-style (parent s3.example.com is in list)
		// s3.examples.com should be virtual-host (different domain)
		assert.Contains(t, pathStyle, "api.s3.example.com")
		assert.Contains(t, virtualHost, "s3.example.com")
		assert.Contains(t, virtualHost, "s3.examples.com")
	})

	t.Run("IP address as domain", func(t *testing.T) {
		domainNames := []string{"127.0.0.1"}
		pathStyle, virtualHost := classifyDomainNames(domainNames)

		// IP address should be treated as virtual-host
		assert.Empty(t, pathStyle)
		assert.Contains(t, virtualHost, "127.0.0.1")
	})
}

// TestClassifyDomainNamesUseCases tests real-world use cases
func TestClassifyDomainNamesUseCases(t *testing.T) {
	t.Run("Issue #7356 - Prometheus blackbox exporter scenario", func(t *testing.T) {
		// From the PR: allow both path-style and virtual-host within same subdomain
		// curl -H 'Host: develop.s3.mydomain.com' http://127.0.0.1:8000/prometheus-blackbox-exporter/status.html
		// curl -H 'Host: prometheus-blackbox-exporter.s3.mydomain.com' http://127.0.0.1:8000/status.html

		domainNames := []string{"s3.mydomain.com", "develop.s3.mydomain.com"}
		pathStyle, virtualHost := classifyDomainNames(domainNames)

		// develop.s3.mydomain.com should be path-style for /bucket/object access
		assert.Contains(t, pathStyle, "develop.s3.mydomain.com",
			"develop subdomain should be path-style")

		// s3.mydomain.com should be virtual-host for bucket.s3.mydomain.com access
		assert.Contains(t, virtualHost, "s3.mydomain.com",
			"parent domain should be virtual-host")
	})

	t.Run("Multi-environment setup", func(t *testing.T) {
		// Common scenario: different environments using different access styles
		domainNames := []string{
			"s3.company.com",         // Production - virtual-host style
			"dev.s3.company.com",     // Development - path-style
			"test.s3.company.com",    // Testing - path-style
			"staging.s3.company.com", // Staging - path-style
		}
		pathStyle, virtualHost := classifyDomainNames(domainNames)

		assert.Len(t, pathStyle, 3, "Should have 3 path-style domains")
		assert.Len(t, virtualHost, 1, "Should have 1 virtual-host domain")
		assert.Contains(t, virtualHost, "s3.company.com")
	})

	t.Run("Mixed production setup", func(t *testing.T) {
		// Multiple base domains with their own subdomains
		domainNames := []string{
			"s3-us-east.company.com",
			"api.s3-us-east.company.com",
			"s3-eu-west.company.com",
			"api.s3-eu-west.company.com",
		}
		pathStyle, virtualHost := classifyDomainNames(domainNames)

		assert.Contains(t, pathStyle, "api.s3-us-east.company.com")
		assert.Contains(t, pathStyle, "api.s3-eu-west.company.com")
		assert.Contains(t, virtualHost, "s3-us-east.company.com")
		assert.Contains(t, virtualHost, "s3-eu-west.company.com")
	})
}
