package kafka

import (
	"strings"
	"testing"

	"github.com/Shopify/sarama"
)

func TestConfigureSASLTLSRejectsPartialMTLSConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  SASLTLSConfig
	}{
		{
			name: "missing key",
			cfg: SASLTLSConfig{
				TLSEnabled:    true,
				TLSClientCert: "/tmp/client.crt",
			},
		},
		{
			name: "missing cert",
			cfg: SASLTLSConfig{
				TLSEnabled:   true,
				TLSClientKey: "/tmp/client.key",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ConfigureSASLTLS(sarama.NewConfig(), tt.cfg)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), "both tls_client_cert and tls_client_key must be provided") {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestConfigureSASLTLSConfiguresSCRAMSHA256(t *testing.T) {
	config := sarama.NewConfig()
	err := ConfigureSASLTLS(config, SASLTLSConfig{
		SASLEnabled:   true,
		SASLMechanism: "SCRAM-SHA-256",
		SASLUsername:  "alice",
		SASLPassword:  "secret",
	})
	if err != nil {
		t.Fatalf("ConfigureSASLTLS returned error: %v", err)
	}

	if !config.Net.SASL.Enable {
		t.Fatal("expected SASL to be enabled")
	}
	if config.Net.SASL.Mechanism != sarama.SASLTypeSCRAMSHA256 {
		t.Fatalf("unexpected mechanism: %v", config.Net.SASL.Mechanism)
	}
	if config.Net.SASL.SCRAMClientGeneratorFunc == nil {
		t.Fatal("expected SCRAM client generator")
	}
}
