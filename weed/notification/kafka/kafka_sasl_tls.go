package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/xdg-go/scram"
)

// SASLTLSConfig holds SASL and TLS configuration for Kafka connections.
type SASLTLSConfig struct {
	SASLEnabled   bool
	SASLMechanism string
	SASLUsername  string
	SASLPassword  string

	TLSEnabled            bool
	TLSCACert             string
	TLSClientCert         string
	TLSClientKey          string
	TLSInsecureSkipVerify bool
}

// ConfigureSASLTLS applies SASL and TLS settings to a sarama config.
func ConfigureSASLTLS(config *sarama.Config, st SASLTLSConfig) error {
	if st.SASLEnabled {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = st.SASLUsername
		config.Net.SASL.Password = st.SASLPassword

		mechanism := strings.ToUpper(st.SASLMechanism)
		switch mechanism {
		case "PLAIN", "":
			config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		case "SCRAM-SHA-256":
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &scramClient{HashGeneratorFcn: scram.SHA256}
			}
		case "SCRAM-SHA-512":
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &scramClient{HashGeneratorFcn: scram.SHA512}
			}
		default:
			return fmt.Errorf("unsupported SASL mechanism: %s", mechanism)
		}
	}

	if st.TLSEnabled {
		if (st.TLSClientCert == "") != (st.TLSClientKey == "") {
			return fmt.Errorf("both tls_client_cert and tls_client_key must be provided for mTLS, or neither")
		}

		tlsConfig := &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: st.TLSInsecureSkipVerify,
		}

		if st.TLSCACert != "" {
			caCert, err := os.ReadFile(st.TLSCACert)
			if err != nil {
				return fmt.Errorf("failed to read CA certificate: %w", err)
			}
			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				return fmt.Errorf("failed to parse CA certificate")
			}
			tlsConfig.RootCAs = caCertPool
		}

		if st.TLSClientCert != "" && st.TLSClientKey != "" {
			cert, err := tls.LoadX509KeyPair(st.TLSClientCert, st.TLSClientKey)
			if err != nil {
				return fmt.Errorf("failed to load client certificate/key: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsConfig
	}

	return nil
}

// scramClient implements the sarama.SCRAMClient interface.
type scramClient struct {
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (c *scramClient) Begin(userName, password, authzID string) (err error) {
	client, err := c.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	c.ClientConversation = client.NewConversation()
	return nil
}

func (c *scramClient) Step(challenge string) (string, error) {
	return c.ClientConversation.Step(challenge)
}

func (c *scramClient) Done() bool {
	return c.ClientConversation.Done()
}
