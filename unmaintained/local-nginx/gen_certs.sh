#!/bin/bash
set -e

# Generate a private key
openssl genrsa -out key.pem 2048

# Generate a self-signed certificate
openssl req -new -x509 -key key.pem -out cert.pem -days 365 \
    -subj "/C=US/ST=State/L=City/O=Organization/OU=Unit/CN=localhost"

echo "Certificate (cert.pem) and Key (key.pem) generated successfully in $(pwd)"
