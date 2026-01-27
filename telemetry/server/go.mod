module github.com/seaweedfs/seaweedfs/telemetry/server

go 1.25

toolchain go1.25.0

require (
	github.com/prometheus/client_golang v1.23.2
	github.com/seaweedfs/seaweedfs v0.0.0-00010101000000-000000000000
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.66.1 // indirect
	github.com/prometheus/procfs v0.19.2 // indirect
	go.yaml.in/yaml/v2 v2.4.2 // indirect
	golang.org/x/sys v0.39.0 // indirect
)

replace github.com/seaweedfs/seaweedfs => ../..
