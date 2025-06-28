module github.com/seaweedfs/seaweedfs/telemetry/server

go 1.21

require (
	github.com/prometheus/client_golang v1.17.0
	github.com/seaweedfs/seaweedfs/telemetry/proto v0.0.0-00010101000000-000000000000
	google.golang.org/protobuf v1.31.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/prometheus/client_model v0.4.1-0.20230718164431-9a2bf3000d16 // indirect
	github.com/prometheus/common v0.44.0 // indirect
	github.com/prometheus/procfs v0.11.1 // indirect
	golang.org/x/sys v0.11.0 // indirect
)

replace github.com/seaweedfs/seaweedfs/telemetry/proto => ../proto
