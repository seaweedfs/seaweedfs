package volume_server_ui

import (
	"bytes"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/stats"
)

func TestStatusTpl(t *testing.T) {
	args := struct {
		Version       string
		Masters       []string
		Volumes       interface{}
		EcVolumes     interface{}
		RemoteVolumes interface{}
		DiskStatuses  interface{}
		Stats         interface{}
		Counters      *stats.ServerStats
	}{
		Version: "3.59",
		Masters: []string{"localhost:9333"},
		EcVolumes: []interface{}{
			struct {
				VolumeId   uint32
				Collection string
				Size       uint64
				Shards     []interface{}
				CreatedAt  time.Time
			}{
				VolumeId:   1,
				Collection: "ectest",
				Size:       8 * 1024 * 1024,
				Shards: []interface{}{
					struct {
						ShardId uint8
						Size    int64
					}{
						ShardId: 4,
						Size:    1024 * 1024,
					},
					struct {
						ShardId uint8
						Size    uint32
					}{
						ShardId: 6,
						Size:    1024 * 1024,
					},
				},
				CreatedAt: time.Now(),
			},
		},
		Counters: stats.NewServerStats(),
	}

	var buf bytes.Buffer
	if err := StatusTpl.Execute(&buf, args); err != nil {
		t.Logf("output: %s", buf.String())
		t.Fatalf("template execution error: %v", err)
	}

	if !bytes.Contains(buf.Bytes(), []byte("8.00 MiB")) {
		t.Errorf("output does not contain formatted volume size '8.00 MiB'")
	}
	if !bytes.Contains(buf.Bytes(), []byte("1.00 MiB")) {
		t.Errorf("output does not contain formatted shard size '1.00 MiB'")
	}

	if !bytes.Contains(buf.Bytes(), []byte("Erasure Coding Shards")) {
		t.Errorf("output does not contain 'Erasure Coding Shards'")
	}
	if !bytes.Contains(buf.Bytes(), []byte("ectest")) {
		t.Errorf("output does not contain 'ectest'")
	}
}
