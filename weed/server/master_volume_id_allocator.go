package weed_server

import (
	"crypto/tls"
	"fmt"
	"io"
	"strings"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/topology"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// newVolumeIdAllocator returns the allocator and an io.Closer to release the
// etcd client on shutdown; the closer is nil for built-in allocation.
func newVolumeIdAllocator(v *util.ViperProxy, topo *topology.Topology) (topology.VolumeIdAllocator, io.Closer, error) {
	servers := v.GetString("master.volume_id_etcd.servers")
	timeout, err := time.ParseDuration(v.GetString("master.volume_id_etcd.timeout"))
	if err != nil {
		return nil, nil, fmt.Errorf("parse master.volume_id_etcd.timeout: %w", err)
	}

	var tlsConfig *tls.Config
	caFile := v.GetString("master.volume_id_etcd.tls_ca_file")
	certFile := v.GetString("master.volume_id_etcd.tls_client_crt_file")
	keyFile := v.GetString("master.volume_id_etcd.tls_client_key_file")
	if caFile != "" || certFile != "" || keyFile != "" {
		tlsInfo := transport.TLSInfo{
			CertFile:      certFile,
			KeyFile:       keyFile,
			TrustedCAFile: caFile,
		}
		if tlsConfig, err = tlsInfo.ClientConfig(); err != nil {
			return nil, nil, fmt.Errorf("etcd TLS client configuration error: %w", err)
		}
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(servers, ","),
		Username:    v.GetString("master.volume_id_etcd.username"),
		Password:    v.GetString("master.volume_id_etcd.password"),
		DialTimeout: timeout,
		TLS:         tlsConfig,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("connect volume id etcd: %w", err)
	}

	glog.Infof("volume id allocator: etcd %s", servers)
	allocator := topology.NewEtcdVolumeIdAllocator(
		client,
		v.GetString("master.volume_id_etcd.key_prefix"),
		timeout,
		topo.GetMaxVolumeId)
	return allocator, allocator, nil
}
