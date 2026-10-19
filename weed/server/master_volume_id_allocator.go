package weed_server

import (
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/topology"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func newVolumeIdAllocator(v *util.ViperProxy, topo *topology.Topology) (topology.VolumeIdAllocator, error) {
	servers := v.GetString("master.volume_id_etcd.servers")
	timeout, err := time.ParseDuration(v.GetString("master.volume_id_etcd.timeout"))
	if err != nil {
		return nil, fmt.Errorf("parse master.volume_id_etcd.timeout: %w", err)
	}

	var tlsConfig *tls.Config
	if caFile := v.GetString("master.volume_id_etcd.tls_ca_file"); caFile != "" {
		tlsInfo := transport.TLSInfo{
			CertFile:      v.GetString("master.volume_id_etcd.tls_client_crt_file"),
			KeyFile:       v.GetString("master.volume_id_etcd.tls_client_key_file"),
			TrustedCAFile: caFile,
		}
		if tlsConfig, err = tlsInfo.ClientConfig(); err != nil {
			return nil, fmt.Errorf("etcd TLS client configuration error: %w", err)
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
		return nil, fmt.Errorf("connect volume id etcd: %w", err)
	}

	glog.Infof("volume id allocator: etcd %s", servers)
	return topology.NewEtcdVolumeIdAllocator(
		client.KV,
		v.GetString("master.volume_id_etcd.key_prefix"),
		timeout,
		topo.GetMaxVolumeId), nil
}
