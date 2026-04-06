package v2bridge

import (
	"fmt"
	"hash/fnv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

type VolumeAccess interface {
	WithVolume(path string, fn func(*blockvol.BlockVol) error) error
}

type ReceiverEndpoints struct {
	DataAddr string
	CtrlAddr string
}

// CommandBindings owns the concrete BlockVol-backed command operations.
// It performs backend binding only and does not own server-side event semantics.
type CommandBindings struct {
	volumes        VolumeAccess
	listenAddr     string
	advertisedHost string
}

func NewCommandBindings(volumes VolumeAccess, listenAddr, advertisedHost string) *CommandBindings {
	return &CommandBindings{
		volumes:        volumes,
		listenAddr:     listenAddr,
		advertisedHost: advertisedHost,
	}
}

func (b *CommandBindings) ApplyRole(a blockvol.BlockVolumeAssignment) error {
	if b == nil || b.volumes == nil {
		return nil
	}
	role := blockvol.RoleFromWire(a.Role)
	ttl := blockvol.LeaseTTLFromWire(a.LeaseTtlMs)
	return b.volumes.WithVolume(a.Path, func(vol *blockvol.BlockVol) error {
		return vol.HandleAssignment(a.Epoch, role, ttl)
	})
}

func (b *CommandBindings) StartReceiver(path, dataAddr, ctrlAddr string) (ReceiverEndpoints, error) {
	if b == nil || b.volumes == nil {
		return ReceiverEndpoints{}, nil
	}
	var endpoints ReceiverEndpoints
	if err := b.volumes.WithVolume(path, func(vol *blockvol.BlockVol) error {
		if b.advertisedHost != "" {
			if err := vol.StartReplicaReceiver(dataAddr, ctrlAddr, b.advertisedHost); err != nil {
				return err
			}
		} else {
			if err := vol.StartReplicaReceiver(dataAddr, ctrlAddr); err != nil {
				return err
			}
		}
		if vol.ReplicaReceiverAddr() != nil {
			endpoints.DataAddr = vol.ReplicaReceiverAddr().DataAddr
			endpoints.CtrlAddr = vol.ReplicaReceiverAddr().CtrlAddr
		}
		return nil
	}); err != nil {
		return ReceiverEndpoints{}, err
	}
	if endpoints.DataAddr == "" {
		endpoints.DataAddr = dataAddr
	}
	if endpoints.CtrlAddr == "" {
		endpoints.CtrlAddr = ctrlAddr
	}
	return endpoints, nil
}

func (b *CommandBindings) ConfigurePrimaryReplication(path string, addrs []blockvol.ReplicaAddr) (string, error) {
	if b == nil || b.volumes == nil || len(addrs) == 0 {
		return "", nil
	}
	rebuildAddr := rebuildListenAddr(path, b.listenAddr)
	if err := b.volumes.WithVolume(path, func(vol *blockvol.BlockVol) error {
		if len(addrs) == 1 {
			vol.SetReplicaAddr(addrs[0].DataAddr, addrs[0].CtrlAddr)
		} else {
			vol.SetReplicaAddrs(addrs)
		}
		if err := vol.StartRebuildServer(rebuildAddr); err != nil {
			glog.Warningf("v2bridge: start rebuild server %s on %s: %v", path, rebuildAddr, err)
		}
		return nil
	}); err != nil {
		return "", err
	}
	return rebuildAddr, nil
}

func (b *CommandBindings) IsPrimaryShipperConnected(path string) bool {
	if b == nil || b.volumes == nil {
		return false
	}
	connected := false
	_ = b.volumes.WithVolume(path, func(vol *blockvol.BlockVol) error {
		connected = vol.PrimaryShipperConnected()
		return nil
	})
	return connected
}

func rebuildListenAddr(path, listenAddr string) string {
	basePort := 3260
	if idx := strings.LastIndex(listenAddr, ":"); idx >= 0 {
		var p int
		if _, err := fmt.Sscanf(listenAddr[idx+1:], "%d", &p); err == nil && p > 0 {
			basePort = p
		}
	}
	h := fnv.New32a()
	h.Write([]byte(path))
	offset := int(h.Sum32()%500) * 3
	dataPort := basePort + 1000 + offset
	rebuildPort := dataPort + 2
	host := listenAddr
	if idx := strings.LastIndex(host, ":"); idx >= 0 {
		host = host[:idx]
	}
	return fmt.Sprintf("%s:%d", host, rebuildPort)
}
