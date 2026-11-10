package command

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func startMiniVector() error {
	binary := *miniVectorBinary
	if binary == "weed-vector" {
		if executable, err := os.Executable(); err == nil {
			candidate := filepath.Join(filepath.Dir(executable), binary)
			if resolved, err := exec.LookPath(candidate); err == nil {
				binary = resolved
			}
		}
	}
	if _, err := exec.LookPath(binary); err != nil {
		return fmt.Errorf("%s is unavailable: install the vector component or set -vector.binary: %w", binary, err)
	}
	address := util.JoinHostPort(*miniIp, *miniVectorPort)
	bindIP := *miniBindIp
	if bindIP == "" {
		bindIP = *miniIp
	}
	if net.ParseIP(bindIP) == nil {
		ctx, cancel := context.WithTimeout(miniClientsCtx(), 5*time.Second)
		addresses, err := net.DefaultResolver.LookupIPAddr(ctx, bindIP)
		cancel()
		if err != nil {
			return fmt.Errorf("resolve vector bind address %s: %w", bindIP, err)
		}
		if len(addresses) == 0 {
			return fmt.Errorf("vector bind address %s has no IP addresses", bindIP)
		}
		bindIP = addresses[0].String()
	}

	args := []string{"--listen=" + util.JoinHostPort(bindIP, *miniVectorPort), "--advertise=" + address, "--masters=" + string(pb.NewServerAddress(*miniIp, *miniMasterOptions.port, *miniMasterOptions.portGrpc))}
	if miniFilerOptions.filerGroup != nil && *miniFilerOptions.filerGroup != "" {
		args = append(args, "--filer-group="+*miniFilerOptions.filerGroup)
	}
	if *miniVectorMetricsPort > 0 {
		args = append(args, fmt.Sprintf("--metrics-port=%d", *miniVectorMetricsPort), "--metrics-ip="+bindIP)
	}
	if directory := util.ConfigurationFileDirectory.String(); directory != "" {
		args = append(args, "--config-dir="+util.ResolvePath(directory))
	}
	command := exec.CommandContext(miniClientsCtx(), binary, args...)
	command.Stdout, command.Stderr = os.Stdout, os.Stderr
	if err := command.Start(); err != nil {
		return err
	}
	done := trackMiniClient()
	exited := make(chan error, 1)
	go func() {
		defer done()
		err := command.Wait()
		exited <- err
		if err != nil && miniClientsCtx().Err() == nil {
			glog.Errorf("vector engine exited: %v", err)
		}
	}()
	ctx, cancel := context.WithTimeout(miniClientsCtx(), 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-exited:
			return fmt.Errorf("vector engine exited before readiness: %v", err)
		case <-ctx.Done():
			_ = command.Process.Kill()
			return ctx.Err()
		case <-ticker.C:
			conn, err := net.DialTimeout("tcp", miniVectorLocalAddress(bindIP, *miniVectorPort), 100*time.Millisecond)
			if err == nil {
				_ = conn.Close()
				if !miniVectorRegistered(ctx, address) {
					continue
				}
				glog.V(0).Infof("vector engine ready on %s", address)
				return nil
			}
		}
	}
}

func miniVectorLocalAddress(bindIP string, port int) string {
	if ip := net.ParseIP(bindIP); ip != nil && ip.IsUnspecified() {
		if ip.To4() != nil {
			bindIP = "127.0.0.1"
		} else {
			bindIP = "::1"
		}
	}
	return util.JoinHostPort(bindIP, port)
}

func miniVectorRegistered(ctx context.Context, address string) bool {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	master := pb.NewServerAddress(*miniIp, *miniMasterOptions.port, *miniMasterOptions.portGrpc)
	ready := false
	_ = pb.WithMasterClient(ctx, false, master, security.LoadClientTLS(util.GetViper(), "grpc.client"), false, func(client master_pb.SeaweedClient) error {
		response, err := client.ListClusterNodes(ctx, &master_pb.ListClusterNodesRequest{ClientType: "vector", FilerGroup: *miniFilerOptions.filerGroup})
		if err != nil {
			return err
		}
		for _, node := range response.ClusterNodes {
			if node.Address == address {
				ready = true
				break
			}
		}
		return nil
	})
	return ready
}
