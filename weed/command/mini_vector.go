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
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func startMiniVector() error {
	binary := *miniVectorBinary
	if binary == "weed-vector" {
		if executable, err := os.Executable(); err == nil {
			candidate := filepath.Join(filepath.Dir(executable), binary)
			if _, err := os.Stat(candidate); err == nil {
				binary = candidate
			}
		}
	}
	if _, err := exec.LookPath(binary); err != nil {
		return fmt.Errorf("%s is unavailable: install the vector component or set -vector.binary: %w", binary, err)
	}
	address := util.JoinHostPort(*miniIp, *miniVectorPort)
	args := []string{"--listen=" + address, "--masters=" + string(pb.NewServerAddress(*miniIp, *miniMasterOptions.port, *miniMasterOptions.portGrpc))}
	if miniFilerOptions.filerGroup != nil && *miniFilerOptions.filerGroup != "" {
		args = append(args, "--filer-group="+*miniFilerOptions.filerGroup)
	}
	if *miniVectorMetricsPort > 0 {
		args = append(args, fmt.Sprintf("--metrics-port=%d", *miniVectorMetricsPort), "--metrics-ip="+*miniIp)
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
			conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
			if err == nil {
				_ = conn.Close()
				glog.V(0).Infof("vector engine ready on %s", address)
				return nil
			}
		}
	}
}
