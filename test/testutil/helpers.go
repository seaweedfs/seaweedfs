package testutil

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const SeaweedMiniStartupTimeout = 45 * time.Second

func HasDocker() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "version")
	return cmd.Run() == nil
}

// RequireImageRunnable runs a throwaway container with the given `docker run`
// arguments and skips the test when its process dies on SIGILL. Upstream
// images increasingly target x86-64-v3 (AVX2) - ClickHouse since 26.5 - so a
// host CPU without those instructions cannot run them at all, and the failure
// says nothing about the code under test. Any other outcome, whether a clean
// run, an ordinary non-zero exit, or a pull failure, is left for the test
// itself to report: the probe can only skip on the one failure the code
// cannot cause.
func RequireImageRunnable(t *testing.T, args ...string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", append([]string{"run", "--rm"}, args...)...)
	out, err := cmd.CombinedOutput()
	if err == nil {
		return
	}
	var exitErr *exec.ExitError
	// 132 = 128 + SIGILL: docker propagates the container process's death.
	if errors.As(err, &exitErr) &&
		(exitErr.ExitCode() == 132 ||
			strings.Contains(string(out), "Illegal instruction")) {
		t.Skipf("image cannot execute on this CPU (illegal instruction): docker run %s\n%s",
			strings.Join(args, " "), out)
	}
}

func IsIllegalInstruction(err error) bool {
	var exitErr *exec.ExitError
	return errors.As(err, &exitErr) && exitErr.ExitCode() == 132
}

func FindBindIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "127.0.0.1"
	}
	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if !ok || ipNet.IP == nil {
			continue
		}
		ip := ipNet.IP.To4()
		if ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
			continue
		}
		return ip.String()
	}
	return "127.0.0.1"
}

func WriteIAMConfig(dir, accessKey, secretKey string) (string, error) {
	iamConfigPath := filepath.Join(dir, "iam_config.json")
	iamConfig := fmt.Sprintf(`{
  "identities": [
    {
      "name": "admin",
      "credentials": [
        {
          "accessKey": "%s",
          "secretKey": "%s"
        }
      ],
      "actions": [
        "Admin",
        "Read",
        "List",
        "Tagging",
        "Write"
      ]
    },
    {
      "name": "anonymous"
    }
  ]
}`, accessKey, secretKey)

	if err := os.WriteFile(iamConfigPath, []byte(iamConfig), 0644); err != nil {
		return "", err
	}
	return iamConfigPath, nil
}

func WaitForService(url string, timeout time.Duration) bool {
	client := &http.Client{Timeout: 2 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			resp, err := client.Get(url)
			if err == nil {
				resp.Body.Close()
				return true
			}
		}
	}
}

func WaitForPort(port int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	address := fmt.Sprintf("127.0.0.1:%d", port)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, 500*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}
