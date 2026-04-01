package testutil

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const SeaweedMiniStartupTimeout = 45 * time.Second

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
    }
  ]
}`, accessKey, secretKey)

	if err := os.WriteFile(iamConfigPath, []byte(iamConfig), 0644); err != nil {
		return "", err
	}
	return iamConfigPath, nil
}

func MustFreeMiniPort(t *testing.T, name string) int {
	t.Helper()

	const (
		minPort = 10000
		maxPort = 55000
	)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < 1000; i++ {
		port := minPort + r.Intn(maxPort-minPort)

		listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			continue
		}
		_ = listener.Close()

		grpcPort := port + 10000
		grpcListener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", grpcPort))
		if err != nil {
			continue
		}
		_ = grpcListener.Close()

		return port
	}

	t.Fatalf("failed to get free weed mini port for %s", name)
	return 0
}
