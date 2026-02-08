package testutil

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
)

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
