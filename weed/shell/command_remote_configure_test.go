package shell

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
)

func TestMaskRemoteConfSecrets(t *testing.T) {
	conf := &remote_pb.RemoteConf{
		Type:                    "azure",
		Name:                    "r1",
		S3AccessKey:             "AKID",
		S3SecretKey:             "s3-secret",
		AzureAccountName:        "account",
		AzureAccountKey:         "azure-account-key==",
		BackblazeApplicationKey: "b2-secret",
		AliyunSecretKey:         "aliyun-secret",
		TencentSecretKey:        "tencent-secret",
		BaiduAccessKey:          "baidu-ak",
		BaiduSecretKey:          "baidu-secret",
		WasabiSecretKey:         "wasabi-secret",
		FilebaseSecretKey:       "filebase-secret",
		StorjSecretKey:          "storj-secret",
		ContaboSecretKey:        "contabo-secret",
	}
	maskRemoteConfSecrets(conf)
	for name, got := range map[string]string{
		"s3":        conf.S3SecretKey,
		"azure":     conf.AzureAccountKey,
		"backblaze": conf.BackblazeApplicationKey,
		"aliyun":    conf.AliyunSecretKey,
		"tencent":   conf.TencentSecretKey,
		"baidu ak":  conf.BaiduAccessKey,
		"baidu sk":  conf.BaiduSecretKey,
		"wasabi":    conf.WasabiSecretKey,
		"filebase":  conf.FilebaseSecretKey,
		"storj":     conf.StorjSecretKey,
		"contabo":   conf.ContaboSecretKey,
	} {
		if got == "" || strings.Trim(got, "*") != "" {
			t.Errorf("%s secret not masked: %q", name, got)
		}
	}
	if len(conf.AzureAccountKey) != len("azure-account-key==") {
		t.Errorf("masked azure key changed length: %d", len(conf.AzureAccountKey))
	}
	if conf.AzureAccountName != "account" || conf.S3AccessKey != "AKID" {
		t.Errorf("non-secret fields changed: %q %q", conf.AzureAccountName, conf.S3AccessKey)
	}

	empty := &remote_pb.RemoteConf{Type: "azure"}
	maskRemoteConfSecrets(empty)
	if empty.AzureAccountKey != "" {
		t.Errorf("an unset key must stay empty (Entra ID), got %q", empty.AzureAccountKey)
	}

	gcs := &remote_pb.RemoteConf{Type: "gcs", GcsGoogleApplicationCredentials: ` {"type":"service_account","private_key":"-----BEGIN PRIVATE KEY-----"}`}
	maskRemoteConfSecrets(gcs)
	if strings.Contains(gcs.GcsGoogleApplicationCredentials, "PRIVATE") || strings.Trim(gcs.GcsGoogleApplicationCredentials, "*") != "" {
		t.Errorf("inline gcs credentials not masked: %q", gcs.GcsGoogleApplicationCredentials)
	}
	path := &remote_pb.RemoteConf{Type: "gcs", GcsGoogleApplicationCredentials: "/etc/seaweedfs/gcs.json"}
	maskRemoteConfSecrets(path)
	if path.GcsGoogleApplicationCredentials != "/etc/seaweedfs/gcs.json" {
		t.Errorf("a gcs credentials path must stay listed, got %q", path.GcsGoogleApplicationCredentials)
	}
}
