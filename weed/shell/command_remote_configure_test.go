package shell

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"google.golang.org/protobuf/proto"
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
		YandexToken:             `{"access_token":"access","refresh_token":"refresh"}`,
		YandexClientSecret:      "yandex-secret",
	}
	original := proto.Clone(conf).(*remote_pb.RemoteConf)
	maskRemoteConfSecrets(conf)
	for name, pair := range map[string][2]string{
		"s3":        {original.S3SecretKey, conf.S3SecretKey},
		"azure":     {original.AzureAccountKey, conf.AzureAccountKey},
		"backblaze": {original.BackblazeApplicationKey, conf.BackblazeApplicationKey},
		"aliyun":    {original.AliyunSecretKey, conf.AliyunSecretKey},
		"tencent":   {original.TencentSecretKey, conf.TencentSecretKey},
		"baidu ak":  {original.BaiduAccessKey, conf.BaiduAccessKey},
		"baidu sk":  {original.BaiduSecretKey, conf.BaiduSecretKey},
		"wasabi":    {original.WasabiSecretKey, conf.WasabiSecretKey},
		"filebase":  {original.FilebaseSecretKey, conf.FilebaseSecretKey},
		"storj":     {original.StorjSecretKey, conf.StorjSecretKey},
		"contabo":   {original.ContaboSecretKey, conf.ContaboSecretKey},
		"yandex tk": {original.YandexToken, conf.YandexToken},
		"yandex cs": {original.YandexClientSecret, conf.YandexClientSecret},
	} {
		if want := strings.Repeat("*", len(pair[0])); pair[1] != want {
			t.Errorf("%s secret masked as %q, want %q", name, pair[1], want)
		}
	}
	if conf.AzureAccountName != "account" || conf.S3AccessKey != "AKID" {
		t.Errorf("non-secret fields changed: %q %q", conf.AzureAccountName, conf.S3AccessKey)
	}

	empty := &remote_pb.RemoteConf{Type: "azure"}
	maskRemoteConfSecrets(empty)
	if empty.AzureAccountKey != "" {
		t.Errorf("an unset key must stay empty (Entra ID), got %q", empty.AzureAccountKey)
	}

	inline := ` {"type":"service_account","private_key":"-----BEGIN PRIVATE KEY-----"}`
	gcs := &remote_pb.RemoteConf{Type: "gcs", GcsGoogleApplicationCredentials: inline}
	maskRemoteConfSecrets(gcs)
	if want := strings.Repeat("*", len(inline)); gcs.GcsGoogleApplicationCredentials != want {
		t.Errorf("inline gcs credentials masked as %q, want %q", gcs.GcsGoogleApplicationCredentials, want)
	}
	path := &remote_pb.RemoteConf{Type: "gcs", GcsGoogleApplicationCredentials: "/etc/seaweedfs/gcs.json"}
	maskRemoteConfSecrets(path)
	if path.GcsGoogleApplicationCredentials != "/etc/seaweedfs/gcs.json" {
		t.Errorf("a gcs credentials path must stay listed, got %q", path.GcsGoogleApplicationCredentials)
	}
}

func TestConfigureFlagSetYandex(t *testing.T) {
	c := &commandRemoteConfigure{}
	conf := &remote_pb.RemoteConf{}
	fs, _, _, _, code, oauthURL := c.configureFlagSet(conf, false)
	err := fs.Parse([]string{
		"-name=disk", "-type=yandex",
		"-yandex.token=token-json",
		"-yandex.client_id=app", "-yandex.client_secret=secret",
		"-yandex.root=/seaweedfs", "-yandex.trash", "-yandex.app_folder",
		"-yandex.code=the-code",
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if conf.Type != "yandex" || conf.YandexToken != "token-json" || conf.YandexClientId != "app" || conf.YandexClientSecret != "secret" {
		t.Fatalf("conf = %+v", conf)
	}
	if conf.YandexRoot != "/seaweedfs" || !conf.YandexTrash || !conf.YandexAppFolder {
		t.Fatalf("yandex options = %+v", conf)
	}
	if *code != "the-code" || *oauthURL {
		t.Fatalf("one-time flags: code %q, oauth url %v", *code, *oauthURL)
	}

	existing := &remote_pb.RemoteConf{Type: "yandex", YandexRoot: "/kept"}
	fs, _, _, _, _, _ = c.configureFlagSet(existing, true)
	if err = fs.Parse([]string{"-yandex.token=next"}); err != nil {
		t.Fatalf("parse existing: %v", err)
	}
	if existing.YandexRoot != "/kept" {
		t.Fatalf("an omitted flag reset the root to %q", existing.YandexRoot)
	}
	if existing.YandexToken != "next" {
		t.Fatalf("token = %q", existing.YandexToken)
	}
}

func TestApplyTypeDefaultsYandex(t *testing.T) {
	c := &commandRemoteConfigure{}
	conf := &remote_pb.RemoteConf{Type: "yandex", YandexRoot: "/stale", S3Region: "us-east-2"}
	c.applyTypeDefaults(conf)
	if conf.YandexRoot != "/" {
		t.Fatalf("root = %q, want /", conf.YandexRoot)
	}
	if conf.S3Region != "" {
		t.Fatalf("s3 region = %q, want it cleared", conf.S3Region)
	}
}
