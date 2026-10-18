package command

import "testing"

func TestServerFilerDisableHttpFlag(t *testing.T) {
	flag := cmdServer.Flag.Lookup("filer.disableHttp")
	if flag == nil {
		t.Fatal("weed server does not expose -filer.disableHttp")
	}
	if flag.DefValue != "false" {
		t.Fatalf("-filer.disableHttp default = %q, want false", flag.DefValue)
	}
}

func TestServerGatewayDataCenterFlags(t *testing.T) {
	for _, name := range []string{"s3.dataCenter", "sftp.dataCenter"} {
		flag := cmdServer.Flag.Lookup(name)
		if flag == nil {
			t.Fatalf("weed server does not expose -%s", name)
		}
		if flag.DefValue != "" {
			t.Fatalf("-%s default = %q, want empty", name, flag.DefValue)
		}
	}
}
