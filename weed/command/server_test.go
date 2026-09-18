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
