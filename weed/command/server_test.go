package command

import (
	"math"
	"strconv"
	"testing"
	"time"
)

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

func TestIdleTimeoutSeconds(t *testing.T) {
	if got := idleTimeoutSeconds(nil); got != 10*time.Second {
		t.Fatalf("idleTimeoutSeconds(nil) = %v, want 10s", got)
	}
	zero, neg, five := 0, -3, 5
	for _, tc := range []struct {
		name    string
		seconds *int
		want    time.Duration
	}{
		{"zero disables", &zero, 0},
		{"negative disables", &neg, 0},
		{"positive converts", &five, 5 * time.Second},
	} {
		if got := idleTimeoutSeconds(tc.seconds); got != tc.want {
			t.Errorf("%s: idleTimeoutSeconds = %v, want %v", tc.name, got, tc.want)
		}
	}
	// Only 64-bit int can exceed the clamp threshold.
	if strconv.IntSize == 64 {
		huge := math.MaxInt
		want := time.Duration(math.MaxInt64/int64(time.Second)) * time.Second
		if got := idleTimeoutSeconds(&huge); got != want {
			t.Errorf("huge clamps: idleTimeoutSeconds = %v, want %v", got, want)
		}
	}
}
