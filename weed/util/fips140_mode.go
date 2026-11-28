package util

import (
	"crypto/fips140"
	"fmt"
	"os"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// FipsModeSetting is the security.toml setting that states the FIPS 140-3
// mode every SeaweedFS process of a deployment runs in. Go processes take the
// mode from GODEBUG and check it against this setting; the Rust binaries take
// it from this setting alone. FipsModeEnv sets it from the environment.
const (
	FipsModeSetting = "fips.mode"
	FipsModeEnv     = "WEED_FIPS_MODE"
)

// The values of fips.mode. An empty value leaves every process as it is.
const (
	FipsModeOff    = "off"
	FipsModeOn     = "on"
	FipsModeStrict = "strict"
)

var fips140Enabled = fips140.Enabled

// fips140Godebug names the fips140 GODEBUG value this process runs with.
func fips140Godebug() string {
	switch {
	case fips140Enforced():
		return "only"
	case fips140Enabled():
		return "on"
	}
	return "off"
}

// CheckFipsMode returns an error when mode, the value of fips.mode read from
// source, is not one SeaweedFS knows or does not match the fips140 GODEBUG
// this process runs with: off needs fips140=off, on needs fips140=on and
// strict needs fips140=only. An empty mode is not checked.
func CheckFipsMode(source, mode string) error {
	var want string
	switch mode {
	case "":
		return nil
	case FipsModeOff:
		want = "off"
	case FipsModeOn:
		want = "on"
	case FipsModeStrict:
		want = "only"
	default:
		return fmt.Errorf("%s is %q; want %s, %s or %s", source, mode, FipsModeOff, FipsModeOn, FipsModeStrict)
	}
	if got := fips140Godebug(); got != want {
		return fmt.Errorf("%s is %q, which needs GODEBUG=fips140=%s; this process runs with fips140=%s", source, mode, want, got)
	}
	return nil
}

// fipsModeSource names where fips.mode came from for messages: the
// environment variable when it is set, else the setting in file.
func fipsModeSource(file string) string {
	if os.Getenv(FipsModeEnv) != "" {
		return FipsModeEnv
	}
	return file + ": " + FipsModeSetting
}

// FipsModeValue returns fips.mode from v, the environment's value when
// WEED_FIPS_MODE is set, "" when neither sets it. A value that is not a
// string, such as an array or a table, is an error naming source: viper's
// GetString would read it as "" and so skip the check.
func FipsModeValue(source string, v interface{ Get(string) interface{} }) (string, error) {
	switch value := v.Get(FipsModeSetting).(type) {
	case nil:
		return "", nil
	case string:
		return value, nil
	default:
		return "", fmt.Errorf("%s is %v, a %T, not a string; want %s, %s or %s", source, value, value, FipsModeOff, FipsModeOn, FipsModeStrict)
	}
}

// RequireFipsMode exits when fips.mode, from the environment or the
// security.toml merged into the shared configuration, is not a string or
// fails CheckFipsMode, so a process never runs in a FIPS mode other than the
// one the deployment states. Call it after loading security.toml.
func RequireFipsMode() {
	source := fipsModeSource(securityConfigFile())
	mode, err := FipsModeValue(source, GetViper())
	if err == nil {
		err = CheckFipsMode(source, mode)
	}
	if err != nil {
		glog.Fatalf("%v", err)
	}
	if mode != "" {
		glog.V(0).Infof("%s is %q, matching GODEBUG fips140=%s", source, mode, fips140Godebug())
	}
}
