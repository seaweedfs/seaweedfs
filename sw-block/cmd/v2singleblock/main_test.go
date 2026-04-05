package main

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunSmoke_ProducesSingleNodeMVPResult(t *testing.T) {
	path := filepath.Join(t.TempDir(), "single-node-mvp.blk")

	var out bytes.Buffer
	oldOutput := commandOutput
	commandOutput = &out
	defer func() {
		commandOutput = oldOutput
	}()

	runErr := runSmoke([]string{
		"--path", path,
		"--name", "mvp-vol",
		"--node", "node-a",
		"--write-text", "hello-v2",
	})
	if runErr != nil {
		t.Fatalf("runSmoke: %v", runErr)
	}

	var result smokeResult
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal output %q: %v", out.String(), err)
	}
	if result.VolumeName != "mvp-vol" {
		t.Fatalf("volume_name=%q", result.VolumeName)
	}
	if result.Role != "primary" {
		t.Fatalf("role=%q", result.Role)
	}
	if result.Mode != "allocated_only" {
		t.Fatalf("mode=%q", result.Mode)
	}
	if !strings.Contains(result.Readback, "68656c6c6f2d7632") {
		t.Fatalf("readback_hex=%q", result.Readback)
	}
}

func TestRunRestartSmoke_PreservesDataAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "single-node-restart.blk")

	var out bytes.Buffer
	oldOutput := commandOutput
	commandOutput = &out
	defer func() {
		commandOutput = oldOutput
	}()

	runErr := runRestartSmoke([]string{
		"--path", path,
		"--name", "restart-vol",
		"--node", "node-a",
		"--write-text", "hello-restart",
	})
	if runErr != nil {
		t.Fatalf("runRestartSmoke: %v", runErr)
	}

	var result restartSmokeResult
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal output %q: %v", out.String(), err)
	}
	if result.VolumeName != "restart-vol" {
		t.Fatalf("volume_name=%q", result.VolumeName)
	}
	if result.Role != "primary" {
		t.Fatalf("role=%q", result.Role)
	}
	if result.Mode != "allocated_only" {
		t.Fatalf("mode=%q", result.Mode)
	}
	if result.InitialWrite != result.PostRestart {
		t.Fatalf("initial=%q post_restart=%q", result.InitialWrite, result.PostRestart)
	}
	if !strings.Contains(result.PostRestart, "68656c6c6f2d72657374617274") {
		t.Fatalf("post_restart_hex=%q", result.PostRestart)
	}
}

func TestRunISCSISmoke_ExportsFrontendAndReportsAddress(t *testing.T) {
	path := filepath.Join(t.TempDir(), "single-node-iscsi.blk")

	var out bytes.Buffer
	oldOutput := commandOutput
	commandOutput = &out
	defer func() {
		commandOutput = oldOutput
	}()

	runErr := runISCSISmoke([]string{
		"--path", path,
		"--name", "iscsi-vol",
		"--node", "node-a",
		"--iqn", "iqn.2026-04.com.seaweedfs:test.cli",
	})
	if runErr != nil {
		t.Fatalf("runISCSISmoke: %v", runErr)
	}

	var result iscsiSmokeResult
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal output %q: %v", out.String(), err)
	}
	if result.VolumeName != "iscsi-vol" {
		t.Fatalf("volume_name=%q", result.VolumeName)
	}
	if result.Role != "primary" {
		t.Fatalf("role=%q", result.Role)
	}
	if result.Mode != "allocated_only" {
		t.Fatalf("mode=%q", result.Mode)
	}
	if result.IQN != "iqn.2026-04.com.seaweedfs:test.cli" {
		t.Fatalf("iqn=%q", result.IQN)
	}
	if !strings.Contains(result.Address, "127.0.0.1:") {
		t.Fatalf("address=%q", result.Address)
	}
}
