package actions

import (
	"context"
	"fmt"
	"strings"

	tr "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner"
)

// RegisterIOActions registers IO-related actions.
func RegisterIOActions(r *tr.Registry) {
	r.RegisterFunc("dd_write", tr.TierBlock, ddWrite)
	r.RegisterFunc("dd_read_md5", tr.TierBlock, ddReadMD5)
	r.RegisterFunc("fio", tr.TierBlock, fioAction)
	r.RegisterFunc("fio_verify", tr.TierBlock, fioVerify)
	r.RegisterFunc("mkfs", tr.TierBlock, mkfsAction)
	r.RegisterFunc("mount", tr.TierBlock, mountAction)
	r.RegisterFunc("umount", tr.TierBlock, umountAction)
	r.RegisterFunc("write_loop_bg", tr.TierBlock, writeLoopBg)
	r.RegisterFunc("stop_bg", tr.TierBlock, stopBg)
}

func ddSyncConv(mode string) (string, error) {
	switch mode {
	case "", "fsync":
		return "fsync", nil
	case "fdatasync":
		return "fdatasync", nil
	case "none":
		return "", nil
	default:
		return "", fmt.Errorf("unsupported sync_mode %q", mode)
	}
}

// ddWrite writes random data using dd, returns the md5 checksum.
func ddWrite(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	device := act.Params["device"]
	if device == "" {
		return nil, fmt.Errorf("dd_write: device param required")
	}
	bs := act.Params["bs"]
	if bs == "" {
		bs = "1M"
	}
	count := act.Params["count"]
	if count == "" {
		count = "1"
	}
	oflag := act.Params["oflag"]
	if oflag == "" {
		oflag = "direct"
	}
	syncConv, err := ddSyncConv(act.Params["sync_mode"])
	if err != nil {
		return nil, fmt.Errorf("dd_write: %w", err)
	}

	node, err := GetNode(actx, act.Node)
	if err != nil {
		return nil, err
	}

	// Generate random data to temp file, write to device, compute md5.
	tmpFile := tempPath(actx, "dd-data")
	if err := ensureTempRoot(ctx, node, actx); err != nil {
		return nil, fmt.Errorf("dd_write: %w", err)
	}
	genCmd := fmt.Sprintf("dd if=/dev/urandom of=%s bs=%s count=%s status=none", tmpFile, bs, count)
	_, stderr, code, err := node.RunRoot(ctx, genCmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("dd_write gen: code=%d stderr=%s err=%v", code, stderr, err)
	}

	writeCmd := fmt.Sprintf("dd if=%s of=%s bs=%s oflag=%s", tmpFile, device, bs, oflag)
	if syncConv != "" {
		writeCmd += fmt.Sprintf(" conv=%s", syncConv)
	}
	if seek := act.Params["seek"]; seek != "" {
		writeCmd += fmt.Sprintf(" seek=%s", seek)
	}
	writeCmd += " status=none"
	if actx.Log != nil {
		if syncConv == "" {
			actx.Log("  dd_write: device=%s bs=%s count=%s oflag=%s sync_mode=none", device, bs, count, oflag)
		} else {
			actx.Log("  dd_write: device=%s bs=%s count=%s oflag=%s sync_mode=%s", device, bs, count, oflag, syncConv)
		}
	}
	_, stderr, code, err = node.RunRoot(ctx, writeCmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("dd_write: code=%d sync_mode=%s stderr=%s err=%v", code, syncConvOrNone(syncConv), stderr, err)
	}

	md5Cmd := fmt.Sprintf("md5sum %s | cut -d' ' -f1", tmpFile)
	stdout, stderr, code, err := node.RunRoot(ctx, md5Cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("dd_write md5: code=%d stderr=%s err=%v", code, stderr, err)
	}
	node.Run(ctx, fmt.Sprintf("rm -f %s", tmpFile))

	md5 := strings.TrimSpace(stdout)
	if md5 == "" {
		return nil, fmt.Errorf("dd_write: empty md5")
	}

	return map[string]string{"value": md5}, nil
}

// ddReadMD5 reads from device using dd and returns md5.
func ddReadMD5(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	device := act.Params["device"]
	if device == "" {
		return nil, fmt.Errorf("dd_read_md5: device param required")
	}
	bs := act.Params["bs"]
	if bs == "" {
		bs = "1M"
	}
	count := act.Params["count"]
	if count == "" {
		count = "1"
	}

	node, err := GetNode(actx, act.Node)
	if err != nil {
		return nil, err
	}

	tmpFile := tempPath(actx, "dd-read")
	if err := ensureTempRoot(ctx, node, actx); err != nil {
		return nil, fmt.Errorf("dd_read_md5: %w", err)
	}
	readCmd := fmt.Sprintf("dd if=%s of=%s bs=%s count=%s iflag=direct status=none", device, tmpFile, bs, count)
	if skip := act.Params["skip"]; skip != "" {
		readCmd += fmt.Sprintf(" skip=%s", skip)
	}
	_, stderr, code, err := node.RunRoot(ctx, readCmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("dd_read_md5 read: code=%d stderr=%s err=%v", code, stderr, err)
	}

	cmd := fmt.Sprintf("md5sum %s | cut -d' ' -f1", tmpFile)
	stdout, stderr, code, err := node.RunRoot(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("dd_read_md5: code=%d stderr=%s err=%v", code, stderr, err)
	}
	node.Run(ctx, fmt.Sprintf("rm -f %s", tmpFile))

	md5 := strings.TrimSpace(stdout)
	if md5 == "" {
		return nil, fmt.Errorf("dd_read_md5: empty md5")
	}

	return map[string]string{"value": md5}, nil
}

func syncConvOrNone(syncConv string) string {
	if syncConv == "" {
		return "none"
	}
	return syncConv
}

func fioAction(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	device := act.Params["device"]
	if device == "" {
		return nil, fmt.Errorf("fio: device param required")
	}

	node, err := GetNode(actx, act.Node)
	if err != nil {
		return nil, err
	}

	rw := act.Params["rw"]
	if rw == "" {
		rw = "randwrite"
	}
	bs := act.Params["bs"]
	if bs == "" {
		bs = "4k"
	}
	iodepth := act.Params["iodepth"]
	if iodepth == "" {
		iodepth = "32"
	}
	runtime := act.Params["runtime"]
	if runtime == "" {
		runtime = "10"
	}
	name := act.Params["name"]
	if name == "" {
		name = "fio_test"
	}

	cmd := fmt.Sprintf("fio --name=%s --filename=%s --rw=%s --bs=%s --iodepth=%s --direct=1 --runtime=%s --time_based --output-format=json",
		name, device, rw, bs, iodepth, runtime)
	if size := act.Params["size"]; size != "" {
		cmd += fmt.Sprintf(" --size=%s", size)
	}
	stdout, stderr, code, err := node.RunRoot(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("fio: code=%d stderr=%s err=%v", code, stderr, err)
	}

	return map[string]string{"value": stdout}, nil
}

func fioVerify(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	device := act.Params["device"]
	if device == "" {
		return nil, fmt.Errorf("fio_verify: device param required")
	}

	node, err := GetNode(actx, act.Node)
	if err != nil {
		return nil, err
	}

	bs := act.Params["bs"]
	if bs == "" {
		bs = "4k"
	}
	size := act.Params["size"]
	if size == "" {
		size = "10M"
	}

	// Write with verify pattern, then read+verify.
	cmd := fmt.Sprintf("fio --name=verify --filename=%s --rw=write --bs=%s --size=%s --direct=1 --verify=crc32c --do_verify=1 --output-format=json",
		device, bs, size)
	stdout, stderr, code, err := node.RunRoot(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("fio_verify: code=%d stderr=%s err=%v", code, stderr, err)
	}

	return map[string]string{"value": stdout}, nil
}

func mkfsAction(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	device := act.Params["device"]
	if device == "" {
		return nil, fmt.Errorf("mkfs: device param required")
	}
	fstype := act.Params["fstype"]
	if fstype == "" {
		fstype = "ext4"
	}

	node, err := GetNode(actx, act.Node)
	if err != nil {
		return nil, err
	}

	cmd := fmt.Sprintf("mkfs.%s -F %s", fstype, device)
	_, stderr, code, err := node.RunRoot(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("mkfs: code=%d stderr=%s err=%v", code, stderr, err)
	}
	return nil, nil
}

func mountAction(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	device := act.Params["device"]
	if device == "" {
		return nil, fmt.Errorf("mount: device param required")
	}
	mountpoint := act.Params["mountpoint"]
	if mountpoint == "" {
		mountpoint = "/mnt/test"
	}

	node, err := GetNode(actx, act.Node)
	if err != nil {
		return nil, err
	}

	node.RunRoot(ctx, fmt.Sprintf("mkdir -p %s", mountpoint))
	_, stderr, code, err := node.RunRoot(ctx, fmt.Sprintf("mount %s %s", device, mountpoint))
	if err != nil || code != 0 {
		return nil, fmt.Errorf("mount: code=%d stderr=%s err=%v", code, stderr, err)
	}
	return map[string]string{"value": mountpoint}, nil
}

func umountAction(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	mountpoint := act.Params["mountpoint"]
	if mountpoint == "" {
		mountpoint = "/mnt/test"
	}

	node, err := GetNode(actx, act.Node)
	if err != nil {
		return nil, err
	}

	_, stderr, code, err := node.RunRoot(ctx, fmt.Sprintf("umount %s", mountpoint))
	if err != nil || code != 0 {
		return nil, fmt.Errorf("umount: code=%d stderr=%s err=%v", code, stderr, err)
	}
	return nil, nil
}

// writeLoopBg starts a background dd write loop. Returns PID.
// Params: device (required), bs (default: "4k"), oflag (default: "direct")
func writeLoopBg(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	device := act.Params["device"]
	if device == "" {
		return nil, fmt.Errorf("write_loop_bg: device param required")
	}
	bs := act.Params["bs"]
	if bs == "" {
		bs = "4k"
	}
	oflag := act.Params["oflag"]
	if oflag == "" {
		oflag = "direct"
	}

	node, err := GetNode(actx, act.Node)
	if err != nil {
		return nil, err
	}

	bgLog := tempPath(actx, "bg.log")
	if err := ensureTempRoot(ctx, node, actx); err != nil {
		return nil, fmt.Errorf("write_loop_bg: %w", err)
	}
	cmd := fmt.Sprintf("setsid bash -c 'while true; do dd if=/dev/urandom of=%s bs=%s count=1 oflag=%s conv=notrunc 2>/dev/null; done' &>%s & echo $!",
		device, bs, oflag, bgLog)
	stdout, stderr, code, err := node.RunRoot(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("write_loop_bg: code=%d stderr=%s err=%v", code, stderr, err)
	}

	pid := strings.TrimSpace(stdout)
	if pid == "" {
		return nil, fmt.Errorf("write_loop_bg: empty PID")
	}

	return map[string]string{"value": pid}, nil
}

// stopBg kills a background process by PID.
// Params: pid (required)
func stopBg(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	pid := act.Params["pid"]
	if pid == "" {
		return nil, fmt.Errorf("stop_bg: pid param required")
	}

	node, err := GetNode(actx, act.Node)
	if err != nil {
		return nil, err
	}

	cmd := fmt.Sprintf("kill %s; wait %s 2>/dev/null || true", pid, pid)
	node.RunRoot(ctx, cmd)

	return nil, nil
}

// ensureTempRoot creates the per-run temp directory on the remote node.
// Uses RunRoot so the directory is created with root privileges, ensuring
// subsequent RunRoot commands can write into it.
func ensureTempRoot(ctx context.Context, node interface{ RunRoot(context.Context, string) (string, string, int, error) }, actx *tr.ActionContext) error {
	if actx.TempRoot == "" {
		return nil
	}
	_, stderr, code, err := node.RunRoot(ctx, fmt.Sprintf("mkdir -p %s", actx.TempRoot))
	if err != nil || code != 0 {
		return fmt.Errorf("mkdir TempRoot %s: code=%d stderr=%s err=%v", actx.TempRoot, code, stderr, err)
	}
	return nil
}

// tempPath returns a path under the per-run temp root for the given suffix.
// Falls back to /tmp if TempRoot is empty (backward compat).
func tempPath(actx *tr.ActionContext, suffix string) string {
	root := actx.TempRoot
	if root == "" {
		root = "/tmp"
	}
	return root + "/sw-" + suffix
}
