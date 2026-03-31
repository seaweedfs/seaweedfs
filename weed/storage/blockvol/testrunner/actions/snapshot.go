package actions

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	tr "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner/infra"
)

// RegisterSnapshotActions registers snapshot and resize actions.
func RegisterSnapshotActions(r *tr.Registry) {
	r.RegisterFunc("snapshot_create", tr.TierBlock, snapshotCreate)
	r.RegisterFunc("snapshot_delete", tr.TierBlock, snapshotDelete)
	r.RegisterFunc("snapshot_list", tr.TierBlock, snapshotList)
	r.RegisterFunc("resize", tr.TierBlock, resizeAction)
	r.RegisterFunc("iscsi_rescan", tr.TierBlock, iscsiRescan)
	r.RegisterFunc("get_block_size", tr.TierBlock, getBlockSize)
	r.RegisterFunc("snapshot_export_s3", tr.TierBlock, snapshotExportS3)
	r.RegisterFunc("snapshot_import_s3", tr.TierBlock, snapshotImportS3)
}

func snapshotCreate(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	tgt, err := getHATarget(actx, act.Target)
	if err != nil {
		return nil, err
	}

	idStr := act.Params["id"]
	if idStr == "" {
		return nil, fmt.Errorf("snapshot_create: id param required")
	}
	id, err := strconv.ParseUint(idStr, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("snapshot_create: invalid id %q: %w", idStr, err)
	}

	if err := tgt.CreateSnapshot(ctx, uint32(id)); err != nil {
		return nil, fmt.Errorf("snapshot_create: %w", err)
	}
	actx.Log("  created snapshot %d", id)
	return nil, nil
}

func snapshotDelete(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	tgt, err := getHATarget(actx, act.Target)
	if err != nil {
		return nil, err
	}

	idStr := act.Params["id"]
	if idStr == "" {
		return nil, fmt.Errorf("snapshot_delete: id param required")
	}
	id, err := strconv.ParseUint(idStr, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("snapshot_delete: invalid id %q: %w", idStr, err)
	}

	if err := tgt.DeleteSnapshot(ctx, uint32(id)); err != nil {
		return nil, fmt.Errorf("snapshot_delete: %w", err)
	}
	actx.Log("  deleted snapshot %d", id)
	return nil, nil
}

func snapshotList(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	tgt, err := getHATarget(actx, act.Target)
	if err != nil {
		return nil, err
	}

	snaps, err := tgt.ListSnapshots(ctx)
	if err != nil {
		return nil, fmt.Errorf("snapshot_list: %w", err)
	}

	actx.Log("  snapshots: %d entries", len(snaps))
	return map[string]string{"value": strconv.Itoa(len(snaps))}, nil
}

func resizeAction(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	tgt, err := getHATarget(actx, act.Target)
	if err != nil {
		return nil, err
	}

	var newSizeBytes uint64
	if s := act.Params["new_size_bytes"]; s != "" {
		newSizeBytes, err = strconv.ParseUint(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("resize: invalid new_size_bytes %q: %w", s, err)
		}
	} else if s := act.Params["new_size"]; s != "" {
		newSizeBytes, err = parseHumanSize(s)
		if err != nil {
			return nil, fmt.Errorf("resize: invalid new_size %q: %w", s, err)
		}
	} else {
		return nil, fmt.Errorf("resize: new_size or new_size_bytes param required")
	}

	if err := tgt.Resize(ctx, newSizeBytes); err != nil {
		return nil, fmt.Errorf("resize: %w", err)
	}
	actx.Log("  resized to %d bytes", newSizeBytes)
	return nil, nil
}

func iscsiRescan(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := GetNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("iscsi_rescan: %w", err)
	}

	_, stderr, code, err := node.RunRoot(ctx, "iscsiadm -m session -R")
	if err != nil || code != 0 {
		return nil, fmt.Errorf("iscsi_rescan: code=%d stderr=%s err=%v", code, stderr, err)
	}

	// Give kernel time to update block device size.
	select {
	case <-time.After(2 * time.Second):
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	actx.Log("  iSCSI sessions rescanned")
	return nil, nil
}

func getBlockSize(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	device := act.Params["device"]
	if device == "" {
		return nil, fmt.Errorf("get_block_size: device param required")
	}

	node, err := GetNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("get_block_size: %w", err)
	}

	cmd := fmt.Sprintf("blockdev --getsize64 %s", device)
	stdout, stderr, code, err := node.RunRoot(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("get_block_size: code=%d stderr=%s err=%v", code, stderr, err)
	}

	size := strings.TrimSpace(stdout)
	actx.Log("  %s size: %s bytes", device, size)
	return map[string]string{"value": size}, nil
}

// parseHumanSize converts human-readable sizes like "100M", "1G", "512K" to bytes.
func parseHumanSize(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return 0, fmt.Errorf("empty size")
	}

	multiplier := uint64(1)
	suffix := s[len(s)-1]
	switch suffix {
	case 'K', 'k':
		multiplier = 1024
		s = s[:len(s)-1]
	case 'M', 'm':
		multiplier = 1024 * 1024
		s = s[:len(s)-1]
	case 'G', 'g':
		multiplier = 1024 * 1024 * 1024
		s = s[:len(s)-1]
	case 'T', 't':
		multiplier = 1024 * 1024 * 1024 * 1024
		s = s[:len(s)-1]
	}

	val, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, err
	}
	return val * multiplier, nil
}

// snapshotExportS3 exports a snapshot from a target to an S3 bucket.
// Params: bucket, key_prefix, s3_endpoint, s3_access_key, s3_secret_key, s3_region, snapshot_id (optional).
// Returns: manifest_key, data_key, size_bytes, sha256.
func snapshotExportS3(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	tgt, err := getHATarget(actx, act.Target)
	if err != nil {
		return nil, err
	}

	opts := infra.ExportS3Opts{
		Bucket:      act.Params["bucket"],
		KeyPrefix:   act.Params["key_prefix"],
		S3Endpoint:  act.Params["s3_endpoint"],
		S3AccessKey: act.Params["s3_access_key"],
		S3SecretKey: act.Params["s3_secret_key"],
		S3Region:    act.Params["s3_region"],
	}
	if opts.Bucket == "" || opts.S3Endpoint == "" {
		return nil, fmt.Errorf("snapshot_export_s3: bucket and s3_endpoint required")
	}
	if idStr := act.Params["snapshot_id"]; idStr != "" {
		id, err := strconv.ParseUint(idStr, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("snapshot_export_s3: invalid snapshot_id %q: %w", idStr, err)
		}
		opts.SnapshotID = uint32(id)
	}

	result, err := tgt.ExportSnapshotS3(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("snapshot_export_s3: %w", err)
	}

	actx.Log("  exported to s3://%s/%s (%d bytes, sha256=%s)", opts.Bucket, result.DataKey, result.SizeBytes, result.SHA256)
	out := map[string]string{
		"value": result.SHA256,
	}
	if act.SaveAs != "" {
		actx.Vars[act.SaveAs+"_manifest_key"] = result.ManifestKey
		actx.Vars[act.SaveAs+"_data_key"] = result.DataKey
		actx.Vars[act.SaveAs+"_size_bytes"] = strconv.FormatUint(result.SizeBytes, 10)
		actx.Vars[act.SaveAs+"_sha256"] = result.SHA256
	}
	return out, nil
}

// snapshotImportS3 imports a snapshot from an S3 bucket into a target.
// Params: bucket, manifest_key, s3_endpoint, s3_access_key, s3_secret_key, s3_region, allow_overwrite.
// Returns: size_bytes, sha256.
func snapshotImportS3(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	tgt, err := getHATarget(actx, act.Target)
	if err != nil {
		return nil, err
	}

	opts := infra.ImportS3Opts{
		Bucket:      act.Params["bucket"],
		ManifestKey: act.Params["manifest_key"],
		S3Endpoint:  act.Params["s3_endpoint"],
		S3AccessKey: act.Params["s3_access_key"],
		S3SecretKey: act.Params["s3_secret_key"],
		S3Region:    act.Params["s3_region"],
	}
	if opts.Bucket == "" || opts.ManifestKey == "" || opts.S3Endpoint == "" {
		return nil, fmt.Errorf("snapshot_import_s3: bucket, manifest_key, and s3_endpoint required")
	}
	if act.Params["allow_overwrite"] == "true" {
		opts.AllowOverwrite = true
	}

	result, err := tgt.ImportSnapshotS3(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("snapshot_import_s3: %w", err)
	}

	actx.Log("  imported %d bytes (sha256=%s)", result.SizeBytes, result.SHA256)
	out := map[string]string{
		"value": result.SHA256,
	}
	if act.SaveAs != "" {
		actx.Vars[act.SaveAs+"_size_bytes"] = strconv.FormatUint(result.SizeBytes, 10)
		actx.Vars[act.SaveAs+"_sha256"] = result.SHA256
	}
	return out, nil
}
