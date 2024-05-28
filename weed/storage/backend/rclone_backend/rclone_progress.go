//go:build rclone
// +build rclone

package rclone_backend

import "github.com/rclone/rclone/fs/accounting"

type ProgressReader struct {
	acc *accounting.Account
	tr  *accounting.Transfer
	fn  func(progressed int64, percentage float32) error
}

func (pr *ProgressReader) Read(p []byte) (n int, err error) {
	n, err = pr.acc.Read(p)
	if err != nil {
		return
	}
	snap := pr.tr.Snapshot()
	err = pr.fn(snap.Bytes, 100*float32(snap.Bytes)/float32(snap.Size))
	return
}
