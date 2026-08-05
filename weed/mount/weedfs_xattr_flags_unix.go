//go:build !windows && !freebsd

package mount

import sys "golang.org/x/sys/unix"

const (
	xattr_CREATE  = sys.XATTR_CREATE
	xattr_REPLACE = sys.XATTR_REPLACE
)
