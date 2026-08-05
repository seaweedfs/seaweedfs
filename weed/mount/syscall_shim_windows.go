package mount

// Windows has no fcntl lock types and no access-mode mask. The Linux values
// stand in: nothing on Windows feeds these, and the lock table only ever
// compares them against each other.
const (
	f_RDLCK   = 0
	f_WRLCK   = 1
	f_UNLCK   = 2
	o_ACCMODE = 0x3

	xattr_CREATE  = 1
	xattr_REPLACE = 2
)
