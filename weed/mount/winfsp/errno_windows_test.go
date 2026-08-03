package winfsp

import (
	"testing"

	cgofuse "github.com/winfsp/cgofuse/fuse"
)

// The errno values are spelled out so the table stays portable and testable on
// any runner. This pins them to what cgofuse actually decodes, so a divergence
// is a failing test rather than an operation reporting an unrelated error.
func TestErrnoValuesMatchCgofuse(t *testing.T) {
	for _, c := range []struct {
		name string
		ours int
		want int
	}{
		{"EPERM", ePERM, cgofuse.EPERM},
		{"ENOENT", eNOENT, cgofuse.ENOENT},
		{"EINTR", eINTR, cgofuse.EINTR},
		{"EIO", eIO, cgofuse.EIO},
		{"ENXIO", eNXIO, cgofuse.ENXIO},
		{"EBADF", eBADF, cgofuse.EBADF},
		{"EAGAIN", eAGAIN, cgofuse.EAGAIN},
		{"ENOMEM", eNOMEM, cgofuse.ENOMEM},
		{"EACCES", eACCES, cgofuse.EACCES},
		{"EBUSY", eBUSY, cgofuse.EBUSY},
		{"EEXIST", eEXIST, cgofuse.EEXIST},
		{"EXDEV", eXDEV, cgofuse.EXDEV},
		{"ENODEV", eNODEV, cgofuse.ENODEV},
		{"ENOTDIR", eNOTDIR, cgofuse.ENOTDIR},
		{"EISDIR", eISDIR, cgofuse.EISDIR},
		{"EINVAL", eINVAL, cgofuse.EINVAL},
		{"ENFILE", eNFILE, cgofuse.ENFILE},
		{"EMFILE", eMFILE, cgofuse.EMFILE},
		{"EFBIG", eFBIG, cgofuse.EFBIG},
		{"ENOSPC", eNOSPC, cgofuse.ENOSPC},
		{"ESPIPE", eSPIPE, cgofuse.ESPIPE},
		{"EROFS", eROFS, cgofuse.EROFS},
		{"EMLINK", eMLINK, cgofuse.EMLINK},
		{"EPIPE", ePIPE, cgofuse.EPIPE},
		{"ERANGE", eRANGE, cgofuse.ERANGE},
		{"ENAMETOOLONG", eNAMETOOLONG, cgofuse.ENAMETOOLONG},
		{"ENOSYS", eNOSYS, cgofuse.ENOSYS},
		{"ENOTEMPTY", eNOTEMPTY, cgofuse.ENOTEMPTY},
		{"ELOOP", eLOOP, cgofuse.ELOOP},
		{"ENODATA", eNODATA, cgofuse.ENODATA},
		{"ENOTSUP", eNOTSUP, cgofuse.ENOTSUP},
	} {
		if c.ours != c.want {
			t.Errorf("%s = %d, cgofuse uses %d", c.name, c.ours, c.want)
		}
	}
}
