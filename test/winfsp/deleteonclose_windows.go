package winfsp

import "golang.org/x/sys/windows"

// createDeleteOnClose opens a new file that Windows removes when the last
// handle to it closes. Installers and editors use this for temporaries, and
// os has no way to ask for it.
func createDeleteOnClose(path string) (windows.Handle, error) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return windows.InvalidHandle, err
	}
	return windows.CreateFile(
		p,
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil,
		windows.CREATE_NEW,
		windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_FLAG_DELETE_ON_CLOSE,
		0,
	)
}

func closeHandle(h windows.Handle) error { return windows.CloseHandle(h) }
