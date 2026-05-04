package util

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

const maxFilenameLength = 255

func TestFolderWritable(folder string) (err error) {
	fileInfo, err := os.Stat(folder)
	if err != nil {
		return err
	}
	if !fileInfo.IsDir() {
		return errors.New("Not a valid folder!")
	}
	perm := fileInfo.Mode().Perm()
	glog.V(0).Infoln("Folder", folder, "Permission:", perm)
	if 0200&perm != 0 {
		return nil
	}
	return errors.New("Not writable!")
}

func GetFileSize(file *os.File) (size int64, err error) {
	var fi os.FileInfo
	if fi, err = file.Stat(); err == nil {
		size = fi.Size()
	}
	return
}

func FileExists(filename string) bool {

	_, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return true

}

func FolderExists(folder string) bool {

	fileInfo, err := os.Stat(folder)
	if err != nil {
		return false
	}
	return fileInfo.IsDir()

}

func CheckFile(filename string) (exists, canRead, canWrite bool, modTime time.Time, fileSize int64) {
	exists = true
	fi, err := os.Stat(filename)
	if os.IsNotExist(err) {
		exists = false
		return
	}
	if err != nil {
		glog.Errorf("check %s: %v", filename, err)
		return
	}
	if fi.Mode()&0400 != 0 {
		canRead = true
	}
	if fi.Mode()&0200 != 0 {
		canWrite = true
	}
	modTime = fi.ModTime()
	fileSize = fi.Size()
	return
}

// ResolvePath expands a leading "~", "~/", or "~username/" in path to the
// corresponding home directory, mirroring shell tilde expansion. A path
// without a leading "~" or whose tilde cannot be resolved is returned
// unchanged so callers can pass any user-supplied path through this helper
// without worrying about non-tilde inputs or lookup failures.
func ResolvePath(path string) string {

	if !strings.HasPrefix(path, "~") {
		return path
	}

	if path == "~" {
		if usr, err := user.Current(); err == nil {
			return usr.HomeDir
		}
		return path
	}

	if strings.HasPrefix(path, "~/") {
		if usr, err := user.Current(); err == nil {
			return filepath.Join(usr.HomeDir, path[2:])
		}
		return path
	}

	// "~username" or "~username/rest"
	rest := ""
	name := path[1:]
	if i := strings.IndexByte(name, '/'); i >= 0 {
		rest = name[i+1:]
		name = name[:i]
	}
	usr, err := user.Lookup(name)
	if err != nil {
		return path
	}
	if rest == "" {
		return usr.HomeDir
	}
	return filepath.Join(usr.HomeDir, rest)
}

func FileNameBase(filename string) string {
	lastDotIndex := strings.LastIndex(filename, ".")
	if lastDotIndex < 0 {
		return filename
	}
	return filename[:lastDotIndex]
}

func ToShortFileName(path string) string {
	fileName := filepath.Base(path)
	if fileNameBytes := []byte(fileName); len(fileNameBytes) > maxFilenameLength {
		shaStr := fmt.Sprintf("%x", sha256.Sum256(fileNameBytes))
		fileNameBase := FileNameBase(fileName)
		fileExt := fileName[len(fileNameBase):]
		fileNameBaseBates := bytes.ToValidUTF8([]byte(fileNameBase)[:maxFilenameLength-len([]byte(fileExt))-8], []byte{})
		shortFileName := string(fileNameBaseBates) + shaStr[len(shaStr)-8:]
		return filepath.Join(filepath.Dir(path), shortFileName) + fileExt
	}
	return path
}

// Copied from os.WriteFile(), adding file sync.
// see https://github.com/golang/go/issues/20599
func WriteFile(name string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err1 := f.Sync(); err1 != nil && err == nil {
		err = err1
	}
	if err1 := f.Close(); err1 != nil && err == nil {
		err = err1
	}
	return err
}
