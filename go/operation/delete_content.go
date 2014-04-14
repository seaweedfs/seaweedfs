package operation

import (
	"code.google.com/p/weed-fs/go/util"
)

func DeleteFile(master string, fileId string) error {
	fileUrl, err := LookupFileId(master, fileId)
	if err != nil {
		return err
	}
	return util.Delete(fileUrl)
}
