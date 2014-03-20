package operation

import (
	"code.google.com/p/weed-fs/go/util"
)

func DeleteFile(server string, fileId string) error {
	fileUrl, err := LookupFileId(server, fileId)
	if err != nil {
		return err
	}
	return util.Delete(fileUrl)
}
