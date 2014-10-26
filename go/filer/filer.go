package filer

type FileId string //file id on weedfs

type FileEntry struct {
	Name string `json:"name,omitempty"` //file name without path
	Id   FileId `json:"fid,omitempty"`
}

type Filer interface {
	CreateFile(filePath string, fid string) (err error)
	FindFile(filePath string) (fid string, err error)
	FindDirectory(dirPath string) (dirId DirectoryId, err error)
	ListDirectories(dirPath string) (dirs []DirectoryEntry, err error)
	ListFiles(dirPath string, lastFileName string, limit int) (files []FileEntry, err error)
	DeleteDirectory(dirPath string, recursive bool) (err error)
	DeleteFile(filePath string) (fid string, err error)
	Move(fromPath string, toPath string) (err error)
}
