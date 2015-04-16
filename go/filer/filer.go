package filer

type FileId string //file id in SeaweedFS

type FileEntry struct {
	Name string `json:"name,omitempty"` //file name without path
	Id   FileId `json:"fid,omitempty"`
}

type DirectoryId int32

type DirectoryEntry struct {
	Name string //dir name without path
	Id   DirectoryId
}

type Filer interface {
	CreateFile(fullFileName string, fid string) (err error)
	FindFile(fullFileName string) (fid string, err error)
	DeleteFile(fullFileName string) (fid string, err error)

	//Optional functions. embedded filer support these
	FindDirectory(dirPath string) (dirId DirectoryId, err error)
	ListDirectories(dirPath string) (dirs []DirectoryEntry, err error)
	ListFiles(dirPath string, lastFileName string, limit int) (files []FileEntry, err error)
	DeleteDirectory(dirPath string, recursive bool) (err error)
	Move(fromPath string, toPath string) (err error)
}
