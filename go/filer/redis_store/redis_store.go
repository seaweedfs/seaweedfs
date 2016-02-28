package redis_store

import (
	"fmt"
	"path/filepath"

	"github.com/chrislusf/seaweedfs/go/filer"
	redis "gopkg.in/redis.v2"
)

type RedisStore struct {
	Client *redis.Client
	dm     *DirectoryManager
}

func NewRedisStore(hostPort string, database int) *RedisStore {
	client := redis.NewTCPClient(&redis.Options{
		Addr:     hostPort,
		Password: "", // no password set
		DB:       int64(database),
	})
	dm := InitDirectoryManger(client)
	return &RedisStore{Client: client, dm: dm}
}

func (s *RedisStore) Get(fullFileName string) (fid string, err error) {
	return s.dm.FindFile(fullFileName)
}
func (s *RedisStore) Put(fullFileName string, fid string) (err error) {
	return s.dm.PutFile(fullFileName, fid)
}

// Currently the fid is returned
func (s *RedisStore) Delete(fullFileName string) (fid string, err error) {
	return s.dm.DeleteFile(fullFileName)
}

func (s *RedisStore) Close() {
	if s.Client != nil {
		s.Client.Close()
	}
}

func (s *RedisStore) FindDirectory(dirPath string) (dirId filer.DirectoryId, err error) {
	return s.dm.FindDirectory(dirPath)
}
func (s *RedisStore) ListDirectories(dirPath string) (dirs []filer.DirectoryEntry, err error) {
	return s.dm.ListDirectories(dirPath)
}
func (s *RedisStore) ListFiles(dirPath string, lastFileName string, limit int) (files []filer.FileEntry, err error) {
	return s.dm.ListFiles(dirPath, lastFileName, limit)
}
func (s *RedisStore) DeleteDirectory(dirPath string, recursive bool) (err error) {
	if recursive {
		nDirs, _ := s.dm.GetSubDirectoriesNum(dirPath)
		if nDirs > 0 {
			return fmt.Errorf("Fail to delete directory %s: %d sub directories found!", dirPath, nDirs)
		}
		nFiles, _ := s.dm.GetFilesNum(dirPath)
		if nFiles > 0 {
			return fmt.Errorf("Fail to delete directory %s: %d files found!", dirPath, nFiles)

		}

	} else {
		err = s.dm.DeleteDirectory(dirPath)
	}
	return
}

/*
Move a folder or a file, with 4 Use cases:
mv fromDir toNewDir
mv fromDir toOldDir
mv fromFile toDir
mv fromFile toFile
*/
func (s *RedisStore) Move(fromPath string, toPath string) error {
	//first check whether fromPath is a directory
	fromDid, _ := s.dm.FindDirectory(fromPath)
	if fromDid > 0 {
		toDid, _ := s.dm.FindDirectory(toPath)
		if toDid > 0 {
			//move under an existing dir
			return s.dm.MoveUnderDirectory(fromPath, toPath, "")
		}
		//move to a new dir
		return s.dm.MoveUnderDirectory(fromPath, filepath.Dir(toPath), filepath.Base(toPath))
	}
	//whether fromPath is a file path
	if fid, err := s.dm.DeleteFile(fromPath); err == nil {
		toDid, _ := s.dm.FindDirectory(toPath)
		if toDid > 0 {
			//move file under an existing dir
			return s.dm.PutFile(filepath.Join(toPath, filepath.Base(fromPath)), fid)
		}
		//move to a folder with a new name
		return s.dm.PutFile(toPath, fid)
	}
	return fmt.Errorf("File %s is not found!", fromPath)
}
