package redis_store

import (
	"github.com/chrislusf/seaweedfs/go/filer"
	"github.com/chrislusf/seaweedfs/go/filer/flat_namespace"
	redis "gopkg.in/redis.v2"
)

type RedisStore struct {
	Client *redis.Client
}

func NewRedisStore(hostPort string, database int) *RedisStore {
	client := redis.NewTCPClient(&redis.Options{
		Addr:     hostPort,
		Password: "", // no password set
		DB:       int64(database),
	})
	return &RedisStore{Client: client}
}

/*
func (s *RedisStore) Get(fullFileName string) (fid string, err error) {
	fid, err = s.Client.Get(fullFileName).Result()
	if err == redis.Nil {
		err = nil
	}
	return fid, err
}
func (s *RedisStore) Put(fullFileName string, fid string) (err error) {
	_, err = s.Client.Set(fullFileName, fid).Result()
	if err == redis.Nil {
		err = nil
	}
	return err
}

// Currently the fid is not returned
func (s *RedisStore) Delete(fullFileName string) (fid string, err error) {
	_, err = s.Client.Del(fullFileName).Result()
	if err == redis.Nil {
		err = nil
	}
	return "", err
}
*/
func (s *RedisStore) Get(fullFileName string) (fid string, err error) {
	fid, err = s.Client.Get(fullFileName).Result()
	if err == redis.Nil {
		err = nil
	}
	return fid, err
}
func (s *RedisStore) Put(fullFileName string, fid string) (err error) {
	_, err = s.Client.Set(fullFileName, fid).Result()
	if err == redis.Nil {
		err = nil
	}
	return err
}

// Currently the fid is not returned
func (s *RedisStore) Delete(fullFileName string) (fid string, err error) {
	_, err = s.Client.Del(fullFileName).Result()
	if err == redis.Nil {
		err = nil
	}
	return "", err
}

func (s *RedisStore) Close() {
	if s.Client != nil {
		s.Client.Close()
	}
}

func (c *RedisStore) FindDirectory(dirPath string) (dirId filer.DirectoryId, err error) {
	return 0, flat_namespace.ErrNotImplemented
}
func (c *RedisStore) ListDirectories(dirPath string) (dirs []filer.DirectoryEntry, err error) {
	return nil, flat_namespace.ErrNotImplemented
}
func (c *RedisStore) ListFiles(dirPath string, lastFileName string, limit int) (files []filer.FileEntry, err error) {
	return nil, flat_namespace.ErrNotImplemented
}
func (c *RedisStore) DeleteDirectory(dirPath string, recursive bool) (err error) {
	return flat_namespace.ErrNotImplemented
}

func (c *RedisStore) Move(fromPath string, toPath string) error {
	return flat_namespace.ErrNotImplemented
}
