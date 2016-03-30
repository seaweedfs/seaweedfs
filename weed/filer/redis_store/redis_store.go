package redis_store

import (
	redis "gopkg.in/redis.v2"
)

type RedisStore struct {
	Client *redis.Client
}

func NewRedisStore(hostPort string, password string, database int) *RedisStore {
	client := redis.NewTCPClient(&redis.Options{
		Addr:     hostPort,
		Password: password,
		DB:       int64(database),
	})
	return &RedisStore{Client: client}
}

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
