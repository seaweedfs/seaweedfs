package redis_store

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/chrislusf/seaweedfs/go/filer"
	"github.com/chrislusf/seaweedfs/go/filer/embedded_filer"
	"github.com/chrislusf/seaweedfs/go/glog"
	redis "gopkg.in/redis.v2"
)

type DirectoryManager struct {
	Client           *redis.Client
	dirMaxIdKey      string
	dirKeyPrefix     string
	dirFileKeyPrefix string
}

func InitDirectoryManger(client *redis.Client) *DirectoryManager {
	dirMaxIdKey := "swfs:dir-max-id"
	dm := &DirectoryManager{
		Client:           client,
		dirMaxIdKey:      dirMaxIdKey,
		dirKeyPrefix:     "d:",
		dirFileKeyPrefix: "df:"}

	dm.initDirectoryId()
	return dm
}

//check whether directory id exists in redis, if not then init to zero.
func (dm *DirectoryManager) initDirectoryId() error {
	err := dm.Client.Get(dm.dirMaxIdKey).Err()
	if err == redis.Nil {
		err = dm.Client.Set(dm.dirMaxIdKey, "1").Err()
		if err != nil {
			glog.Errorln("init dir max id error:", err)
		}
	} else if err != nil {
		glog.Errorln("get dir max id error:", err)
	}
	return err
}

//use redis lua script to make all parent dirs in the dirPath, like linux command `mkdir -p`
func (dm *DirectoryManager) MakeDirectory(dirPath string) (filer.DirectoryId, error) {
	dirPath = embedded_filer.CleanFilePath(dirPath)
	if dirPath == "/" {
		return 1, nil
	}
	parts := strings.Split(dirPath, "/")
	//'d' stands for directory. root must end with a slash
	root := dm.dirKeyPrefix + "/"
	script := redis.NewScript(`
		local root=KEYS[1]
		local dirMaxIdKey=KEYS[2]
		local did, exists
		for i, v in ipairs(ARGV) do
			exists=redis.call('hexists', root, v)
			if exists == 0 then
				did=redis.call('incr', dirMaxIdKey)
				redis.call('hset', root, v, did)
			end
			if i==1 then
				root=root .. v
			else
				root=root..'/'..v
			end
		end
		redis.call('set', 'last-dir-id', did)
		return did
	`)
	result, err := script.Run(dm.Client, []string{root, dm.dirMaxIdKey}, parts[1:]).Result()
	if err != nil {
		glog.Errorln("redis eval make directory script error:", err)
		return 0, err
	}
	did, ok := result.(int64)
	if !ok {
		glog.Errorln("convert result:", result, " get:", did)
	}
	return filer.DirectoryId(did), err
}

//delete directory dirPath and its sub-directories recursively;
//it's not this function's responsibility to check whether dirPath is empty.
func (dm *DirectoryManager) DeleteDirectory(dirPath string) error {
	dirPath = embedded_filer.CleanFilePath(dirPath)
	if dirPath == "/" {
		return fmt.Errorf("Can not delete %s", dirPath)
	}
	script := redis.NewScript(`
		local delSubs
		delSubs = function(dir) 
			local subs=redis.call('hgetall', dir); 
			for i, v in ipairs(subs) do 
				if i%2 ~= 0 then 
					local subd=dir..'/'..v; 
					delSubs(subd); 
				end 
			end 
		end
		delSubs(KEYS[1])
	`)
	err := script.Run(dm.Client, []string{dm.dirKeyPrefix + dirPath}, nil).Err()
	return err
}

func (dm *DirectoryManager) FindDirectory(dirPath string) (dirId filer.DirectoryId, err error) {
	entry, err := dm.findDirectory(dirPath)
	return entry.Id, err
}

func (dm *DirectoryManager) findDirectory(dirPath string) (filer.DirectoryEntry, error) {
	dirPath = embedded_filer.CleanFilePath(dirPath)
	if dirPath == "/" {
		return filer.DirectoryEntry{Name: "/", Id: 1}, nil
	}
	basePart := filepath.Base(dirPath)
	parentPart := filepath.Dir(dirPath)
	did, err := dm.Client.HGet(dm.dirKeyPrefix+parentPart, basePart).Int64()
	//adjust redis.Nil to golang nil
	if err == redis.Nil {
		err = nil
	}
	if err != nil {
		return filer.DirectoryEntry{}, err
	}
	return filer.DirectoryEntry{Name: basePart, Id: filer.DirectoryId(did)}, nil
}

/*
moving is finished by following steps:
1.get old dir's directory id
2.delete old dir from its parent's hash table
3.add old dir's directory id into new parent dir's hash table, if newName is not empty, use it
4.if the full old dir path key exists(its a non-empty directory), rename it to full new dir path key
*/
func (dm *DirectoryManager) MoveUnderDirectory(oldDirPath string, newParentDirPath string, newName string) error {
	oldDirPath = embedded_filer.CleanFilePath(oldDirPath)
	oldDirName := filepath.Base(oldDirPath) //old dir's name without path
	oldParentDir := filepath.Dir(oldDirPath)

	newParentDirPath = embedded_filer.CleanFilePath(newParentDirPath)
	newParentDirName := filepath.Base(newParentDirPath)  // new parent dir's name without path
	newParentParentDir := filepath.Dir(newParentDirPath) //newParentDirPath's parent directory

	/*
		lua script comments:
			suppose oldDirPathKey is d:/a/b/c, newParentDirPathKey is d:/a/b/d, newName is "";
			then oldParentDirKey is d:/a/b, oldDirName is c, and
			newParentParentDirKey is d:/a/b, newParentDirName is d;
			newTargetName is c
	*/
	script := redis.NewScript(`
		local oldDirPathKey=KEYS[1]
		local newParentDirPathKey=KEYS[2]
		local oldParentDirKey=ARGV[1]
		local oldDirName=ARGV[2]
		local newParentParentDirKey=ARGV[3]
		local newParentDirName=ARGV[4]
		local newTargetName=ARGV[5]
		local oldDirId=redis.call('hget', oldParentDirKey, oldDirName)
		local newParentDirId=redis.call('hget', newParentParentDirKey, newParentDirName)
		if oldDirId == false then
			return -1
		end
		if newParentDirId == false then
			return -2
		end
		redis.call('hdel', oldParentDirKey, oldDirName)
		redis.call('hset', newParentDirPathKey, newTargetName, oldDirId)
		local exists=redis.call('exists', oldDirPathKey)
		if exists==1 then
			redis.call('rename', oldDirPathKey, newParentDirPathKey..'/'..newTargetName)
		end
		return 0
	`)
	if newName == "" {
		newName = oldDirName
	}
	result, err := script.Run(
		dm.Client,
		[]string{
			dm.dirKeyPrefix + oldDirPath,
			dm.dirKeyPrefix + newParentDirPath},
		[]string{
			dm.dirKeyPrefix + oldParentDir,
			oldDirName,
			dm.dirKeyPrefix + newParentParentDir,
			newParentDirName, newName}).Result()
	if err != nil {
		glog.Errorln("redis eval move directory script error:", err)
	} else {
		ret, ok := result.(int64)
		if !ok {
			glog.Errorln("convert result:", result, " get:", ret)
		}
		if ret == -1 {
			err = fmt.Errorf("src dir: %s not exists", oldDirPath)
		} else if ret == -2 {
			err = fmt.Errorf("dest dir: %s not exists", newParentDirPath)
		}
	}
	return err
}

func (dm *DirectoryManager) ListDirectories(dirPath string) (dirNames []filer.DirectoryEntry, err error) {
	dirPath = embedded_filer.CleanFilePath(dirPath)
	result, le := dm.Client.HGetAllMap(dm.dirKeyPrefix + dirPath).Result()
	if le != nil {
		glog.Errorf("get sub-directories of %s error:%v", dirPath, err)
		return dirNames, le
	}
	for k, v := range result {
		did, _ := strconv.Atoi(v)
		entry := filer.DirectoryEntry{Name: k, Id: filer.DirectoryId(did)}
		dirNames = append(dirNames, entry)
	}
	return dirNames, nil
}
