package redis_store

import (
	"fmt"
	"path/filepath"
	"sort"
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
		local did
		for i, v in ipairs(ARGV) do
			did=redis.call('hget', root, v)
			if did == false then
				did=redis.call('incr', dirMaxIdKey)
				redis.call('hset', root, v, did)
			end
			if i==1 then
				root=root .. v
			else
				root=root..'/'..v
			end
		end
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

//delete directory dirPath and its sub-directories or files recursively ;
//it's not this function's responsibility to check whether dirPath is en empty directory.
func (dm *DirectoryManager) DeleteDirectory(dirPath string) error {
	dirPath = embedded_filer.CleanFilePath(dirPath)
	if dirPath == "/" {
		return fmt.Errorf("Can not delete %s", dirPath)
	}
	dirPathParent := filepath.Dir(dirPath)
	dirPathName := filepath.Base(dirPath)
	/*
		lua comments:
			1. delete dirPath's sub-directories and files recursively
			2. delete dirPath itself from its parent dir

	*/
	script := redis.NewScript(`
		local delSubs
		local dirKeyPrefix=KEYS[1]
		local dirFileKeyPrefix=KEYS[2]
		local dirPathParent=ARGV[1]
		local dirPathName=ARGV[2]
		delSubs = function(dir) 
			local subs=redis.call('hgetall', dirKeyPrefix..dir); 
			for i, v in ipairs(subs) do 
				if i%2 ~= 0 then 
					redis.call('hdel', dirKeyPrefix..dir, v)
					local subd=dir..'/'..v; 
					delSubs(subd); 
				end 
			end 
			redis.call('del', dirFileKeyPrefix..dir)
		end
		delSubs(dirPathParent..'/'..dirPathName)
		redis.call('hdel', dirKeyPrefix..dirPathParent, dirPathName)
		return 0
	`)
	//we do not use lua to get dirPath's parent dir and its basename, so do it in golang
	err := script.Run(dm.Client, []string{dm.dirKeyPrefix, dm.dirFileKeyPrefix}, []string{dirPathParent, dirPathName}).Err()
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

after moving is done, it returns 0, -1 indicates that source dir is not found, -2 indicates that destination dir is not found
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

//return a dir's sub-directories, directories' name are sorted lexicographically
func (dm *DirectoryManager) ListDirectories(dirPath string) (dirNames []filer.DirectoryEntry, err error) {
	dirPath = embedded_filer.CleanFilePath(dirPath)
	result, le := dm.Client.HGetAllMap(dm.dirKeyPrefix + dirPath).Result()
	if le != nil {
		glog.Errorf("get sub-directories of %s error:%v", dirPath, err)
		return dirNames, le
	}
	//sort entries by directories' name
	keys := make(sort.StringSlice, len(result))
	i := 0
	for k, _ := range result {
		keys[i] = k
		i++
	}
	keys.Sort()
	for _, k := range keys {
		v := result[k]
		did, _ := strconv.Atoi(v)
		entry := filer.DirectoryEntry{Name: k, Id: filer.DirectoryId(did)}
		dirNames = append(dirNames, entry)
	}
	return dirNames, nil
}

//get the amount of directories under a directory
func (dm *DirectoryManager) GetSubDirectoriesNum(dirPath string) (int64, error) {
	dirPath = embedded_filer.CleanFilePath(dirPath)
	return dm.getSubItemsNum(dm.dirKeyPrefix + dirPath)
}

//use lua script to make directories and then put file for atomic
func (dm *DirectoryManager) PutFile(fullFileName string, fid string) error {
	fullFileName = embedded_filer.CleanFilePath(fullFileName)
	dirPath := filepath.Dir(fullFileName)
	fname := filepath.Base(fullFileName)
	parts := strings.Split(dirPath, "/")
	//'d' stands for directory. root must end with a slash
	root := dm.dirKeyPrefix + "/"
	script := redis.NewScript(`
		local root=KEYS[1]
		local dirMaxIdKey=KEYS[2]
		local dirFileKeyPrefix=KEYS[3]
		local fname=KEYS[4]
		local fid=KEYS[5]
		local dirPath=table.concat(ARGV, '/')
		local did
		for i, v in ipairs(ARGV) do
			did=redis.call('hget', root, v)
			if did == false then
				did=redis.call('incr', dirMaxIdKey)
				redis.call('hset', root, v, did)
			end
			if i==1 then
				root=root .. v
			else
				root=root..'/'..v
			end
		end
		redis.call('hset', dirFileKeyPrefix..'/'..dirPath, fname, fid)
		return did
	`)
	err := script.Run(dm.Client, []string{root, dm.dirMaxIdKey, dm.dirFileKeyPrefix, fname, fid}, parts[1:]).Err()
	if err != nil {
		glog.Errorln("redis eval put file script error:", err)
	}
	return err
}

func (dm *DirectoryManager) FindFile(fullFileName string) (fid string, err error) {
	fullFileName = embedded_filer.CleanFilePath(fullFileName)
	dirPath := filepath.Dir(fullFileName)
	fname := filepath.Base(fullFileName)
	result, err := dm.Client.HGet(dm.dirFileKeyPrefix+dirPath, fname).Result()
	if err == redis.Nil {
		err = nil
	}
	if err != nil {
		glog.Errorf("get file %s error:%v", fullFileName, err)
	}
	return result, err
}

func (dm *DirectoryManager) DeleteFile(fullFileName string) (fid string, err error) {
	fullFileName = embedded_filer.CleanFilePath(fullFileName)
	dirPath := filepath.Dir(fullFileName)
	fname := filepath.Base(fullFileName)
	script := redis.NewScript(`
		local dirPathKey=KEYS[1]
		local fname = ARGV[1]
		local fid=redis.call('hget', dirPathKey, fname)
		if fid ~= false then
			redis.call('hdel', dirPathKey, fname)
		end
		return fid
	`)
	result, err := script.Run(dm.Client, []string{dm.dirFileKeyPrefix + dirPath}, []string{fname}).Result()
	if err != nil {
		glog.Errorf("delete file %s error:%v\n", fullFileName, err)
	}
	fid, ok := result.(string)
	if !ok {
		glog.Errorf("convert result %v to string failed", result)
	}
	return fid, err
}

//list files under dirPath, use lastFileName and limit for pagination
//in fact, this implemention has a bug, you can not get first file of the first page;
//the api needs to be modified
func (dm *DirectoryManager) ListFiles(dirPath string, lastFileName string, limit int) (files []filer.FileEntry, err error) {

	dirPath = embedded_filer.CleanFilePath(dirPath)
	result, le := dm.Client.HGetAllMap(dm.dirFileKeyPrefix + dirPath).Result()
	if le != nil {
		glog.Errorf("get files of %s error:%v", dirPath, err)
		return files, le
	}
	//if the lastFileName argument is ok
	//when list first page, pass lastFileName an empty string
	if _, ok := result[lastFileName]; ok || lastFileName == "" {
		//sort entries by file names
		//when files amount is large, here may be slow
		nResult := len(result)
		keys := make(sort.StringSlice, nResult)
		i := 0
		for k, _ := range result {
			keys[i] = k
			i++
		}
		keys.Sort()
		index := -1
		if ok {
			index = keys.Search(lastFileName)
		}
		cnt := 0
		for _, k := range keys[index+1:] {
			fid := result[k]
			entry := filer.FileEntry{Name: k, Id: filer.FileId(fid)}
			files = append(files, entry)
			cnt++
			if cnt == limit {
				break
			}
		}
	}
	return files, le
}

//get the amount of files under a directory
func (dm *DirectoryManager) GetFilesNum(dirPath string) (int64, error) {
	dirPath = embedded_filer.CleanFilePath(dirPath)
	return dm.getSubItemsNum(dm.dirFileKeyPrefix + dirPath)
}

//get a hash key's fields number
func (dm *DirectoryManager) getSubItemsNum(key string) (int64, error) {
	result, err := dm.Client.HLen(key).Result()
	if err == redis.Nil {
		err = nil
	}
	if err != nil {
		glog.Errorf("hlen %s error:%v", key, err)
	}
	return result, err
}
