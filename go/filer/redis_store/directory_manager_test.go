package redis_store

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	redis "gopkg.in/redis.v2"
)

var dm *DirectoryManager = nil

func TestDirectory(t *testing.T) {
	//root dir
	did, err := dm.MakeDirectory("/")
	if err != nil {
		t.Errorf("make root dir error:%v\n", err)
	}
	if did != 1 {
		t.Errorf("root dir's id is not 1!")
	}
	//make some dirs
	dir := "/a/b/c"
	did, err = dm.MakeDirectory(dir)
	if err != nil {
		t.Errorf("make dirs:%s, error:%v\n", dir, err)
	}
	if did != 4 {
		t.Errorf("dir:%s's id is %d, 4 is expected!", dir, did)
	}
	//check its parent dir
	parentDir := filepath.Dir(dir)
	did, err = dm.FindDirectory(parentDir)
	if err != nil {
		t.Errorf("get %s's parent dir %s, error:%v\n", dir, parentDir, err)
	}
	if did != 3 {
		t.Errorf("dir:%s's id is %d, 3 is expected!", parentDir, did)
	}
	//make another dir with same parent
	dir = "/a/b/d"
	did, err = dm.MakeDirectory(dir)
	if err != nil {
		t.Errorf("make dirs:%s, error:%v\n", dir, err)
	}
	if did != 5 {
		t.Errorf("dir:%s's id is %d, 5 is expected!", dir, did)
	}
	//check its parent dir
	parentDir = filepath.Dir(dir)
	did, err = dm.FindDirectory(parentDir)
	if err != nil {
		t.Errorf("get %s's parent dir %s, error:%v\n", dir, parentDir, err)
	}
	if did != 3 {
		t.Errorf("dir:%s's id is %d, 3 is expected!", parentDir, did)
	}
	//find /a
	dir = "/a"
	did, err = dm.FindDirectory(dir)
	if err != nil {
		t.Errorf("find dir %s, error:%v\n", dir, err)
	}
	if did != 2 {
		t.Errorf("dir:%s's id is %d, 2 is expected!", dir, did)
	}
	//make /a/b/c/e, so /a/b/c has a sub-directory
	dir = "/a/b/c/e"
	did, err = dm.MakeDirectory(dir)
	if err != nil {
		t.Errorf("make dirs:%s, error:%v\n", dir, err)
	}
	if did != 6 {
		t.Errorf("dir:%s's id is %d, 6 is expected!", dir, did)
	}

	/*
	 *
	 *	move to an existing dir
	 *
	 *
	 */

	//move /a/b/c under /a/b/d
	from := "/a/b/c"
	to := "/a/b/d"
	err = dm.MoveUnderDirectory(from, to, "")
	if err != nil {
		t.Errorf("move %s to %s error:%v", from, to, err)
	}
	//now /a/b/c should not exist
	did, err = dm.FindDirectory(from)
	if err != nil {
		t.Errorf("find from dir %s error:%v", from, err)
	}
	if did != 0 {
		t.Errorf("%s still exists after moved under %s", from, to)
	}
	//now /a/b/d/c should exist
	dir = filepath.Join(to, filepath.Base(from))
	did, err = dm.FindDirectory(dir)
	if did != 4 {
		t.Errorf("new dir %s has a wrong dir id:%d, 4 is expected", dir, did)
	}
	//now /a/b/d/c/e also should exist, this indicates c is moved under /a/b/d entirely, include its sub-directories
	dir = "/a/b/d/c/e"
	did, err = dm.FindDirectory(dir)
	if did != 6 {
		t.Errorf("new dir %s has a wrong dir id:%d, 6 is expected", dir, did)
	}

	/*
	 *
	 * move to an new dir
	 *
	 */

	//now move /a/b/d/c to /a/b/f, f is a new dir, this means c will be changed to f, but dir id will not be changed
	from = "/a/b/d/c"
	to = "/a/b/f"
	err = dm.MoveUnderDirectory(from, filepath.Dir(to), filepath.Base(to))
	if err != nil {
		t.Errorf("move %s to %s error:%v", from, to, err)
	}
	// /a/b/d/c should not exist
	dir = from
	did, err = dm.FindDirectory(dir)
	if err != nil {
		t.Errorf("find dir %s error:%v", dir, err)
	}
	if did != 0 {
		t.Errorf("%s still exists after moved to %s", from, to)
	}
	// /a/b/f should exist
	dir = to
	did, err = dm.FindDirectory(dir)
	if did != 4 {
		t.Errorf("new dir %s has a wrong dir id:%d, 4 is expected", dir, did)
	}
	// /a/b/f/e also should exist
	dir = to + "/e"
	did, err = dm.FindDirectory(dir)
	if did != 6 {
		t.Errorf("new dir %s has a wrong dir id:%d, 6 is expected", dir, did)
	}

	/*
	 * list a dir's sub-directories
	 *
	 */
	dir = "/a/b"
	entries, err := dm.ListDirectories(dir)
	if !(entries[0].Name == "d" && entries[1].Name == "f") {
		t.Errorf("get entries:%v, expect 'd', 'f'", entries)
	}
	/*
		* delete a dir: /a/b/f/e
		 *
	*/
	dir = "/a/b/f"
	err = dm.DeleteDirectory(dir)
	if err != nil {
		t.Errorf("delete dir:%s error:%v", dir, err)
	}
	// now the deleted dir should not exist
	did, err = dm.FindDirectory(dir)
	if err != nil {
		t.Errorf("get deleted dir %s error:%v", dir, err)
	}
	if did != 0 {
		t.Errorf("the dir %s is not deleted!", dir)
	}
	// now the deleted dir's sub-directory also should not exist
	dir = "/a/b/f/e"
	did, err = dm.FindDirectory(dir)
	if err != nil {
		t.Errorf("get deleted dir %s error:%v", dir, err)
	}
	if did != 0 {
		t.Errorf("the dir %s is not deleted!", dir)
	}
}

func clearRedisKeys(client *redis.Client, dirKeyPrefix string, dirMaxIdKey string) error {
	result, err := client.Keys(dirKeyPrefix + "*").Result()
	if err != nil {
		fmt.Println("get redis keys error:", err)
		return err
	}
	if len(result) > 0 {
		n, err := client.Del(result...).Result()
		if err != nil {
			fmt.Println("del keys error:", err)
		} else {
			fmt.Println("del", n, " keys.")
		}
	}
	n, err := client.Del(dirMaxIdKey).Result()
	if err != nil {
		fmt.Printf("del dirMaxIdKey:%s, error:%v\n", dirMaxIdKey, err)
	} else {
		fmt.Println("del", n, " keys.")
	}
	return err
}
func TestMain(m *testing.M) {
	redisClient := redis.NewTCPClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,
	})
	err := clearRedisKeys(redisClient, "d:", "swfs:dir-max-id")
	if err != nil {
		os.Exit(-1)
	}
	dm = InitDirectoryManger(redisClient)
	ret := m.Run()
	redisClient.Close()
	os.Exit(ret)
}
