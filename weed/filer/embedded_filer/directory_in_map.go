package embedded_filer

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/util"
)

var writeLock sync.Mutex //serialize changes to dir.log

type DirectoryEntryInMap struct {
	sync.Mutex
	Name           string
	Parent         *DirectoryEntryInMap
	subDirectories map[string]*DirectoryEntryInMap
	Id             filer.DirectoryId
}

func (de *DirectoryEntryInMap) getChild(dirName string) (*DirectoryEntryInMap, bool) {
	de.Lock()
	defer de.Unlock()
	child, ok := de.subDirectories[dirName]
	return child, ok
}
func (de *DirectoryEntryInMap) addChild(dirName string, child *DirectoryEntryInMap) {
	de.Lock()
	defer de.Unlock()
	de.subDirectories[dirName] = child
}
func (de *DirectoryEntryInMap) removeChild(dirName string) {
	de.Lock()
	defer de.Unlock()
	delete(de.subDirectories, dirName)
}
func (de *DirectoryEntryInMap) hasChildren() bool {
	de.Lock()
	defer de.Unlock()
	return len(de.subDirectories) > 0
}
func (de *DirectoryEntryInMap) children() (dirNames []filer.DirectoryEntry) {
	de.Lock()
	defer de.Unlock()
	for k, v := range de.subDirectories {
		dirNames = append(dirNames, filer.DirectoryEntry{Name: k, Id: v.Id})
	}
	return dirNames
}

type DirectoryManagerInMap struct {
	Root      *DirectoryEntryInMap
	max       filer.DirectoryId
	logFile   *os.File
	isLoading bool
}

func (dm *DirectoryManagerInMap) newDirectoryEntryInMap(parent *DirectoryEntryInMap, name string) (d *DirectoryEntryInMap, err error) {
	d = &DirectoryEntryInMap{Name: name, Parent: parent, subDirectories: make(map[string]*DirectoryEntryInMap)}
	var parts []string
	for p := d; p != nil && p.Name != ""; p = p.Parent {
		parts = append(parts, p.Name)
	}
	n := len(parts)
	if n <= 0 {
		return nil, fmt.Errorf("Failed to create folder %s/%s", parent.Name, name)
	}
	for i := 0; i < n/2; i++ {
		parts[i], parts[n-1-i] = parts[n-1-i], parts[i]
	}
	dm.max++
	d.Id = dm.max
	dm.log("add", "/"+strings.Join(parts, "/"), strconv.Itoa(int(d.Id)))
	return d, nil
}

func (dm *DirectoryManagerInMap) log(words ...string) {
	if !dm.isLoading {
		dm.logFile.WriteString(strings.Join(words, "\t") + "\n")
	}
}

func NewDirectoryManagerInMap(dirLogFile string) (dm *DirectoryManagerInMap, err error) {
	dm = &DirectoryManagerInMap{}
	//dm.Root do not use newDirectoryEntryInMap, since dm.max will be changed
	dm.Root = &DirectoryEntryInMap{subDirectories: make(map[string]*DirectoryEntryInMap)}
	if dm.logFile, err = os.OpenFile(dirLogFile, os.O_RDWR|os.O_CREATE, 0644); err != nil {
		return nil, fmt.Errorf("cannot write directory log file %s: %v", dirLogFile, err)
	}
	return dm, dm.load()
}

func (dm *DirectoryManagerInMap) processEachLine(line string) error {
	if strings.HasPrefix(line, "#") {
		return nil
	}
	if line == "" {
		return nil
	}
	parts := strings.Split(line, "\t")
	if len(parts) == 0 {
		return nil
	}
	switch parts[0] {
	case "add":
		v, pe := strconv.Atoi(parts[2])
		if pe != nil {
			return pe
		}
		if e := dm.loadDirectory(parts[1], filer.DirectoryId(v)); e != nil {
			return e
		}
	case "mov":
		newName := ""
		if len(parts) >= 4 {
			newName = parts[3]
		}
		if e := dm.MoveUnderDirectory(parts[1], parts[2], newName); e != nil {
			return e
		}
	case "del":
		if e := dm.DeleteDirectory(parts[1]); e != nil {
			return e
		}
	default:
		fmt.Printf("line %s has %s!\n", line, parts[0])
		return nil
	}
	return nil
}
func (dm *DirectoryManagerInMap) load() error {
	dm.max = 0
	lines := bufio.NewReader(dm.logFile)
	dm.isLoading = true
	defer func() { dm.isLoading = false }()
	for {
		line, err := util.Readln(lines)
		if err != nil && err != io.EOF {
			return err
		}
		if pe := dm.processEachLine(string(line)); pe != nil {
			return pe
		}
		if err == io.EOF {
			return nil
		}
	}
}

func (dm *DirectoryManagerInMap) findDirectory(dirPath string) (*DirectoryEntryInMap, error) {
	if dirPath == "" {
		return dm.Root, nil
	}
	dirPath = CleanFilePath(dirPath)
	if dirPath == "/" {
		return dm.Root, nil
	}
	parts := strings.Split(dirPath, "/")
	dir := dm.Root
	for i := 1; i < len(parts); i++ {
		if sub, ok := dir.getChild(parts[i]); ok {
			dir = sub
		} else {
			return dm.Root, filer.ErrNotFound
		}
	}
	return dir, nil
}
func (dm *DirectoryManagerInMap) FindDirectory(dirPath string) (filer.DirectoryId, error) {
	d, e := dm.findDirectory(dirPath)
	if e == nil {
		return d.Id, nil
	}
	return dm.Root.Id, e
}

func (dm *DirectoryManagerInMap) loadDirectory(dirPath string, dirId filer.DirectoryId) error {
	dirPath = CleanFilePath(dirPath)
	if dirPath == "/" {
		return nil
	}
	parts := strings.Split(dirPath, "/")
	dir := dm.Root
	for i := 1; i < len(parts); i++ {
		sub, ok := dir.getChild(parts[i])
		if !ok {
			writeLock.Lock()
			if sub2, createdByOtherThread := dir.getChild(parts[i]); createdByOtherThread {
				sub = sub2
			} else {
				if i != len(parts)-1 {
					writeLock.Unlock()
					return fmt.Errorf("%s should be created after parent %s", dirPath, parts[i])
				}
				var err error
				sub, err = dm.newDirectoryEntryInMap(dir, parts[i])
				if err != nil {
					writeLock.Unlock()
					return err
				}
				if sub.Id != dirId {
					writeLock.Unlock()
					// the dir.log should be the same order as in-memory directory id
					return fmt.Errorf("%s should be have id %v instead of %v", dirPath, sub.Id, dirId)
				}
				dir.addChild(parts[i], sub)
			}
			writeLock.Unlock()
		}
		dir = sub
	}
	return nil
}

func (dm *DirectoryManagerInMap) makeDirectory(dirPath string) (dir *DirectoryEntryInMap, created bool) {
	dirPath = CleanFilePath(dirPath)
	if dirPath == "/" {
		return dm.Root, false
	}
	parts := strings.Split(dirPath, "/")
	dir = dm.Root
	for i := 1; i < len(parts); i++ {
		sub, ok := dir.getChild(parts[i])
		if !ok {
			writeLock.Lock()
			if sub2, createdByOtherThread := dir.getChild(parts[i]); createdByOtherThread {
				sub = sub2
			} else {
				var err error
				sub, err = dm.newDirectoryEntryInMap(dir, parts[i])
				if err != nil {
					writeLock.Unlock()
					return nil, false
				}
				dir.addChild(parts[i], sub)
				created = true
			}
			writeLock.Unlock()
		}
		dir = sub
	}
	return dir, created
}

func (dm *DirectoryManagerInMap) MakeDirectory(dirPath string) (filer.DirectoryId, error) {
	dir, _ := dm.makeDirectory(dirPath)
	return dir.Id, nil
}

func (dm *DirectoryManagerInMap) MoveUnderDirectory(oldDirPath string, newParentDirPath string, newName string) error {
	writeLock.Lock()
	defer writeLock.Unlock()
	oldDir, oe := dm.findDirectory(oldDirPath)
	if oe != nil {
		return oe
	}
	parentDir, pe := dm.findDirectory(newParentDirPath)
	if pe != nil {
		return pe
	}
	dm.log("mov", oldDirPath, newParentDirPath, newName)
	oldDir.Parent.removeChild(oldDir.Name)
	if newName == "" {
		newName = oldDir.Name
	}
	parentDir.addChild(newName, oldDir)
	oldDir.Name = newName
	oldDir.Parent = parentDir
	return nil
}

func (dm *DirectoryManagerInMap) ListDirectories(dirPath string) (dirNames []filer.DirectoryEntry, err error) {
	d, e := dm.findDirectory(dirPath)
	if e != nil {
		return dirNames, e
	}
	return d.children(), nil
}
func (dm *DirectoryManagerInMap) DeleteDirectory(dirPath string) error {
	writeLock.Lock()
	defer writeLock.Unlock()
	if dirPath == "/" {
		return fmt.Errorf("Can not delete %s", dirPath)
	}
	d, e := dm.findDirectory(dirPath)
	if e != nil {
		return e
	}
	if d.hasChildren() {
		return fmt.Errorf("dir %s still has sub directories", dirPath)
	}
	d.Parent.removeChild(d.Name)
	d.Parent = nil
	dm.log("del", dirPath)
	return nil
}

func CleanFilePath(fp string) string {
	ret := filepath.Clean(fp)
	if os.PathSeparator == '\\' {
		return strings.Replace(ret, "\\", "/", -1)
	}
	return ret
}
