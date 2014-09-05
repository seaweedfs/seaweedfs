package filer

import (
	"bufio"
	"code.google.com/p/weed-fs/go/util"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

var writeLock sync.Mutex //serialize changes to dir.log

type DirectoryEntryInMap struct {
	Name           string
	Parent         *DirectoryEntryInMap
	SubDirectories map[string]*DirectoryEntryInMap
	Id             DirectoryId
}

type DirectoryManagerInMap struct {
	Root      *DirectoryEntryInMap
	max       DirectoryId
	logFile   *os.File
	isLoading bool
}

func (dm *DirectoryManagerInMap) NewDirectoryEntryInMap(parent *DirectoryEntryInMap, name string) (d *DirectoryEntryInMap, err error) {
	writeLock.Lock()
	defer writeLock.Unlock()
	d = &DirectoryEntryInMap{Name: name, Parent: parent, SubDirectories: make(map[string]*DirectoryEntryInMap)}
	parts := make([]string, 0)
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
	//dm.Root do not use NewDirectoryEntryInMap, since dm.max will be changed
	dm.Root = &DirectoryEntryInMap{SubDirectories: make(map[string]*DirectoryEntryInMap)}
	if dm.logFile, err = os.OpenFile(dirLogFile, os.O_RDWR|os.O_CREATE, 0644); err != nil {
		return nil, fmt.Errorf("cannot write directory log file %s.idx: %s", dirLogFile, err.Error())
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
		if e := dm.loadDirectory(parts[1], DirectoryId(v)); e != nil {
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
	dirPath = filepath.Clean(dirPath)
	if dirPath == "/" {
		return dm.Root, nil
	}
	parts := strings.Split(dirPath, "/")
	dir := dm.Root
	for i := 1; i < len(parts); i++ {
		if sub, ok := dir.SubDirectories[parts[i]]; ok {
			dir = sub
		} else {
			return dm.Root, fmt.Errorf("Directory %s Not Found", dirPath)
		}
	}
	return dir, nil
}
func (dm *DirectoryManagerInMap) FindDirectory(dirPath string) (DirectoryId, error) {
	d, e := dm.findDirectory(dirPath)
	if e == nil {
		return d.Id, nil
	}
	return dm.Root.Id, e
}

func (dm *DirectoryManagerInMap) loadDirectory(dirPath string, dirId DirectoryId) error {
	dirPath = filepath.Clean(dirPath)
	if dirPath == "/" {
		return nil
	}
	parts := strings.Split(dirPath, "/")
	dir := dm.Root
	for i := 1; i < len(parts); i++ {
		sub, ok := dir.SubDirectories[parts[i]]
		if !ok {
			if i != len(parts)-1 {
				return fmt.Errorf("%s should be created after parent %s!", dirPath, parts[i])
			}
			var err error
			sub, err = dm.NewDirectoryEntryInMap(dir, parts[i])
			if err != nil {
				return err
			}
			if sub.Id != dirId {
				return fmt.Errorf("%s should be have id %v instead of %v!", dirPath, sub.Id, dirId)
			}
			dir.SubDirectories[parts[i]] = sub
		}
		dir = sub
	}
	return nil
}

func (dm *DirectoryManagerInMap) makeDirectory(dirPath string) (dir *DirectoryEntryInMap, created bool) {
	dirPath = filepath.Clean(dirPath)
	if dirPath == "/" {
		return dm.Root, false
	}
	parts := strings.Split(dirPath, "/")
	dir = dm.Root
	for i := 1; i < len(parts); i++ {
		sub, ok := dir.SubDirectories[parts[i]]
		if !ok {
		  var err error
			sub, err = dm.NewDirectoryEntryInMap(dir, parts[i])
			if err != nil {
			  return nil, false
			}
			dir.SubDirectories[parts[i]] = sub
			created = true
		}
		dir = sub
	}
	return dir, created
}

func (dm *DirectoryManagerInMap) MakeDirectory(dirPath string) (DirectoryId, error) {
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
	delete(oldDir.Parent.SubDirectories, oldDir.Name)
	if newName == "" {
		newName = oldDir.Name
	}
	parentDir.SubDirectories[newName] = oldDir
	oldDir.Name = newName
	oldDir.Parent = parentDir
	return nil
}

func (dm *DirectoryManagerInMap) ListDirectories(dirPath string) (dirNames []DirectoryEntry, err error) {
	d, e := dm.findDirectory(dirPath)
	if e != nil {
		return dirNames, e
	}
	for k, v := range d.SubDirectories {
		dirNames = append(dirNames, DirectoryEntry{Name: k, Id: v.Id})
	}
	return dirNames, nil
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
	if len(d.SubDirectories) != 0 {
		return fmt.Errorf("dir %s still has sub directories", dirPath)
	}
	delete(d.Parent.SubDirectories, d.Name)
	d.Parent = nil
	dm.log("del", dirPath)
	return nil
}
