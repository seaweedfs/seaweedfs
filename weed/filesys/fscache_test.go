package filesys

import (
	"testing"

	"github.com/chrislusf/seaweedfs/weed/util"
)

func TestPathSplit(t *testing.T) {
	parts := util.FullPath("/").Split()
	if len(parts) != 0 {
		t.Errorf("expecting an empty list, but getting %d", len(parts))
	}

	parts = util.FullPath("/readme.md").Split()
	if len(parts) != 1 {
		t.Errorf("expecting an empty list, but getting %d", len(parts))
	}

}

func TestFsCache(t *testing.T) {

	cache := newFsCache(nil)

	x := cache.GetFsNode(util.FullPath("/y/x"))
	if x != nil {
		t.Errorf("wrong node!")
	}

	p := util.FullPath("/a/b/c")
	cache.SetFsNode(p, &File{Name: "cc"})
	tNode := cache.GetFsNode(p)
	tFile := tNode.(*File)
	if tFile.Name != "cc" {
		t.Errorf("expecting a FsNode")
	}

	cache.SetFsNode(util.FullPath("/a/b/d"), &File{Name: "dd"})
	cache.SetFsNode(util.FullPath("/a/b/e"), &File{Name: "ee"})
	cache.SetFsNode(util.FullPath("/a/b/f"), &File{Name: "ff"})
	cache.SetFsNode(util.FullPath("/z"), &File{Name: "zz"})
	cache.SetFsNode(util.FullPath("/a"), &File{Name: "aa"})

	b := cache.GetFsNode(util.FullPath("/a/b"))
	if b != nil {
		t.Errorf("unexpected node!")
	}

	a := cache.GetFsNode(util.FullPath("/a"))
	if a == nil {
		t.Errorf("missing node!")
	}

	cache.DeleteFsNode(util.FullPath("/a"))
	if b != nil {
		t.Errorf("unexpected node!")
	}

	a = cache.GetFsNode(util.FullPath("/a"))
	if a != nil {
		t.Errorf("wrong DeleteFsNode!")
	}

	z := cache.GetFsNode(util.FullPath("/z"))
	if z == nil {
		t.Errorf("missing node!")
	}

	y := cache.GetFsNode(util.FullPath("/x/y"))
	if y != nil {
		t.Errorf("wrong node!")
	}

}

func TestFsCacheMove(t *testing.T) {

	cache := newFsCache(nil)

	cache.SetFsNode(util.FullPath("/a/b/d"), &File{Name: "dd"})
	cache.SetFsNode(util.FullPath("/a/b/e"), &File{Name: "ee"})
	cache.SetFsNode(util.FullPath("/z"), &File{Name: "zz"})
	cache.SetFsNode(util.FullPath("/a"), &File{Name: "aa"})

	cache.Move(util.FullPath("/a/b"), util.FullPath("/z/x"))

	d := cache.GetFsNode(util.FullPath("/z/x/d"))
	if d == nil {
		t.Errorf("unexpected nil node!")
	}
	if d.(*File).Name != "dd" {
		t.Errorf("unexpected non dd node!")
	}

}
