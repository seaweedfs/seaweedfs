package filesys

import (
	"sync"

	"github.com/seaweedfs/fuse/fs"

	"github.com/chrislusf/seaweedfs/weed/util"
)

type FsCache struct {
	root *FsNode
	sync.RWMutex
}
type FsNode struct {
	parent       *FsNode
	node         fs.Node
	name         string
	childrenLock sync.RWMutex
	children     map[string]*FsNode
}

func newFsCache(root fs.Node) *FsCache {
	return &FsCache{
		root: &FsNode{
			node: root,
		},
	}
}

func (c *FsCache) GetFsNode(path util.FullPath) fs.Node {

	c.RLock()
	defer c.RUnlock()

	return c.doGetFsNode(path)
}

func (c *FsCache) doGetFsNode(path util.FullPath) fs.Node {
	t := c.root
	for _, p := range path.Split() {
		t = t.findChild(p)
		if t == nil {
			return nil
		}
	}
	return t.node
}

func (c *FsCache) SetFsNode(path util.FullPath, node fs.Node) {

	c.Lock()
	defer c.Unlock()

	c.doSetFsNode(path, node)
}

func (c *FsCache) doSetFsNode(path util.FullPath, node fs.Node) {
	t := c.root
	for _, p := range path.Split() {
		t = t.ensureChild(p)
	}
	t.node = node
}

func (c *FsCache) EnsureFsNode(path util.FullPath, genNodeFn func() fs.Node) fs.Node {

	c.Lock()
	defer c.Unlock()

	t := c.doGetFsNode(path)
	if t != nil {
		return t
	}
	t = genNodeFn()
	c.doSetFsNode(path, t)
	return t
}

func (c *FsCache) DeleteFsNode(path util.FullPath) {

	c.Lock()
	defer c.Unlock()

	t := c.root
	for _, p := range path.Split() {
		t = t.findChild(p)
		if t == nil {
			return
		}
	}
	if t.parent != nil {
		t.parent.disconnectChild(t)
	}
	t.deleteSelf()
}

// oldPath and newPath are full path including the new name
func (c *FsCache) Move(oldPath util.FullPath, newPath util.FullPath) *FsNode {

	c.Lock()
	defer c.Unlock()

	// find old node
	src := c.root
	for _, p := range oldPath.Split() {
		src = src.findChild(p)
		if src == nil {
			return src
		}
	}
	if src.parent != nil {
		src.parent.disconnectChild(src)
	}

	// find new node
	target := c.root
	for _, p := range newPath.Split() {
		target = target.ensureChild(p)
	}
	parent := target.parent
	if dir, ok := src.node.(*Dir); ok {
		dir.name = target.name // target is not Dir, but a shortcut
	}
	if f, ok := src.node.(*File); ok {
		f.Name = target.name
		entry := f.getEntry()
		if entry != nil {
			entry.Name = f.Name
		}
	}
	parent.disconnectChild(target)

	target.deleteSelf()

	src.name = target.name
	src.connectToParent(parent)

	return src
}

func (n *FsNode) connectToParent(parent *FsNode) {
	n.parent = parent
	oldNode := parent.findChild(n.name)
	if oldNode != nil {
		oldNode.deleteSelf()
	}
	if dir, ok := n.node.(*Dir); ok {
		if parent.node != nil {
			dir.parent = parent.node.(*Dir)
		}
	}
	if f, ok := n.node.(*File); ok {
		if parent.node != nil {
			f.dir = parent.node.(*Dir)
		}
	}
	n.childrenLock.Lock()
	parent.children[n.name] = n
	n.childrenLock.Unlock()
}

func (n *FsNode) findChild(name string) *FsNode {
	n.childrenLock.RLock()
	defer n.childrenLock.RUnlock()

	child, found := n.children[name]
	if found {
		return child
	}
	return nil
}

func (n *FsNode) ensureChild(name string) *FsNode {
	n.childrenLock.Lock()
	defer n.childrenLock.Unlock()

	if n.children == nil {
		n.children = make(map[string]*FsNode)
	}
	child, found := n.children[name]
	if found {
		return child
	}
	t := &FsNode{
		parent:   n,
		node:     nil,
		name:     name,
		children: nil,
	}
	n.children[name] = t
	return t
}

func (n *FsNode) disconnectChild(child *FsNode) {
	n.childrenLock.Lock()
	delete(n.children, child.name)
	n.childrenLock.Unlock()
	child.parent = nil
}

func (n *FsNode) deleteSelf() {
	n.childrenLock.Lock()
	for _, child := range n.children {
		child.deleteSelf()
	}
	n.children = nil
	n.childrenLock.Unlock()

	n.node = nil
	n.parent = nil

}
