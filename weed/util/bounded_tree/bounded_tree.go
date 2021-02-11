package bounded_tree

import (
	"sync"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type Node struct {
	Parent   *Node
	Name     string
	Children map[string]*Node
}

type BoundedTree struct {
	root *Node
	sync.RWMutex
	baseDir util.FullPath
}

func NewBoundedTree(baseDir util.FullPath) *BoundedTree {
	return &BoundedTree{
		root: &Node{
			Name: "/",
		},
		baseDir: baseDir,
	}
}

type VisitNodeFunc func(path util.FullPath) (childDirectories []string, err error)

// If the path is not visited, call the visitFn for each level of directory
// No action if the directory has been visited before or does not exist.
// A leaf node, which has no children, represents a directory not visited.
// A non-leaf node or a non-existing node represents a directory already visited, or does not need to visit.
func (t *BoundedTree) EnsureVisited(p util.FullPath, visitFn VisitNodeFunc) (visitErr error) {
	t.Lock()
	defer t.Unlock()

	if t.root == nil {
		return
	}
	if t.baseDir != "/" {
		p = p[len(t.baseDir):]
	}
	components := p.Split()
	// fmt.Printf("components %v %d\n", components, len(components))
	canDelete, err := t.ensureVisited(t.root, t.baseDir, components, 0, visitFn)
	if err != nil {
		return err
	}
	if canDelete {
		t.root = nil
	}
	return nil
}

func (t *BoundedTree) ensureVisited(n *Node, currentPath util.FullPath, components []string, i int, visitFn VisitNodeFunc) (canDeleteNode bool, visitErr error) {

	// println("ensureVisited", currentPath, i)

	if n == nil {
		// fmt.Printf("%s null\n", currentPath)
		return
	}

	if n.isVisited() {
		// fmt.Printf("%s visited %v\n", currentPath, n.Name)
	} else {
		// fmt.Printf("ensure %v\n", currentPath)

		children, err := visitFn(currentPath)
		if err != nil {
			glog.V(0).Infof("failed to visit %s: %v", currentPath, err)
			return false, err
		}

		if len(children) == 0 {
			// fmt.Printf("  canDelete %v without children\n", currentPath)
			return true, nil
		}

		n.Children = make(map[string]*Node)
		for _, child := range children {
			// fmt.Printf("  add child %v %v\n", currentPath, child)
			n.Children[child] = &Node{
				Name: child,
			}
		}
	}

	if i >= len(components) {
		return
	}

	// fmt.Printf("  check child %v %v\n", currentPath, components[i])

	toVisitNode, found := n.Children[components[i]]
	if !found {
		// fmt.Printf("  did not find child %v %v\n", currentPath, components[i])
		return
	}

	// fmt.Printf("  ensureVisited %v %v\n", currentPath, toVisitNode.Name)
	canDelete, childVisitErr := t.ensureVisited(toVisitNode, currentPath.Child(components[i]), components, i+1, visitFn)
	if childVisitErr != nil {
		return false, childVisitErr
	}
	if canDelete {

		// fmt.Printf("  delete %v %v\n", currentPath, components[i])
		delete(n.Children, components[i])

		if len(n.Children) == 0 {
			// fmt.Printf("  canDelete %v\n", currentPath)
			return true, nil
		}
	}

	return false, nil

}

func (n *Node) isVisited() bool {
	if n == nil {
		return true
	}
	if len(n.Children) > 0 {
		return true
	}
	return false
}

func (n *Node) getChild(childName string) *Node {
	if n == nil {
		return nil
	}
	if len(n.Children) > 0 {
		return n.Children[childName]
	}
	return nil
}

func (t *BoundedTree) HasVisited(p util.FullPath) bool {

	t.RLock()
	defer t.RUnlock()

	if t.root == nil {
		return true
	}

	components := p.Split()
	// fmt.Printf("components %v %d\n", components, len(components))
	return t.hasVisited(t.root, util.FullPath("/"), components, 0)
}

func (t *BoundedTree) hasVisited(n *Node, currentPath util.FullPath, components []string, i int) bool {

	if n == nil {
		return true
	}

	if !n.isVisited() {
		return false
	}

	// fmt.Printf("  hasVisited child %v %+v %d\n", currentPath, components, i)

	if i >= len(components) {
		return true
	}

	toVisitNode, found := n.Children[components[i]]
	if !found {
		return true
	}

	return t.hasVisited(toVisitNode, currentPath.Child(components[i]), components, i+1)

}
