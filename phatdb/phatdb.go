package phatdb

// create
// close
// delete
// exists
// getACL
// getChildren
// getData
// getSessionId
// getSessionPassword
// register(watcher)
// setACL
// setData

// TODO: https://groups.google.com/forum/#!topic/golang-nuts/ct99dtK2Jo4
import (
	"os"
	"strings"
)

func SplitOnSlash(r rune) bool {
	return r == '/'
}

type StatNode struct {
	Version     uint64 // File version
	CVersion    uint64 // TODO: Children version
	NumChildren uint64 // TODO: Number of children
}

type DataNode struct {
	Value string
	Stats *StatNode
	// Ephemeral identifies creator
}

type FileNode struct {
	Parent   *FileNode
	Children map[string]*FileNode
	Data     string // Temporary -- replaced with DataNode
	Version  uint64 // Temporary -- replaced with DataNode.StatNode
}

func GetNodePath(path string) []string {
	parts := strings.FieldsFunc(path, SplitOnSlash)
	return parts
}

func traverseToNode(root *FileNode, parts []string, createMissing bool) (*FileNode, error) {
	temp := root
	// Walk along the path to find our node
	for _, part := range parts {
		if _, exists := temp.Children[part]; !exists {
			if !createMissing {
				return nil, os.ErrNotExist
			}
			// Create any missing nodes along the way
			temp.Children[part] = &FileNode{}
			temp.Children[part].Parent = temp
			temp = temp.Children[part]
			temp.Children = make(map[string]*FileNode)
		} else {
			temp = temp.Children[part]
		}
	}
	return temp, nil
}

func createNode(root *FileNode, path string, val string) (*FileNode, error) {
	n, _ := traverseToNode(root, GetNodePath(path), true)
	if n.Version != 0 {
		return nil, os.ErrExist
	}
	_setNode(n, val)
	return n, nil
}

func deleteNode(root *FileNode, path string) (*FileNode, error) {
	parts := GetNodePath(path)
	n, err := traverseToNode(root, parts, false)
	if err != nil {
		return nil, err
	}
	p := n.Parent
	delete(p.Children, parts[len(parts)-1])
	return p, nil
}

func existsNode(root *FileNode, path string) (bool, error) {
	n, err := traverseToNode(root, GetNodePath(path), false)
	// If the error is that the file does/doesn't exist, that's no issue
	// NOTE: os.IsExist may also be reasonable here in the future
	if os.IsNotExist(err) {
		return n != nil, nil
	}
	return n != nil, err
}

func getChildren(root *FileNode, path string) ([]string, error) {
	n, err := traverseToNode(root, GetNodePath(path), false)
	if err != nil {
		return nil, err
	}
	var keys []string
	for k := range n.Children {
		keys = append(keys, k)
	}
	return keys, nil
}

func getNode(root *FileNode, path string) (*FileNode, error) {
	return traverseToNode(root, GetNodePath(path), false)
}

func setNode(root *FileNode, path string, val string) (*FileNode, error) {
	n, err := traverseToNode(root, GetNodePath(path), false)
	if err != nil {
		return nil, err
	}
	_setNode(n, val)
	return n, nil
}

func _setNode(n *FileNode, val string) {
	// TODO: Attach this to the DataNode
	n.Data = val
	n.Version += 1
}
