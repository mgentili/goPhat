package phatdb

import (
	"fmt"
	"testing"
)

// http://stackoverflow.com/questions/15311969/checking-the-equality-of-two-slices ...
func areEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type nodePathPair struct {
	path     string
	expected []string
}

func TestGetNodePath(t *testing.T) {
	var tests = []nodePathPair{
		// Test the root directory
		{"/", []string{}},
		// Test a directory with trailing slash
		{"/inception/must/go/deeper/", []string{"inception", "must", "go", "deeper"}},
		// Test a file
		{"/dev/null", []string{"dev", "null"}},
	}
	//
	for _, pair := range tests {
		if x := GetNodePath(pair.path); !areEqual(pair.expected, x) {
			t.Errorf("GetNodePath(%v) = %v, want %v", pair.path, x, pair.expected)
		}
	}
}

func setup() *FileNode {
	root := &FileNode{}
	root.Children = make(map[string]*FileNode)
	return root
}

func TestExistsNode(t *testing.T) {
	root := setup()
	//
	path := "/dev/null"
	if exists, err := existsNode(root, path); err != nil || exists != false {
		t.Errorf("Exists thinks files exist when they shouldn't")
	}
}

func TestAddGetDelNode(t *testing.T) {
	root := setup()
	//
	path := "/dev/null"
	val1 := "empty"
	val2 := "nothingness"
	// Create the node
	n, err := createNode(root, path, val1)
	if err != nil || n.Value != val1 || n.Stats.Version != 1 {
		t.Errorf("Set node failed")
	}
	// Update the contents of the node
	setNode(root, path, val2)
	if n, err := getNode(root, path); err != nil || n.Value != val2 || n.Stats.Version != 2 {
		t.Errorf("Get and/or set node failed")
	}
	// Ensure the node exists
	if exists, err := existsNode(root, path); err != nil || exists != true {
		t.Errorf("Exists reported the wrong result")
	}
	// Delete the node
	deleteNode(root, path)
	if _, err := getNode(root, path); err == nil {
		t.Errorf("Delete did not succeed")
	}
	// Create the node again -- currently we expect the version to be 1 again
	// TODO: Should this have different behaviour? Is this what you'd expect?
	if n, err = createNode(root, path, val1); n.Value != val1 || n.Stats.Version != 1 {
		t.Errorf("Set node failed")
	}
}

func TestGetChildren(t *testing.T) {
	root := setup()
	//
	path := "/dev/null"
	// Create the children of /dev/null
	children := []string{"a", "b", "c", "d", "e"}
	for _, child := range children {
		createNode(root, fmt.Sprintf("%s/%s", path, child), child)
	}
	// Ensure all the expected children are there
	if names, _ := getChildren(root, path); !areEqual(names, children) {
		t.Errorf("getChildren: wanted %v, received %v", children, names)
	}
	// Delete a child and then retest
	deleteNode(root, "/dev/null/a")
	children = children[1:]
	if names, _ := getChildren(root, path); !areEqual(names, children) {
		t.Errorf("getChildren: wanted %v, received %v", children, names)
	}
}
