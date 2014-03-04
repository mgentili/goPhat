package phatdb

import (
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
	if existsNode(root, path) != false {
		t.Errorf("Exists thinks files exist when they shouldn't")
	}
}

func TestAddGetDelNode(t *testing.T) {
	root := setup()
	//
	path := "/dev/null"
	val := "empty"
	n1 := addNode(root, path, val)
	if n1.Data != val {
		t.Errorf("Set node failed")
	}
	if n, _ := getNode(root, path); n.Data != val {
		t.Errorf("Get and/or set node failed")
	}
	if existsNode(root, path) != true {
		t.Errorf("Exists reported the wrong result")
	}
	deleteNode(root, path)
	if _, err := getNode(root, path); err == nil {
		t.Errorf("Delete did not succeed")
	}
}
