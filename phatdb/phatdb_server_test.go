package phatdb

import (
	"os"
	"testing"
)

func TestDatabaseServer(t *testing.T) {
	input := make(chan DBCommand)
	go DatabaseServer(input)
	//
	// A bad command should fail
	badCmd := DBCommand{"HAMMERTIME", "", "", make(chan *DBResponse)}
	input <- badCmd
	// TODO: Ensure it's the expected error
	if resp := <-badCmd.Done; resp.Reply != nil || resp.Error == nil {
		t.Errorf("A bad command returned non-error response")
	}
	// Create should succeed
	createCmd := DBCommand{"CREATE", "/dev/null", "empty", make(chan *DBResponse)}
	input <- createCmd
	if resp := <-createCmd.Done; (resp.Reply.(*FileNode)).Data != "empty" || resp.Error != nil {
		t.Errorf("CREATE that should work has failed")
	}
	// Try to create a file that already exists
	input <- createCmd
	if resp := <-createCmd.Done; resp.Reply.(*FileNode) != nil || !os.IsExist(resp.Error) {
		t.Errorf("CREATE has succeeded even though file already exists")
	}
	//
	getCmd := DBCommand{"GET", "/dev/null", "", make(chan *DBResponse)}
	input <- getCmd
	if resp := <-getCmd.Done; resp.Reply.(*FileNode).Data != "empty" || resp.Reply.(*FileNode).Version != 1 || resp.Error != nil {
		t.Errorf("GET fails")
	}
	//
	setCmd := DBCommand{"SET", "/dev/null", "nullify", make(chan *DBResponse)}
	input <- setCmd
	if resp := <-setCmd.Done; resp.Reply.(*FileNode).Data != "nullify" || resp.Reply.(*FileNode).Version != 2 || resp.Error != nil {
		t.Errorf("SET fails")
	}
}
