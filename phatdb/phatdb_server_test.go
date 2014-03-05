package phatdb

import (
	"os"
	"testing"
)

func TestDatabaseServer(t *testing.T) {
	input := make(chan DBCommandWithChannel)
	go DatabaseServer(input)
	//
	// A bad command should fail
	badCmd := DBCommandWithChannel{&DBCommand{"HAMMERTIME", "", ""}, make(chan *DBResponse)}
	input <- badCmd
	// TODO: Ensure it's the expected error
	if resp := <-badCmd.Done; resp.Reply != nil || resp.Error == nil {
		t.Errorf("A bad command returned non-error response")
	}
	// Create should succeed
	createCmd := DBCommandWithChannel{&DBCommand{"CREATE", "/dev/null", "empty"}, make(chan *DBResponse)}
	input <- createCmd
	if resp := <-createCmd.Done; (resp.Reply.(*DataNode)).Value != "empty" || resp.Error != nil {
		t.Errorf("CREATE that should work has failed")
	}
	// Try to create a file that already exists
	input <- createCmd
	if resp := <-createCmd.Done; resp.Reply.(*DataNode) != nil || !os.IsExist(resp.Error) {
		t.Errorf("CREATE has succeeded even though file already exists")
	}
	//
	getCmd := DBCommandWithChannel{&DBCommand{"GET", "/dev/null", ""}, make(chan *DBResponse)}
	input <- getCmd
	if resp := <-getCmd.Done; resp.Reply.(*DataNode).Value != "empty" || resp.Reply.(*DataNode).Stats.Version != 1 || resp.Error != nil {
		t.Errorf("GET fails")
	}
	//
	setCmd := DBCommandWithChannel{&DBCommand{"SET", "/dev/null", "nullify"}, make(chan *DBResponse)}
	input <- setCmd
	if resp := <-setCmd.Done; resp.Reply.(*DataNode).Value != "nullify" || resp.Reply.(*DataNode).Stats.Version != 2 || resp.Error != nil {
		t.Errorf("SET fails")
	}
	//
}
