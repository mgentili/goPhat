package phatdb

import (
	"os"
	"testing"
)

func TestDatabaseServer(t *testing.T) {
	input := make(chan DBCommand)
	go DatabaseServer(input)
	//
	// Create should succeed
	setCmd := DBCommand{"CREATE", "/dev/null", "empty", make(chan *DBResponse)}
	input <- setCmd
	resp := <-setCmd.Done
	if (resp.Reply.(*FileNode)).Data != "empty" || resp.Error != nil {
		t.Errorf("CREATE that should work has failed")
	}
	// Try to create a file that already exists
	input <- setCmd
	resp = <-setCmd.Done
	if resp.Reply != nil || resp.Error != os.ErrExist {
		t.Errorf("CREATE has succeeded even though file already exists")
	}
	//
}
