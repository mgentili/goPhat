package phatdb

import (
	"testing"
)

func TestDatabaseHash(t *testing.T) {
	input := make(chan DBCommandWithChannel)
	go DatabaseServer(input)
	//
	hashCmd := DBCommandWithChannel{&DBCommand{"SHA256", "", ""}, make(chan *DBResponse)}
	input <- hashCmd
	expected := "55c8e1bda0453b939ff4b403e418931485841ec44c3256877d4509cd6b0c4199"
	if resp := <-hashCmd.Done; resp.Reply != expected || resp.Error != "" {
		t.Errorf("Hash returned %v instead of %v", resp.Reply, expected)
	}
	//
	createCmd := DBCommandWithChannel{&DBCommand{"CREATE", "/dev/null", "empty"}, make(chan *DBResponse)}
	input <- createCmd
	if resp := <-createCmd.Done; (resp.Reply.(*DataNode)).Value != "empty" || resp.Error != "" {
		t.Errorf("CREATE that should work has failed")
	}
	//
	input <- hashCmd
	expected = "e154e0661633e98ef88150d6ae5e2b80a2cdc198b43f684b63d0640178b2fed9"
	if resp := <-hashCmd.Done; resp.Reply != expected || resp.Error != "" {
		t.Errorf("Hash returned %v instead of %v", resp.Reply, expected)
	}
}

func TestDatabaseServer(t *testing.T) {
	input := make(chan DBCommandWithChannel)
	go DatabaseServer(input)
	//
	// A bad command should fail
	badCmd := DBCommandWithChannel{&DBCommand{"HAMMERTIME", "", ""}, make(chan *DBResponse)}
	input <- badCmd
	// TODO: Ensure it's the expected error
	if resp := <-badCmd.Done; resp.Reply != nil || resp.Error == "" {
		t.Errorf("A bad command returned non-error response")
	}
	// Create should succeed
	createCmd := DBCommandWithChannel{&DBCommand{"CREATE", "/dev/null", "empty"}, make(chan *DBResponse)}
	input <- createCmd
	if resp := <-createCmd.Done; (resp.Reply.(*DataNode)).Value != "empty" || resp.Error != "" {
		t.Errorf("CREATE that should work has failed")
	}
	// Try to create a file that already exists
	input <- createCmd
	if resp := <-createCmd.Done; resp.Error == "" {
		t.Errorf("CREATE has succeeded even though file already exists")
	}
	//
	getCmd := DBCommandWithChannel{&DBCommand{"GET", "/dev/null", ""}, make(chan *DBResponse)}
	input <- getCmd
	if resp := <-getCmd.Done; resp.Reply.(*DataNode).Value != "empty" || resp.Reply.(*DataNode).Stats.Version != 1 || resp.Error != "" {
		t.Errorf("GET fails")
	}
	//
	setCmd := DBCommandWithChannel{&DBCommand{"SET", "/dev/null", "nullify"}, make(chan *DBResponse)}
	input <- setCmd
	if resp := <-setCmd.Done; resp.Error != "" {
		t.Errorf("SET fails")
	}
	//
	for _, path := range []string{"/dev/nulled", "/dev/random", "/dev/urandom"} {
		setCmd = DBCommandWithChannel{&DBCommand{"CREATE", path, "nullify"}, make(chan *DBResponse)}
		input <- setCmd
		if resp := <-setCmd.Done; resp.Error != "" {
			t.Errorf("SET fails with %s", resp.Error)
		}
	}
	// Check get children
	for _, path := range []string{"/dev", "/dev/"} {
		childrenCmd := DBCommandWithChannel{&DBCommand{"CHILDREN", path, ""}, make(chan *DBResponse)}
		input <- childrenCmd
		expected := []string{"null", "nulled", "random", "urandom"}
		if resp := <-childrenCmd.Done; !areEqual(expected, resp.Reply.([]string)) || resp.Error != "" {
			t.Errorf("CHILDREN fails: expected %s, received %s (err: %s)", expected, resp.Reply.([]string), resp.Error)
		}
	}
}
