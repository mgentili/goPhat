package phatqueue

import (
	"testing"
)

func TestQServer(t *testing.T) {
	input := make(chan QCommandWithChannel)
	go QueueServer(input)
	//
	popCmd := QCommandWithChannel{&QCommand{"POP", ""}, make(chan *QResponse)}
	lenCmd := QCommandWithChannel{&QCommand{"LEN", ""}, make(chan *QResponse)}
	// A bad command should fail
	badCmd := QCommandWithChannel{&QCommand{"HAMMERTIME", ""}, make(chan *QResponse)}
	input <- badCmd
	// TODO: Ensure it's the expected error
	if resp := <-badCmd.Done; resp.Reply != nil || resp.Error == "" {
		t.Errorf("A bad command returned non-error response")
	}
	// Check behaviour on an empty pop
	input <- popCmd
	if resp := <-popCmd.Done; resp.Reply != nil && resp.Error != "" {
		t.Errorf("Empty pop didn't behave as expected")
	}
	//
	elems := []string{"/dev/nulled", "/dev/random", "/dev/urandom"}
	for _, val := range elems {
		// Place an object on the queue
		pushCmd := QCommandWithChannel{&QCommand{"PUSH", val}, make(chan *QResponse)}
		input <- pushCmd
		<-pushCmd.Done
	}
	//
	for i, val := range elems {
		input <- popCmd
		resp := <-popCmd.Done
		if resp.Error != "" && resp.Reply != val {
			t.Errorf("SET fails with %s", resp.Error)
		}
		input <- lenCmd
		resp = <-lenCmd.Done
		if resp.Error != "" && resp.Reply != len(elems)-i {
			t.Errorf("Length didn't match expectation")
		}
		//doneCmd = DBCommandWithChannel{&DBCommand{"DONE", }, make(chan *DBResponse)}
	}
}
