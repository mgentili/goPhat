package queuedisk

import (
	"testing"
)

func TestQServer(t *testing.T) {

	input := make(chan QCommandWithChannel)
	go QueueServer(input)

	elems := []string{"/dev/nulled", "/dev/random", "/dev/urandom"}
	for _, val := range elems {
		// Place an object on the queue
		pushCmd := QCommandWithChannel{&QCommand{"PUSH", val}, make(chan *QResponse)}
		input <- pushCmd
		<-pushCmd.Done
	}
    popCmd := QCommandWithChannel{&QCommand{"POP", ""}, make(chan *QResponse)}
    input <- popCmd
    <-popCmd.Done
}
