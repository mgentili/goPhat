package queuedisk

import (
	"testing"
	queue "github.com/mgentili/goPhat/phatqueue"
)

func TestQServer(t *testing.T) {

	input := make(chan queue.QCommandWithChannel)
	go QueueServer(input)

	elems := []string{"/dev/nulled", "/dev/random", "/dev/urandom"}
    for i := 0; i < 100; i++ {
	for _, val := range elems {
		// Place an object on the queue
		pushCmd := queue.QCommandWithChannel{&queue.QCommand{"PUSH", val}, make(chan *queue.QResponse)}
		input <- pushCmd
		<-pushCmd.Done
}
    popCmd := queue.QCommandWithChannel{&queue.QCommand{"POP", ""}, make(chan *queue.QResponse)}
    input <- popCmd
    <-popCmd.Done

    }
    popCmd := queue.QCommandWithChannel{&queue.QCommand{"POP", ""}, make(chan *queue.QResponse)}
    input <- popCmd
    <-popCmd.Done


    popCmd = queue.QCommandWithChannel{&queue.QCommand{"POP", ""}, make(chan *queue.QResponse)}
    input <- popCmd
    <-popCmd.Done

}
