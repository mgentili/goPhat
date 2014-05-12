package queuedisk

import (
	queue "github.com/mgentili/goPhat/phatqueue"
)

var OpsPerCommit = 100

func QueueServer(input chan queue.QCommandWithChannel) {
	// Set up the queue
	mq := MessageQueue{}
	mq.Init(OpsPerCommit)

	// Enter the command loop
	for {
		request := <-input
		req := request.Cmd
		resp := &queue.QResponse{}
		switch req.Command {
		case "PUSH":
			mq.Push(req.Value)
		case "POP":
			v := mq.Pop()
			if v != nil {
				resp.Reply = v
			} else {
				resp.Error = "Nothing to pop"
			}
        case "SNAPSHOT":
            mq.Snapshot()
		case "DONE":
			mq.Done(req.Value.(string))
		case "LEN":
			resp.Reply = mq.Len()
		case "LEN_IN_PROGRESS":
			resp.Reply = mq.LenInProgress()
		default:
			resp.Error = "Unknown command"
		}

		request.Done <- resp
	}
}
