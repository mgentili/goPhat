package phatqueue

import (
	"fmt"
)

type QCommand struct {
	Command string
	Value   interface{}
}

type QResponse struct {
	Reply interface{}
	Error string
}

type QCommandWithChannel struct {
	Cmd  *QCommand
	Done chan *QResponse
}

type QSnapshot struct {
	Data          []byte
	SnapshotIndex uint
}

func QueueServer(input chan QCommandWithChannel) {
	// Set up the queue
	mq := new(MessageQueue)
	mq.Init()
	copyOnWrite := false
	// Enter the command loop
	for {
		request := <-input
		req := request.Cmd
		resp := &QResponse{}

		if copyOnWrite {
			switch req.Command {
			case "PUSH", "POP", "DONE":
				// we're writing, so we need to do a copy
				fmt.Printf("copying the queue because copy on write")
				mq = mq.Copy()
				copyOnWrite = false
			}
		}

		switch req.Command {
		case "PUSH":
			mq.Push(req.Value.(string))
		case "POP":
			v := mq.Pop()
			if v != nil {
				resp.Reply = v
			} else {
				resp.Error = "Nothing to pop"
			}
		case "DONE":
			mq.Done(req.Value.(string))
		case "LEN":
			resp.Reply = mq.Len()
		case "LEN_IN_PROGRESS":
			resp.Reply = mq.LenInProgress()
		case "SNAPSHOT":
			// need to ask for the index here, to guarantee it's the current one
			index := req.Value.(func() uint)()
			// TODO: set copyOnWrite and do this in the background
			bytes, err := mq.Bytes()
			if err != nil {
				resp.Error = err.Error()
			} else {
				resp.Reply = QSnapshot{bytes, index}
			}
		default:
			resp.Error = "Unknown command"
		}

		request.Done <- resp
	}
}
