package phatqueue

import "strconv"

type QCommand struct {
	Command string
	Value   string
}

type QResponse struct {
	Reply interface{}
	Error string
}

type QCommandWithChannel struct {
	Cmd  *QCommand
	Done chan *QResponse
}

func QueueServer(input chan QCommandWithChannel) {
	// Set up the queue
	mq := MessageQueue{}
	mq.Init()
	// Enter the command loop
	for {
		request := <-input
		req := request.Cmd
		resp := &QResponse{}
		switch req.Command {
		case "PUSH":
			mq.Push(req.Value)
		case "POP":
			v := mq.Pop()
			if v == nil {
				resp.Reply = v
			}
		case "DONE":
			mid, err := strconv.Atoi(req.Value)
			if err == nil {
				mq.Done(mid)
			} else {
				resp.Error = "Conversion from string to integer had an issue"
			}
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
