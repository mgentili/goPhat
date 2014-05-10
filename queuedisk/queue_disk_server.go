package queuedisk

import "os"
import "io/ioutil"
import "encoding/gob"
import "encoding/binary"
import "bytes"
import "log"

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

func ParseLogFile(buffer []byte) []LogEntry {
    gob.Register(LogEntry{})
    logEntries := []LogEntry{}

    buf_len := len(buffer)
    beg_idx := 0
    int_len := 4

    for beg_idx < buf_len {
        le := LogEntry{}

        length := int(binary.LittleEndian.Uint32(buffer[beg_idx:beg_idx + int_len]))
        beg_idx += int_len
        decoder := gob.NewDecoder(bytes.NewBuffer(buffer[beg_idx:beg_idx+length]))
        decoder.Decode(&le)
        logEntries = append(logEntries, le)
        beg_idx += length
    }
    return logEntries
}

func QueueServer(input chan QCommandWithChannel) {
	// Set up the queue
	mq := MessageQueue{}
	mq.Init()

	//if backup exists restore, otherwise we are just starting
	if _, err := os.Stat(queue_file); !os.IsNotExist(err) {
		var r []byte
		r, err = ioutil.ReadFile(queue_file)
        logEntries := ParseLogFile(r)
        mq.RecoverLog(logEntries)
        log.Println("%v,", mq.Len())
	}

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
			if v != nil {
				resp.Reply = v
			} else {
				resp.Error = "Nothing to pop"
			}
		case "DONE":
			mq.Done(req.Value)
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
