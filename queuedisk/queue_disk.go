package queuedisk

import "container/list"
import "strconv"
import "bytes"
import "os"
import "encoding/gob"
import "encoding/binary"
import "github.com/mgentili/goPhat/phatlog"

var queue_file = "queue.bin"

type QMessage struct {
	MessageID string
	Value     interface{}
}

type LogEntry struct {
    Message QMessage
    Command string
}

type MessageQueue struct {
	Queue           list.List
	inProgress      map[string]QMessage
	Id              int
    Log *phatlog.Log
	backup_filename string
    filePtr *os.File
}

func (mq *MessageQueue) Init() {
	mq.inProgress = make(map[string]QMessage)
	mq.backup_filename = queue_file
	mq.Log = phatlog.EmptyLog()
    mq.filePtr, _  = os.OpenFile(mq.backup_filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
}

func (mq *MessageQueue) NextID() int {
	mq.Id += 1
	return mq.Id
}

func (mq *MessageQueue) RecoverLog(logEntries []LogEntry) {
    for _, entry := range logEntries {
        switch entry.Command {
            case "PUSH":
                mq.ReplayPush(entry.Message)
            case "POP":
                mq.ReplayPop()
            }
    }
}

func (mq *MessageQueue) Push(v interface{}) {
	qm := QMessage{strconv.Itoa(mq.NextID()), v}
	mq.Queue.PushBack(qm)
    mq.BackupLog(LogEntry{Message:qm, Command:"PUSH"})
}

func (mq *MessageQueue) Pop() *QMessage {
	if mq.Len() == 0 {
		return nil
	}
	e := mq.Queue.Front()
	qm := e.Value.(QMessage)
	mq.Queue.Remove(e)
	// TODO: When we actually care about "in progress" messages
	// Until then, this is equivalent to a memory leak
	//mq.inProgress[qmesg.MessageID] = qmesg
    mq.BackupLog(LogEntry{Command:"POP"})
	return &qm
}

//ReplayPush/Pop modify the queue in the same way, but do not add to
//logging file (since they are already there!)
func (mq *MessageQueue) ReplayPush(v interface{}) {
	qm := QMessage{strconv.Itoa(mq.NextID()), v}
	mq.Queue.PushBack(qm)
}

func (mq *MessageQueue) ReplayPop() *QMessage {
	if mq.Len() == 0 {
		return nil
	}
	e := mq.Queue.Front()
	qm := e.Value.(QMessage)
	mq.Queue.Remove(e)
	// TODO: When we actually care about "in progress" messages
	// Until then, this is equivalent to a memory leak
	//mq.inProgress[qmesg.MessageID] = qmesg
	return &qm
}

func (mq *MessageQueue) Done(mId string) {
	// TODO: Ensure it exists and return an error otherwise
	delete(mq.inProgress, mId)
}

func (mq *MessageQueue) Len() int {
	return mq.Queue.Len()
}

func (mq *MessageQueue) LenInProgress() int {
	return len(mq.inProgress)
}

//write copy of log to disk
func (mq *MessageQueue) BackupLog(logentry LogEntry) error {
    gob.Register(LogEntry{})
    w := new(bytes.Buffer)
    encoder := gob.NewEncoder(w)
    err := encoder.Encode(logentry)

    if err != nil {
		return err
	}

    //write the size of the Logentry to the file
    bs := make([]byte, 4)
    binary.LittleEndian.PutUint32(bs, uint32(len(w.Bytes())))
    _, err = mq.filePtr.Write(bs)

    //write the LogEntry to the file
    _, err = mq.filePtr.Write(w.Bytes())

	if err != nil {
		return err
	}

    mq.filePtr.Sync()

	return nil
}
