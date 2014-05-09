package queuedisk

import "container/list"
import "strconv"
import "bytes"
import "io/ioutil"
import "encoding/gob"

var queue_file = "queue.bin"

type QMessage struct {
	MessageID string
	Value     interface{}
}

type MessageQueue struct {
	queue           list.List
	inProgress      map[string]QMessage
	id              int
	backup_filename string
}

func (mq *MessageQueue) Init() {
	mq.inProgress = make(map[string]QMessage)
	mq.backup_filename = queue_file
}

func (mq *MessageQueue) NextID() int {
	mq.id += 1
	return mq.id
}

func (mq *MessageQueue) Push(v interface{}) {
	qm := QMessage{strconv.Itoa(mq.NextID()), v}
	mq.queue.PushBack(qm)
	mq.Backup()
}

func (mq *MessageQueue) Pop() *QMessage {
	if mq.Len() == 0 {
		return nil
	}
	e := mq.queue.Front()
	qmesg := e.Value.(QMessage)
	mq.queue.Remove(e)
	// TODO: When we actually care about "in progress" messages
	// Until then, this is equivalent to a memory leak
	//mq.inProgress[qmesg.MessageID] = qmesg
	mq.Backup()
	return &qmesg
}

func (mq *MessageQueue) Done(mid string) {
	// TODO: Ensure it exists and return an error otherwise
	delete(mq.inProgress, mid)
	mq.Backup()
}

func (mq *MessageQueue) Len() int {
	return mq.queue.Len()
}

func (mq *MessageQueue) LenInProgress() int {
	return len(mq.inProgress)
}

//write copy of queue to disk
func (mq *MessageQueue) Backup() error {
	w := new(bytes.Buffer)
	encoder := gob.NewEncoder(w)
	err := encoder.Encode(mq)

	if err != nil {
		return err
	}

	err = ioutil.WriteFile(mq.backup_filename, w.Bytes(), 0600)

	if err != nil {
		return err
	}

	return nil
}
