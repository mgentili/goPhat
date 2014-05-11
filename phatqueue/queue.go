package phatqueue

import "strconv"
import "bytes"
import "encoding/gob"

type QMessage struct {
	MessageID string
	Value     interface{}
}

type LogEntry struct {
    Message QMessage
    Command string
}

type MessageQueue struct {
	Queue           []QMessage
	InProgress      map[string]QMessage
	Id              int
}

func (mq *MessageQueue) Init() {
	mq.InProgress = make(map[string]QMessage)
}

func (mq *MessageQueue) NextID() int {
	mq.Id += 1
	return mq.Id
}

func (mq *MessageQueue) Copy() (newmq *MessageQueue) {
    newmq = new(MessageQueue)
    newmq.Init()

    // newmq.queue is initially empty so this effectively copies mq.queue to it
    copy(newmq.Queue, mq.Queue)
    for k, v := range mq.InProgress {
        newmq.InProgress[k] = v
    }
    newmq.Id = mq.Id
    return
}

func (mq *MessageQueue) Push(v interface{}) {
	qm := QMessage{strconv.Itoa(mq.NextID()), v}
	mq.Queue = append(mq.Queue, qm)
}

func (mq *MessageQueue) Pop() *QMessage {
	if mq.Len() == 0 {
		return nil
	}
    var qm QMessage
    qm, mq.Queue = mq.Queue[len(mq.Queue)-1], mq.Queue[:len(mq.Queue)-1]
	return &qm
}

func (mq *MessageQueue) Done(mId string) {
	delete(mq.InProgress, mId)
}

func (mq *MessageQueue) Len() int {
	return len(mq.Queue)
}

func (mq *MessageQueue) LenInProgress() int {
	return len(mq.InProgress)
}

//convert the queue to a byte slice
func (mq *MessageQueue) Bytes() ([]byte, error) {
	var queueState bytes.Buffer
	enc := gob.NewEncoder(&queueState)
	err := enc.Encode(mq.Queue)
	err = enc.Encode(mq.InProgress)
	err = enc.Encode(mq.Id)
	if err != nil {
		return nil, err
	}
	return queueState.Bytes(), nil
}
