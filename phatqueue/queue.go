package phatqueue

import (
	"bytes"
	"container/list"
	"encoding/gob"
	"strconv"
)

type QMessage struct {
	MessageID string
	Value     interface{}
}

type MessageQueue struct {
	queue      list.List
	inProgress map[string]QMessage
	id         int
}

func (mq *MessageQueue) Init() {
	mq.inProgress = make(map[string]QMessage)
}

func (mq *MessageQueue) NextID() int {
	mq.id += 1
	return mq.id
}

func (mq *MessageQueue) Push(v interface{}) {
	qm := QMessage{strconv.Itoa(mq.NextID()), v}
	mq.queue.PushBack(qm)
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
	return &qmesg
}

func (mq *MessageQueue) Done(mid string) {
	// TODO: Ensure it exists and return an error otherwise
	delete(mq.inProgress, mid)
}

func (mq *MessageQueue) Len() int {
	return mq.queue.Len()
}

func (mq *MessageQueue) LenInProgress() int {
	return len(mq.inProgress)
}

func (mq *MessageQueue) Copy() (newmq *MessageQueue) {
	newmq = new(MessageQueue)
	newmq.Init()
	// newmq.queue is initially empty so this effectively copies mq.queue to it
	newmq.queue.PushBackList(&mq.queue)
	for k, v := range mq.inProgress {
		newmq.inProgress[k] = v
	}
	newmq.id = mq.id
	return
}

func (mq *MessageQueue) Bytes() ([]byte, error) {
	var queueState bytes.Buffer
	enc := gob.NewEncoder(&queueState)
	err := enc.Encode(mq.queue)
	err = enc.Encode(mq.inProgress)
	err = enc.Encode(mq.id)
	if err != nil {
		return nil, err
	}
	return queueState.Bytes(), nil
}
