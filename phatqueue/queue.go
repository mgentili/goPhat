package phatqueue

import "container/list"
import "strconv"

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
	mq.inProgress[qmesg.MessageID] = qmesg
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
