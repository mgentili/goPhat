package phatqueue

import "container/list"

type QMessage struct {
	MessageID int
	Value     interface{}
}

type MessageQueue struct {
	queue      list.List
	inProgress map[int]QMessage
	id         int
}

func (mq *MessageQueue) Init() {
	mq.inProgress = make(map[int]QMessage)
}

func (mq *MessageQueue) NextID() int {
	mq.id += 1
	return mq.id
}

func (mq *MessageQueue) Push(v interface{}) {
	qm := QMessage{mq.NextID(), v}
	mq.queue.PushBack(qm)
}

func (mq *MessageQueue) Pop() *QMessage {
	if mq.Len() == 0 {
		return nil
	}
	e := mq.queue.Front()
	mq.queue.Remove(e)
	qm := e.Value.(QMessage)
	mq.inProgress[qm.MessageID] = qm
	return &qm
}

func (mq *MessageQueue) Done(mid int) {
	// TODO: Ensure it exists and return an error otherwise
	delete(mq.inProgress, mid)
}

func (mq *MessageQueue) Len() int {
	return mq.queue.Len()
}

func (mq *MessageQueue) LenInProgress() int {
	return len(mq.inProgress)
}
