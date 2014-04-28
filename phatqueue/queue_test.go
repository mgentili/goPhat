package phatqueue

import (
	"testing"
)

func TestExistsNode(t *testing.T) {
	mq := MessageQueue{}
	mq.Init()
	//
	mq.Push(1)
	mq.Push("2")
	mq.Push(4)
	mq.Push("Eight")
	//
	if mq.Len() != 4 {
		t.Errorf("Incorrect expected queue length")
	}
	//
	qmesg := mq.Pop()
	if qmesg.Value != 1 {
		t.Errorf("Expected result was not returned")
	}
	mq.Done(qmesg.MessageID)
	//
	qmesg = mq.Pop()
	if qmesg.Value != "2" {
		t.Errorf("Expected result was not returned")
	}
	mq.Done(qmesg.MessageID)
	//
	if mq.Len() != 2 {
		t.Errorf("Incorrect expected queue length")
	}
	//
	qmesg = mq.Pop()
	if qmesg.Value != 4 {
		t.Errorf("Expected result was not returned")
	}
	mq.Done(qmesg.MessageID)
	//
	qmesg = mq.Pop()
	if qmesg.Value != "Eight" {
		t.Errorf("Expected result was not returned")
	}
	mq.Done(qmesg.MessageID)
	//
}
