package main

import (
	"encoding/gob"
	"errors"
	"github.com/mgentili/goPhat/client"
	queue "github.com/mgentili/goPhat/phatqueue"
	"github.com/mgentili/goPhat/queueRPC"
)

const (
	DEBUG  = 0
	STATUS = 1
	CALL   = 2
)

type Worker struct {
	Cli       *client.Client
	SeqNumber uint
}

func (w *Worker) debug(level int, format string, args ...interface{}) {
	w.Cli.Log.Printf(level, format, args...)
}

// NewClient creates a new client connected to the server with given id
// and attempts to connect to the master server
func NewWorker(servers []string, id uint, uid string) (*Worker, error) {
	var err error
	gob.Register(queue.QMessage{})
	w := new(Worker)
	w.SeqNumber = 0
	w.Cli, err = client.NewClient(servers, id, uid)
	if err != nil {
		return nil, err
	}

	// We need to register the DataNode and StatNode before we can use them in gob

	return w, nil
}

func (w *Worker) processCall(cmd *queue.QCommand) (*queue.QResponse, error) {
	args := &queueRPC.ClientCommand{w.Cli.Uid, w.SeqNumber, cmd}
	response := &queue.QResponse{}
	w.SeqNumber++
	err := w.Cli.RpcClient.Call("Server.Send", args, response)
	if err != nil {
		return nil, err
	}

	if response.Error != "" {
		return nil, errors.New(response.Error)
	}

	return response, err
}

func (w *Worker) Push(work string) error {
	cmd := &queue.QCommand{"PUSH", work}
	_, err := w.processCall(cmd)
	return err
}

func (w *Worker) Pop() (*queue.QResponse, error) {
	cmd := &queue.QCommand{"POP", ""}
	res, err := w.processCall(cmd)

	// TODO: Make it do something with the response?
	return res, err
}

func (w *Worker) Done() error {
	cmd := &queue.QCommand{"DONE", ""}
	_, err := w.processCall(cmd)
	return err
}
