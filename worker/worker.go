package worker

import (
	//	"encoding/gob"
	"github.com/mgentili/goPhat/client"
	//"github.com/mgentili/goPhat/queueserver"
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
	w := new(Worker)
	w.SeqNumber = 0
	w.Cli, err = client.NewClient(servers, id, uid)
	if err != nil {
		return nil, err
	}

	// We need to register the DataNode and StatNode before we can use them in gob

	return w, nil
}

func (w *Worker) Pop() (string, error) {
	//args := new(queueserver.ClientCommand{w.Cli.Uid, w.SeqNumber, ""})
	args := new(struct{})
	response := new(string)
	w.SeqNumber++
	err := w.Cli.RpcClient.Call("Server.Pop", args, response)
	if err != nil {
		return "", err
	}
	return *response, err
}

func (w *Worker) Done() error {
	//args := new(queueserver.ClientCommand{w.Cli.Uid, w.SeqNumber, ""})
	args := new(struct{})
	response := new(struct{})
	err := w.Cli.RpcClient.Call("Server.Done", args, response)
	return err
}
