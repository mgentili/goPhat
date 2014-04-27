package phatclient

import (
	"encoding/gob"
	"errors"
	"github.com/mgentili/goPhat/client"
	"github.com/mgentili/goPhat/phatdb"
	"time"
)

const (
	DefaultTimeout = time.Duration(5) * time.Second
	ClientTimeout  = time.Duration(3) * time.Second
	ServerTimeout  = time.Duration(2) * time.Second
	DEBUG          = 0
	STATUS         = 1
	CALL           = 2
)


type PhatClient struct {
	Cli *client.Client
}

func (c *PhatClient) debug(level int, format string, args ...interface{}) {
	c.Cli.Log.Printf(level, format, args...)
}

// NewClient creates a new client connected to the server with given id
// and attempts to connect to the master server
func NewClient(servers []string, id uint, uid string) (*PhatClient, error) {
	var err error
	c := new(PhatClient)
	c.Cli, err = client.NewClient(servers, id, uid)
	if err != nil {
		return nil, err
	}

	// We need to register the DataNode and StatNode before we can use them in gob
	gob.Register(phatdb.DataNode{})
	gob.Register(phatdb.StatNode{})

	return c, nil
}

// processCallWithRetry tries to make a client call until a timeout triggers
// retries happen when the RPC call fails
func (c *PhatClient) processCallWithRetry(args *phatdb.DBCommand) (*phatdb.DBResponse, error) {
	reply := &phatdb.DBResponse{}
	timer := time.NewTimer(DefaultTimeout)
	giveupTimer := time.NewTimer(DefaultTimeout * 10)

	var replyErr error

	for {
		dbCall := c.Cli.RpcClient.Go("Server.RPCDB", args, reply, nil)
		select {
		case <-giveupTimer.C:
			c.debug(DEBUG, "Client completely giving up on this call")
			return nil, errors.New("Completely timed out")
		case <-timer.C:
			c.debug(DEBUG, "Single call timed out")
			c.Cli.ConnectToMaster()
			timer.Reset(DefaultTimeout)
		case <-dbCall.Done:
			if dbCall.Error == nil {
				c.debug(STATUS, "Call done with no error")
				replyErr = c.Cli.StringToError(reply.Error)
				if replyErr != nil {
					return nil, replyErr
				}
				return reply, nil
			}
			c.debug(DEBUG, "Call failed with error %v", dbCall.Error)
			time.Sleep(DefaultTimeout / 10)
			//error possibilities 1) network failure 2) server can't process request
			c.Cli.ConnectToMaster()
		}
	}
}

func (c *PhatClient) Create(subpath string, initialdata string) (*phatdb.DataNode, error) {
	c.debug(STATUS, "Creating file %s with data %s", subpath, initialdata)
	args := &phatdb.DBCommand{"CREATE", subpath, initialdata}
	reply, err := c.processCallWithRetry(args)
	if err != nil {
		c.debug(DEBUG, "Create file %s errored %s", subpath, err)
		return nil, err
	}
	c.debug(CALL, "Finished creating file %s with data %s", subpath, initialdata)
	n := reply.Reply.(phatdb.DataNode)
	return &n, err
}

func (c *PhatClient) GetData(subpath string) (*phatdb.DataNode, error) {
	args := &phatdb.DBCommand{"GET", subpath, ""}
	reply, err := c.processCallWithRetry(args)
	if err != nil {
		return nil, err
	}
	n := reply.Reply.(phatdb.DataNode)

	return &n, err
}

func (c *PhatClient) SetData(subpath string, data string) error {
	c.debug(STATUS, "Setting Data")
	args := &phatdb.DBCommand{"SET", subpath, data}
	_, err := c.processCallWithRetry(args)
	if err != nil {
		return err
	}
	return err
}

func (c *PhatClient) GetChildren(subpath string) ([]string, error) {
	args := &phatdb.DBCommand{"CHILDREN", subpath, ""}
	reply, err := c.processCallWithRetry(args)
	if err != nil {
		return nil, err
	}
	return reply.Reply.([]string), err
}

func (c *PhatClient) GetStats(subpath string) (*phatdb.StatNode, error) {
	args := &phatdb.DBCommand{"STAT", subpath, ""}
	reply, err := c.processCallWithRetry(args)
	if err != nil {
		return nil, err
	}
	n := reply.Reply.(phatdb.StatNode)
	return &n, err
}

// Delete deletes a node if it doesn't have any children
func (c *PhatClient) Delete(subpath string) error {
	args := &phatdb.DBCommand{"DELETE", subpath, ""}
	_, err := c.processCallWithRetry(args)
	return err
}

func (c *PhatClient) GetHash() (string, error) {
	args := &phatdb.DBCommand{"SHA256", "", ""}
	reply, err := c.processCallWithRetry(args)
	if err != nil {
		return "", err
	}
	n := reply.Reply.(string)

	return n, err
}
