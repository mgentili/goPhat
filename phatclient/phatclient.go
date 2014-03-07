package phatclient

import (
	"encoding/gob"
	"errors"
	"github.com/mgentili/goPhat/phatdb"
	"log"
	"net/rpc"
	"time"
)

const (
	DefaultTimeout = time.Duration(1) * time.Second
)

func StringToError(s string) error {
	log.Println("Convert to err:", s)
	if s == "" {
		return nil
	}
	return errors.New(s)
}

type PhatClient struct {
	Cache           map[string]string //all data that client has entered
	Timeout         time.Duration
	ServerLocations []string    //addresses of all servers
	NumServers      int         //length of ServerLocations
	MasterId        int         //id of master server
	Id              int         //id of currently connected server
	RpcClient       *rpc.Client //client connection to server (usually the master)
	invalidateChan  chan string //channel that client listens to for cache invalidation
}

type Null struct{}

// NewClient creates a new client connected to the server with given id
// and attempts to connect to the master server
func NewClient(servers []string, id int) (*PhatClient, error) {
	c := new(PhatClient)

	// We need to register the DataNode and StatNode before we can use them in gob
	gob.Register(phatdb.DataNode{})
	gob.Register(phatdb.StatNode{})
	c.ServerLocations = servers
	c.NumServers = len(servers)
	c.Id = id

	err := c.connectToServer(id)
	if err != nil {
		log.Printf("NewClient failed to connect client to given server")
		return nil, err
	}

	c.Timeout = DefaultTimeout

	err = c.connectToMaster()
	if err != nil {
		return c, err
	}

	return c, nil
}

// connectToAnyServer connects client to server with given index
func (c *PhatClient) connectToServer(index int) error {
	client, err := rpc.Dial("tcp", c.ServerLocations[index])
	if err == nil {
		c.Id = index
		c.RpcClient = client
		return nil
	}
	return err
}

// connectToMaster connects client to the current master node
func (c *PhatClient) connectToMaster() error {
	log.Println("Connecting to the master...")

	//connect to any server, and get the master id
	for i := 0; i < c.NumServers; i = i + 1 {
		err := c.RpcClient.Call("Server.GetMaster", new(Null), &c.MasterId)
		if err == nil {
			break
		}
		//if problem with RPC or server is in recovery, need to connect to different server
		c.connectToServer((c.Id + i + 1) % c.NumServers)
	}

	// If the currently connected server isn't the master, connect to master
	if c.MasterId != c.Id {
		log.Printf("Called Server.GetMaster, current master id is %d, my id is %d",
			c.MasterId, c.Id)
		client, err := rpc.Dial("tcp", c.ServerLocations[c.MasterId])
		if err != nil {
			return err
		}
		c.RpcClient.Close()
		c.RpcClient = client
		c.Id = c.MasterId
		log.Printf("Now current master id is %d, my id is %d\n", c.MasterId, c.Id)
	}

	return nil
}

// processCallWithRetry tries to make a client call until a timeout triggers
// retries happen when the RPC call fails
func (c *PhatClient) processCallWithRetry(args *phatdb.DBCommand) (*phatdb.DBResponse, error) {
	reply := &phatdb.DBResponse{}
	timeout := make(chan bool, 1)

	go func() {
		time.Sleep(DefaultTimeout)
		timeout <- true
	}()

	var replyErr error

	for {
		dbCall := c.RpcClient.Go("Server.RPCDB", args, reply, nil)
		select {
		case <-timeout:
			return nil, errors.New("Timed out")
		case <-dbCall.Done:
			replyErr = StringToError(reply.Error)
			if dbCall.Error == nil {
				if replyErr == nil {
					return reply, replyErr
				} else {
					return nil, replyErr
				}
			}
			//error possibilities 1) network failure 2) server can't process request
			c.connectToMaster()
		}
	}
}

func (c *PhatClient) processCall(args *phatdb.DBCommand) (*phatdb.DBResponse, error) {
	reply := &phatdb.DBResponse{}

	err := c.RpcClient.Call("Server.RPCDB", args, reply)
	if err != nil {
		return nil, err
	}
	replyErr := StringToError(reply.Error)
	if replyErr != nil {
		return nil, replyErr
	}
	return reply, replyErr
}

func (c *PhatClient) Create(subpath string, initialdata string) (*phatdb.DataNode, error) {
	args := &phatdb.DBCommand{"CREATE", subpath, initialdata}
	reply, err := c.processCallWithRetry(args)
	if err != nil {
		return nil, err
	}
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
	args := &phatdb.DBCommand{"SET", subpath, data}
	_, err := c.processCallWithRetry(args)
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
	return reply.Reply.(*phatdb.StatNode), err
}
