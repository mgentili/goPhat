package phatclient

import (
	"encoding/gob"
	"errors"
	"github.com/mgentili/goPhat/phatdb"
	"log"
	"net/rpc"
	"time"
)

const DefaultTimeout = time.Duration(100) * time.Millisecond

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
	MasterId        int         //id of master server
	Id              int         //id of currently connected server
	RpcClient       *rpc.Client //client connection to server (usually the master)
	invalidateChan  chan string //channel that client listens to for cache invalidation
}

type Null struct{}

// Create a new client connected to the server with given id
// and attempt to connect to the master server
func NewClient(servers []string, id int) (*PhatClient, error) {
	c := new(PhatClient)
	// We need to register the DataNode and StatNode before we can use them in gob
	gob.Register(phatdb.DataNode{})
	gob.Register(phatdb.StatNode{})
	c.ServerLocations = servers
	c.Id = id

	client, err := rpc.Dial("tcp", servers[id])
	if err != nil {
		return nil, err
	}

	c.RpcClient = client
	c.Timeout = DefaultTimeout

	err = c.connectToMaster()
	if err != nil {
		return c, err
	}

	return c, nil
}

// Iterate through all servers and attempt to connect to any one
func (c *PhatClient) connectToAnyServer() error {
	for i := 0; i < len(c.ServerLocations); i++ {
		client, err := rpc.Dial("tcp", c.ServerLocations[i])
		if err == nil {
			c.Id = i
			c.RpcClient = client
			return nil
		}
	}

	return errors.New("Cannot connect to any server")
}

//connects to current Phat Master
func (c *PhatClient) connectToMaster() error {
	log.Println("Connecting to the master...")

	err := c.RpcClient.Call("Server.GetMaster", new(Null), &c.MasterId)
	if err != nil {
		log.Println(err)
		return err
	}
	// If the given server isn't the master, connect to master
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
	reply, err := c.processCall(args)
	if err != nil {
		return nil, err
	}
	n := reply.Reply.(phatdb.DataNode)
	return &n, err
}

func (c *PhatClient) GetData(subpath string) (*phatdb.DataNode, error) {
	args := &phatdb.DBCommand{"GET", subpath, ""}
	reply, err := c.processCall(args)
	if err != nil {
		return nil, err
	}
	n := reply.Reply.(phatdb.DataNode)
	return &n, err
}

func (c *PhatClient) SetData(subpath string, data string) error {
	args := &phatdb.DBCommand{"SET", subpath, data}
	_, err := c.processCall(args)
	return err
}

func (c *PhatClient) GetChildren(subpath string) ([]string, error) {
	args := &phatdb.DBCommand{"CHILDREN", subpath, ""}
	reply, err := c.processCall(args)
	if err != nil {
		return nil, err
	}
	return reply.Reply.([]string), err
}

func (c *PhatClient) GetStats(subpath string) (*phatdb.StatNode, error) {
	args := &phatdb.DBCommand{"STAT", subpath, ""}
	reply, err := c.processCall(args)
	if err != nil {
		return nil, err
	}
	return reply.Reply.(*phatdb.StatNode), err
}
