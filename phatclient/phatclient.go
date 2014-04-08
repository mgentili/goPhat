package phatclient

import (
	"encoding/gob"
	"errors"
	"github.com/mgentili/goPhat/phatdb"
	"github.com/mgentili/goPhat/level_log"
	"net/rpc"
	"os"
	"time"
	"fmt"
)

const (
	DefaultTimeout = time.Duration(5) * time.Second
	ClientTimeout  = time.Duration(3) * time.Second
	ServerTimeout  = time.Duration(2) * time.Second
	DEBUG = 0
	TRACK = 1
	CALL = 2
	DEBUG_LOCATION = "debug.txt"
	TRACK_LOCATION = "track.txt"
)


func (c *PhatClient) StringToError(s string) error {
	//c.log.Printf(TRACK, "Convert to err:", s)
	if s == "" {
		return nil
	}
	return errors.New(s)
}

type PhatClient struct {
	Timeout         time.Duration
	ServerLocations []string    //addresses of all servers
	NumServers      uint        //length of ServerLocations
	MasterId        uint        //id of master server
	Id              uint        //id of currently connected server
	Uid	string //unique identifier of this client
	RpcClient       *rpc.Client //client connection to server (usually the master)
	log *level_log.Logger //individual client's log
}

func (c *PhatClient) SetupClientLog() {
	levelsToLog := []int{DEBUG, TRACK, CALL}
	c.log = level_log.NewLL(os.Stdout, fmt.Sprintf("%s: ", c.Uid))
	c.log.SetLevelsToLog(levelsToLog)
}

// NewClient creates a new client connected to the server with given id
// and attempts to connect to the master server
func NewClient(servers []string, id uint, uid string) (*PhatClient, error) {

	c := new(PhatClient)

	// We need to register the DataNode and StatNode before we can use them in gob
	gob.Register(phatdb.DataNode{})
	gob.Register(phatdb.StatNode{})

	c.ServerLocations = servers
	c.NumServers = uint(len(servers))
	c.Id = id
	c.MasterId = 0
	c.Uid = uid
	c.Timeout = DefaultTimeout
	c.SetupClientLog()
	err := c.connectToServer(id)
	if err != nil {
		c.log.Printf(DEBUG, "NewClient failed to connect client to server with id %d, error %s", id, err.Error())
		return nil, err
	}

	err = c.connectToMaster()
	if err != nil {
		c.log.Printf(DEBUG, "NewClient failed to connect client to the master server, error %s", err.Error())
		return c, err
	}

	return c, nil
}

// connectToAnyServer connects client to server with given index
func (c *PhatClient) connectToServer(index uint) error {
	c.log.Printf(TRACK, "Trying to connect to server %d", index)
	client, err := rpc.Dial("tcp", c.ServerLocations[index])
	if err != nil {
		return err
	}

	c.Id = index
	c.RpcClient = client
	return nil
	}

// connectToMaster connects client to the current master node
func (c *PhatClient) connectToMaster() error {
	c.log.Printf(TRACK, "Trying to connect to master %d", c.MasterId)
	//connect to any server, and get the master id
	loop:
	for i := uint(0); i < c.NumServers; i = i + 1 {
		timer := time.NewTimer(time.Second)
		call := c.RpcClient.Go("Server.GetMaster", new(struct{}), &c.MasterId, nil)
		select {
		case <-timer.C:
			c.log.Printf(TRACK, "GetMaster timed out!")
		case <-call.Done:
			c.log.Printf(TRACK, "Got response from server!")
			if call.Error == nil {
				c.log.Printf(TRACK, "The master is %d", c.MasterId)
				break loop
			} else {
				c.log.Printf(TRACK, "Errored when calling get master %v", call.Error)
			}
		}
		
		//if problem with RPC or server is in recovery, need to connect to different server
		time.Sleep(DefaultTimeout/10)
		c.connectToServer((c.Id + uint(i+1)) % c.NumServers)
	}

	// If the currently connected server isn't the master, connect to master
	if c.MasterId != c.Id {
		c.log.Printf(TRACK, "Called Server.GetMaster, current master id is %d, my id is %d",
			c.MasterId, c.Id)
		err := c.connectToServer(c.MasterId)
		if err != nil {
			return err
		}
		c.log.Printf(TRACK, "Now current master id is %d, my id is %d\n", c.MasterId, c.Id)
	}

	return nil
}

// processCallWithRetry tries to make a client call until a timeout triggers
// retries happen when the RPC call fails
func (c *PhatClient) processCallWithRetry(args *phatdb.DBCommand) (*phatdb.DBResponse, error) {
	reply := &phatdb.DBResponse{}
	timer := time.NewTimer(DefaultTimeout)
	giveupTimer := time.NewTimer(DefaultTimeout*5)

	var replyErr error

	for {
		dbCall := c.RpcClient.Go("Server.RPCDB", args, reply, nil)
		select {
		case <-giveupTimer.C:
			c.log.Fatal(TRACK, "Client completely giving up on this call")
			return nil, errors.New("Completely timed out")
		case <-timer.C:
			c.log.Printf(TRACK, "Single call timed out")
			c.connectToMaster()
		case <-dbCall.Done:
			if dbCall.Error == nil {
				c.log.Printf(TRACK, "Call done with no error")
				replyErr = c.StringToError(reply.Error)
				if replyErr != nil {
					return nil, replyErr
				}
				return reply, replyErr
			}
			c.log.Printf(TRACK, "Call somehow failed with error %v", dbCall.Error)
			time.Sleep(DefaultTimeout / 10)
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
	replyErr := c.StringToError(reply.Error)
	if replyErr != nil {
		return nil, replyErr
	}
	return reply, replyErr
}

func (c *PhatClient) Create(subpath string, initialdata string) (*phatdb.DataNode, error) {
	c.log.Printf(CALL, "Creating file")
	args := &phatdb.DBCommand{"CREATE", subpath, initialdata}
	reply, err := c.processCallWithRetry(args)
	if err != nil {
		return nil, err
	}
	c.log.Printf(CALL, "Finished creating file")
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
	c.log.Printf(CALL, "Setting Data")
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
