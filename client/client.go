package client

import (
	"errors"
	"fmt"
	"github.com/mgentili/goPhat/level_log"
	"net/rpc"
	"os"
	"time"
)

const (
	DEBUG          = 0
	STATUS         = 1
	CALL           = 2
)

type Client struct {
	ServerLocations []string          //addresses of all servers
	NumServers      uint              //length of ServerLocations
	MasterId        uint              //id of master server
	Id              uint              //id of currently connected server
	Uid             string            //unique identifier of this client
	RpcClient       *rpc.Client       //client connection to server (usually the master)
	Log             *level_log.Logger //individual client's log
}

func (c *Client) SetupClientLog() {
	levelsToLog := []int{DEBUG, STATUS, CALL}
	c.Log = level_log.NewLL(os.Stdout, fmt.Sprintf("%s: ", c.Uid))
	c.Log.SetLevelsToLog(levelsToLog)
}

func (c *Client) StringToError(s string) error {
	if s == "" {
		return nil
	}
	return errors.New(s)
}

// NewClient creates a new client connected to the server with given id
// and attempts to connect to the master server
func NewClient(servers []string, id uint, uid string) (*Client, error) {

	c := new(Client)

	c.ServerLocations = servers
	c.NumServers = uint(len(servers))
	c.Id = id
	c.MasterId = 0
	c.Uid = uid
	c.SetupClientLog()
	err := c.ConnectToServer(id)
	if err != nil {
		c.Log.Printf(DEBUG, "NewClient failed to connect client to server with id %d, error %s", id, err.Error())
		return nil, err
	}

	err = c.ConnectToMaster()
	if err != nil {
		c.Log.Printf(DEBUG, "NewClient failed to connect client to the master server, error %s", err.Error())
		return c, err
	}

	return c, nil
}

// connectToAnyServer connects client to server with given index
func (c *Client) ConnectToServer(index uint) error {
	c.Log.Printf(STATUS, "Trying to connect to server %d", index)
	client, err := rpc.Dial("tcp", c.ServerLocations[index])
	if err != nil {
		return err
	}

	c.Id = index
	c.RpcClient = client
	return nil
}

// connectToMaster connects client to the current master node
func (c *Client) ConnectToMaster() error {
	c.Log.Printf(STATUS, "Trying to connect to master %d", c.MasterId)
	//connect to any server, and get the master id
loop:
	for i := uint(0); i < c.NumServers; i = i + 1 {
		timer := time.NewTimer(time.Second)
		call := c.RpcClient.Go("Server.GetMaster", new(struct{}), &c.MasterId, nil)
		select {
		case <-timer.C:
			c.Log.Printf(DEBUG, "GetMaster timed out!")
		case <-call.Done:
			if call.Error == nil {
				c.Log.Printf(STATUS, "The master is %d", c.MasterId)
				break loop
			} else {
				c.Log.Printf(DEBUG, "Errored when asking server %d for master info: %v", c.Id, call.Error)
			}
		}

		//if problem with RPC or server is in recovery, need to connect to different server
		time.Sleep(time.Second)
		c.ConnectToServer((c.Id + uint(i+1)) % c.NumServers)
	}

	// If the currently connected server isn't the master, connect to master
	if c.MasterId != c.Id {
		c.Log.Printf(STATUS, "Called Server.GetMaster, current master id is %d, my id is %d",
			c.MasterId, c.Id)
		err := c.ConnectToServer(c.MasterId)
		if err != nil {
			return err
		}
		c.Log.Printf(STATUS, "Now current master id is %d, my id is %d\n", c.MasterId, c.Id)
	}

	return nil
}