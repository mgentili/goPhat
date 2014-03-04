package gophat

import (
	"time"
	"net/rpc"
	"fmt"
	"errors"
	"phatdb"
//	"log"
//	"os"
)

const DefaultTimeout = time.Duration(100) * time.Millisecond

type PhatClient struct {
	Cache map[string]string //all data that client has entered
	Timeout time.Duration
	CurrentLocation string //address of currently connected server
	MasterLocation string //address and port of master server
	SlaveLocations []string //addresses and ports of slave servers
	Handles []Handle //all the handles the client holds
	RpcClient *rpc.Client //client connection to server (usually the master)
	invalidateChan chan string //channel that client listens to for cache invalidation
}

type Null struct {}

type toRun func () *rpc.Call

//creates a new client connected to the server with given address
//and then tries to connect to master server
func NewClient(server string) (*PhatClient, error) {
	client, err := rpc.Dial("tcp", server)
	if (err != nil) {
		return nil, err
	}

	c := new(PhatClient)
	c.RpcClient = client
	c.Timeout = DefaultTimeout
	c.CurrentLocation = server

	err = c.connectToMaster()
	if (err != nil) {
		return c, err
	}

	return c, nil
}	

//performs a Go RPC call with a given timeout, returning an 
//error if the response isn't quick enough
func doWithTimeout(dur time.Duration, f toRun) error {
	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(dur)
		timeout <- true
	}()
	response := f()
	select {
	case <-timeout:
		return errors.New("Timed out")
	case <-response.Done:
		if response.Error != nil {
			return response.Error
		}
	}

	return nil
}

//connects to current Phat Master
func (c *PhatClient) connectToMaster() error {
	fmt.Println("Connecting to master")

	/*f := func() *rpc.Call {return c.RpcClient.Go("Server.Getmaster", new(Null), &c.MasterLocation, nil)} 
	err := doWithTimeout(time.Second, f)*/
	err := c.RpcClient.Call("Server.GetMaster", new(Null), &c.MasterLocation)
	if err != nil {
		fmt.Println(err)
		return err
	}

	//if server connected to isn't the master, connect to master
	if c.MasterLocation != c.CurrentLocation {
		client, err := rpc.Dial("tcp", c.MasterLocation)
		if err != nil {
			return err
		}
		c.RpcClient.Close()
		c.RpcClient = client
	}

	return nil
}
type DBCommand struct {
	Command string
	Path    string
	Value   string
}

//returns handle to the root node of file system
func (c *PhatClient) getroot() (phatdb.FileNode, error) {
	args := phatdb.DBCommand{"GETROOT", "", ""}
	var reply phatdb.DBResponse
	err := c.RpcClient.Call("Server.RPCDB", &args, &reply)
	return reply.Reply.(*FileNode), err
}

func (c *PhatClient) open(subpath string) (p, error){
	args := phatdb.DBCommand{"OPEN", subpath, ""}
	var reply phatdb.DBResponse
	err := c.RpcClient.Call("Server.RPCDB", &args, &reply)

	return reply, err
}

func (c *PhatClient) mkfile(subpath string, initialdata string) (phatdb.FileNode, error) {
	args := phatdb.DBCommand{"CREATE", subpath, initialdata}
	var reply phatdb.DBResponse
	err := c.RpcClient.Call("Server.RPCDB", &args, &reply)

	return reply.Reply.(*FileNode), err
}

func (c *PhatClient) mkdir(subpath string) (phatdb.FileNode, error) {
	args := phatdb.DBCommand{"CREATE", subpath, ""}
	var reply phatdb.DBResponse
	err := c.RpcClient.Call("Server.RPCDB", &args, &reply)

	return reply.Reply.(*FileNode), err
}

func (c *PhatClient) getcontents(subpath string) (phatdb.DataNode, error) {
	args := phatdb.DBCommand{"GET", subpath, ""}
	var reply phatdb.DBResponse
	err := c.RpcClient.Call("Server.RPCDB", &args, &reply)

	return reply.Reply.(*DataNode), reply.Error
}

func (c *PhatClient) putcontents(subpath string, data string) error {
	args := phatdb.DBCommand{"SET", subpath, data}
	var reply phatdb.DBResponse
	err := c.RpcClient.Call("Server.RPCDB", &args, &reply)

	return reply.Error
}

func readdir(subpath string) (phatdb.FileNode, error) {
	args := phatdb.DBCommand{"GET", subpath, ""}
	var reply phatdb.DBResponse
	err := c.RpcClient.Call("Server.RPCDB", &args, &reply)

	return reply.Reply.(*FileNode)
}

func stat(subpath string) (phatdb.StatNode, error) {
	args := phatdb.DBCommand{"STAT", subpath, ""}
	var reply phatdb.DBResponse
	err := c.RpcClient.Call("Server.RPCDB", &args, &reply)

	return reply.Reply.(*StatNode), err
}

/*func flock(h Handle, lt LockType) Sequencer {
	var sq Sequencer
	return sq
}

func funlock(h Handle) Sequencer {
	var sq Sequencer
	return sq
}

func delete(h Handle) error {
	return nil
}*/
