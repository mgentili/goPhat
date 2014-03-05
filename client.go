package gophat

import (
	"net/rpc"
	"time"
	//	"fmt"
	//	"errors"
	"github.com/mgentili/goPhat/phatdb"
	"log"
	//	"os"
)

const DefaultTimeout = time.Duration(100) * time.Millisecond

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

//creates a new client connected to the server with given id
//and then tries to connect to master server
func NewClient(servers []string, id int) (*PhatClient, error) {
	c := new(PhatClient)
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

//connects to current Phat Master
func (c *PhatClient) connectToMaster() error {
	log.Println("Connecting to master")

	/*f := func() *rpc.Call {return c.RpcClient.Go("Server.Getmaster", new(Null), &c.MasterLocation, nil)}
	err := doWithTimeout(time.Second, f)*/
	err := c.RpcClient.Call("Server.GetMaster", new(Null), &c.MasterId)
	if err != nil {
		log.Println(err)
		return err
	}
	//if server connected to isn't the master, connect to master
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

//returns handle to the root node of file system
func (c *PhatClient) GetRoot() (*phatdb.DataNode, error) {
	args := phatdb.DBCommand{"GETROOT", "", ""}
	var reply phatdb.DBResponse
	err := c.RpcClient.Call("Server.RPCDB", &args, &reply)
	if err != nil {
		return nil, err
	}

	return reply.Reply.(*phatdb.DataNode), reply.Error
}

/*func (c *PhatClient) open(subpath string) (p, error){
	args := phatdb.DBCommand{"OPEN", subpath, ""}
	var reply phatdb.DBResponse
	err := c.RpcClient.Call("Server.RPCDB", &args, &reply)

	return reply, err
}*/

func (c *PhatClient) Mkfile(subpath string, initialdata string) (*phatdb.DataNode, error) {
	args := phatdb.DBCommand{"CREATE", subpath, initialdata}
	var reply phatdb.DBResponse
	err := c.RpcClient.Call("Server.RPCDB", &args, &reply)
	log.Printf("Mkfile finished!\n")
	if err != nil {
		return nil, err
	}
	return reply.Reply.(*phatdb.DataNode), reply.Error
}

func (c *PhatClient) Mkdir(subpath string) (*phatdb.DataNode, error) {
	args := phatdb.DBCommand{"CREATE", subpath, ""}
	var reply phatdb.DBResponse
	err := c.RpcClient.Call("Server.RPCDB", &args, &reply)
	if err != nil {
		return nil, err
	}

	return reply.Reply.(*phatdb.DataNode), reply.Error
}

func (c *PhatClient) GetContents(subpath string) (*phatdb.DataNode, error) {
	args := phatdb.DBCommand{"GET", subpath, ""}
	var reply phatdb.DBResponse
	log.Printf("Getting contents with subpath %v\n", subpath)
	err := c.RpcClient.Call("Server.RPCDB", &args, &reply)
	log.Printf("Call finished!\n")
	if err != nil {
		return nil, err
	}

	if reply.Error != nil {
		return nil, reply.Error
	}
	log.Printf("Got to bottom without erroring\n")
	return reply.Reply.(*phatdb.DataNode), reply.Error
}

func (c *PhatClient) PutContents(subpath string, data string) error {
	args := phatdb.DBCommand{"SET", subpath, data}
	var reply phatdb.DBResponse
	err := c.RpcClient.Call("Server.RPCDB", &args, &reply)
	if err != nil {
		return err
	}

	return reply.Error
}

func (c *PhatClient) ReadDir(subpath string) (*phatdb.DataNode, error) {
	args := phatdb.DBCommand{"GET", subpath, ""}
	var reply phatdb.DBResponse
	err := c.RpcClient.Call("Server.RPCDB", &args, &reply)
	if err != nil {
		return nil, err
	}

	return reply.Reply.(*phatdb.DataNode), reply.Error
}

func (c *PhatClient) Stat(subpath string) (*phatdb.StatNode, error) {
	args := phatdb.DBCommand{"STAT", subpath, ""}
	var reply phatdb.DBResponse
	err := c.RpcClient.Call("Server.RPCDB", &args, &reply)
	if err != nil {
		return nil, err
	}

	return reply.Reply.(*phatdb.StatNode), reply.Error
}

/*func Flock(h Handle, lt LockType) Sequencer {
	var sq Sequencer
	return sq
}

func Funlock(h Handle) Sequencer {
	var sq Sequencer
	return sq
}

func Delete(h Handle) error {
	return nil
}*/

//performs a Go RPC call with a given timeout, returning an
//error if the response isn't quick enough
/*type toRun func ()

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
}*/
