package gophat

import (
	"time"
	"net/rpc"
	"fmt"
	"log"
	"os"
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
}

type Null struct {}

//creates a new client connected to the server with given address
//and then tries to connect to master server
func NewClient(server string) *PhatClient {
	client, err := rpc.Dial("tcp", server)
	checkError(err)

	c := new(PhatClient)
	c.RpcClient = client
	c.Timeout = DefaultTimeout
	c.CurrentLocation = server

	c.connectToMaster()

	return c
}

//connects to current Phat Master
func (c *PhatClient) connectToMaster() {
	fmt.Println("Connecting to master")
	err := c.RpcClient.Call("Server.Getmaster", new(Null), &c.MasterLocation)
	if err != nil {
		log.Fatal("Cannot get master location");
	}

	//if server connected to isn't the master, connect to master
	if c.MasterLocation != c.CurrentLocation {
		client, err := rpc.Dial("tcp", c.MasterLocation)
		checkError(err)
		c.RpcClient.Close()
		c.RpcClient = client
	}
}

//returns handle to the root node of file system
func (c *PhatClient) getroot() Handle {
	var args RPCArgs
	var reply Handle
	err := c.RpcClient.Call("Server.Getroot", &args, &reply)
	checkError(err)
	return reply
}

func (c *PhatClient) open(h Handle, subpath string) Handle{
	args := RPCArgs{h, subpath, ""}
	var reply Handle
	err := c.RpcClient.Call("Server.Open", &args, &reply)
	checkError(err)
	return reply
}

func mkfile(h Handle, subpath string, initialdata string) Handle {
	var nh Handle
	return nh
}

func mkdir(h Handle, subpath string) Handle {
	var nh Handle
	return nh
}

func getcontents(h Handle, subpath string) Handle {
	var nh Handle
	return nh
}

func putcontents(h Handle, subpath string) error {
	return nil
}

func readdir(h Handle) string {
	return "/"
}

func stat(h Handle) Metadata {
	var md Metadata
	return md
}

func flock(h Handle, lt LockType) Sequencer {
	var sq Sequencer
	return sq
}

func funlock(h Handle) Sequencer {
	var sq Sequencer
	return sq
}

func delete(h Handle) error {
	return nil
}

func checkError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}