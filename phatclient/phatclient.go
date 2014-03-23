package phatclient

import (
	"encoding/gob"
	"errors"
	"github.com/mgentili/goPhat/phatdb"
	"log"
	"net/rpc"
	"time"
	"os"
)

const (
	DefaultTimeout = time.Duration(2) * time.Second
	ClientTimeout = time.Duration(3) * time.Second
	ServerTimeout = time.Duration(2) * time.Second
	CacheTimeout = time.Duration(100) * time.Second
	AllowCaching = true
)

var client_log *log.Logger 

func StringToError(s string) error {
	client_log.Println("Convert to err:", s)
	if s == "" {
		return nil
	}
	return errors.New(s)
}

//the information stored for each entry in the cache
type CacheInfo struct {
	LastInvalidated time.Time
	Invalidated bool

	Response *phatdb.DBResponse
}

type PhatClient struct {
	Cache           map[Handle]*CacheInfo //all data that client has entered
	PathInfo map[Handle]string //maps handles to the path in the file system
	LargestHandle Handle //Handles are monotonically increasing
	Timeout         time.Duration
	ServerLocations []string    //addresses of all servers
	NumServers      uint         //length of ServerLocations
	MasterId        uint         //id of master server
	Id              uint         //id of currently connected server
	RpcClient       *rpc.Client //client connection to server (usually the master)
}

type Null struct{}

type KeepAliveReply struct {
	Invalidations []Handle
}

type LockType string
type Handle int
type Sequencer int

// NewClient creates a new client connected to the server with given id
// and attempts to connect to the master server
func NewClient(servers []string, id uint) (*PhatClient, error) {
	if client_log == nil {
		client_log = log.New(os.Stdout, "CLIENT: ", log.Ltime|log.Lmicroseconds)
	}

	c := new(PhatClient)

	// We need to register the DataNode and StatNode before we can use them in gob
	gob.Register(phatdb.DataNode{})
	gob.Register(phatdb.StatNode{})
	
	c.ServerLocations = servers
	c.NumServers = uint(len(servers))
	c.Id = id
	c.Cache = make(map[Handle]*CacheInfo)
	c.PathInfo = make(map[Handle]string)
	c.Timeout = DefaultTimeout

	err := c.connectToServer(id)
	if err != nil {
		c.Debug("NewClient failed to connect client to server with id %d, error %s", id, err.Error())
		return nil, err
	}

	err = c.connectToMaster()
	if err != nil {
		c.Debug("NewClient failed to connect client to the master server, error %s", err.Error())
		return c, err
	}

	return c, nil
}

func (c *PhatClient) Debug(format string, args ...interface{}) {
	client_log.Printf(format, args...)
}

// KeepAlive pings the server, erroring if no server response within a given time interval
// Client also invalidates cache based on server response 
func (c *PhatClient) KeepAlive() error {
	timer := time.NewTimer(DefaultTimeout)

	reply := &KeepAliveReply{}

	for {
		dbCall := c.RpcClient.Go("Server.KeepAlive", Null{}, reply, nil)
		select {
		case <-timer.C:
			c.Debug("KeepAlive Timed out on client side %v", dbCall.Error)
			return errors.New("KeepAlive Timed Out on Client Side")
		case <-dbCall.Done:
			if dbCall.Error == nil {
				timer.Reset(DefaultTimeout)
				c.Debug("KeepAlive done with no error")
				c.InvalidateCache(reply.Invalidations)
			} else {
				c.Debug("Call somehow failed with error %v, so emptying cache", dbCall.Error)
				c.Cache = make(map[Handle]*CacheInfo)
				time.Sleep(DefaultTimeout/10)
				//error possibilities 1) network failure 2) server can't process request
				c.connectToMaster()
			}
		}
	}
}

func (c *PhatClient) InvalidateCache(handles []Handle) {
	for _,h := range handles {
		delete(c.Cache, h)
	}
}

// connectToAnyServer connects client to server with given index
func (c *PhatClient) connectToServer(index uint) error {
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
	c.Debug("Connecting to the master...%d", c.MasterId)

	//connect to any server, and get the master id
	for i := uint(0); i < c.NumServers; i = i + 1 {
		err := c.RpcClient.Call("Server.GetMaster", new(Null), &c.MasterId)
		if err == nil {
			c.Debug("The master is %d", c.MasterId)
			break
		} else {
			c.Debug("Errored when calling get master %v", err)
		}
		//if problem with RPC or server is in recovery, need to connect to different server
		c.connectToServer((c.Id + uint(i + 1)) % c.NumServers)
	}

	// If the currently connected server isn't the master, connect to master
	if c.MasterId != c.Id {
		c.Debug("Called Server.GetMaster, current master id is %d, my id is %d",
			c.MasterId, c.Id)
		client, err := rpc.Dial("tcp", c.ServerLocations[c.MasterId])
		if err != nil {
			return err
		}
		c.RpcClient.Close()
		c.RpcClient = client
		c.Id = c.MasterId
		c.Debug("Now current master id is %d, my id is %d\n", c.MasterId, c.Id)
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
			if dbCall.Error == nil {
				c.Debug("Call done with no error")
				replyErr = StringToError(reply.Error)
				if replyErr != nil {
					return nil, replyErr
				}
				return reply, replyErr
			}
			c.Debug("Call somehow failed with error %v", dbCall.Error)
			time.Sleep(DefaultTimeout/10)
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

func (c *PhatClient) updateCacheEntry(h Handle, response *phatdb.DBResponse) error {
	if !AllowCaching {
		return nil
	}

	if _,ok := c.Cache[h]; !ok {
		c.Cache[h] = &CacheInfo{}
	}
	
	c.Cache[h].LastInvalidated = time.Now()
	c.Cache[h].Invalidated = false
	c.Cache[h].Response = response

	return nil
}

func (c *PhatClient) createNewHandle() Handle {
	lastHandle := c.LargestHandle
	c.LargestHandle += 1
	return lastHandle
}

// validateCacheEntry checks if there is an entry in the cache with the given handle
// and if that cache entry hasn't been invalidated.
func (c *PhatClient) validateCacheEntry(h Handle) (bool) {
	entry, ok := c.Cache[h]
	if ok {
		if entry.Invalidated {
			return false
		}
		if time.Since(entry.LastInvalidated) > CacheTimeout {
			return false
		}
		c.Debug("Valid cache entry for handle with path %v\n", c.PathInfo[h])
		return true
	}
	return false
}

func (c *PhatClient) Getroot() (Handle, error) {
	subpath := ""
	args := &phatdb.DBCommand{"GET", subpath, ""}
	reply, err := c.processCallWithRetry(args)
	if err != nil {
		return -1, err
	}
	h := c.createNewHandle()
	c.PathInfo[h] = subpath
	c.updateCacheEntry(h, reply)
	return h, err
}

// TODO: Should open effectively perform Getcontents?
func (c *PhatClient) Open(subpath string) (Handle, error) {
	h := c.createNewHandle()
	c.PathInfo[h] = subpath
	return h, nil
}

// Close closes a handle
func (c *PhatClient) Close(h Handle) {
	delete(c.Cache, h)
	delete(c.PathInfo, h)
}

func (c *PhatClient) Mkfile(subpath string, initialdata string) (Handle, error) {
	args := &phatdb.DBCommand{"CREATE", subpath, initialdata}
	reply, err := c.processCallWithRetry(args)
	if err != nil {
		return -1, err
	}
	h := c.createNewHandle()
	c.PathInfo[h] = subpath
	c.updateCacheEntry(h, reply)
	return h, err
}

func (c *PhatClient) Getcontents(h Handle) (*phatdb.DataNode, error) {
	subpath, ok := c.PathInfo[h]
	if !ok {
		return nil, errors.New("Invalid handle")
	}

	if c.validateCacheEntry(h) {
		n := c.Cache[h].Response.Reply.(phatdb.DataNode)
		return &n, nil
	}

	args := &phatdb.DBCommand{"GET", subpath, ""}
	reply, err := c.processCallWithRetry(args)
	if err != nil {
		return nil, err
	}
	c.updateCacheEntry(h, reply)

	n := reply.Reply.(phatdb.DataNode)
	return &n, err
}

func (c *PhatClient) Putcontents(h Handle, data string) error {
	subpath, ok := c.PathInfo[h]
	if !ok {
		errors.New("Invalid handle")
	}

	args := &phatdb.DBCommand{"SET", subpath, data}
	reply, err := c.processCallWithRetry(args)
	if err != nil {
		return err
	}

	c.updateCacheEntry(h, reply)
	return nil
}

// returns a list of the children of a directory node
func (c *PhatClient) Readdir(h Handle) ([]string, error) {
	subpath, ok := c.PathInfo[h]
	if !ok {
		return nil, errors.New("Invalid handle")
	}

	args := &phatdb.DBCommand{"CHILDREN", subpath, ""}
	reply, err := c.processCallWithRetry(args)
	if err != nil {
		return nil, err
	}
	return reply.Reply.([]string), err
}

// Stat returns the metadata for a given node. No caching for now
func (c *PhatClient) Stat(h Handle) (*phatdb.StatNode, error) {
	subpath, ok := c.PathInfo[h]
	if !ok {
		return nil, errors.New("Invalid handle")
	}

	args := &phatdb.DBCommand{"STAT", subpath, ""}
	reply, err := c.processCallWithRetry(args)
	if err != nil {
		return nil, err
	}

//	c.updateCacheEntry(h, reply)
	n := reply.Reply.(phatdb.StatNode)
	return &n, err
}

func (c *PhatClient) Flock(h Handle, l string) (Sequencer, error) {
	subpath, ok := c.PathInfo[h]
	if !ok {
		return -1, errors.New("Invalid handle")
	}

	args := &phatdb.DBCommand{"LOCK", subpath, l}
	reply, err := c.processCallWithRetry(args)
	if err != nil {
		return -1, err
	}
	n := reply.Reply.(Sequencer)
	return n, err
}

func (c *PhatClient) Funlock(h Handle) (Sequencer, error) {
	subpath, ok := c.PathInfo[h]
	if !ok {
		return -1, errors.New("Invalid handle")
	}

	args := &phatdb.DBCommand{"UNLOCK", subpath, ""}
	reply, err := c.processCallWithRetry(args)
	if err != nil {
		return -1, err
	}
	n := reply.Reply.(Sequencer)
	return n, err
}

// Delete deletes a node if it doesn't have any children
func (c *PhatClient) Delete(h Handle) error {
	subpath, ok := c.PathInfo[h]
	if !ok {
		return errors.New("Invalid handle")
	}

	args := &phatdb.DBCommand{"DELETE", subpath, ""}
	_ , err := c.processCallWithRetry(args)
	if err != nil {
		return err
	}

	return err
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
	n := reply.Reply.(phatdb.StatNode)
	return &n, err
}
