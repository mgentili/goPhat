package main

import (
	"flag"
	"fmt"
	"github.com/mgentili/goPhat/phatclient"
	"github.com/mgentili/goPhat/phatdb"
	"github.com/mgentili/goPhat/level_log"
	"github.com/mgentili/goPhat/phatRPC"
	"github.com/mgentili/goPhat/vr"
	"encoding/gob"
	"strconv"
	"strings"
	"time"
	"sync"
)

const (
	HOST             = "127.0.0.1"
	INIT_SERVER_PORT = 9000
	INIT_RPC_PORT    = 6000
	START_NODE_FILE  = "fuzz_testing_exec"
	ALIVE = 0
	KILLED = 1
	STOPPED = 2
	DEBUG = 0
)

type TestMaster struct {
	Replicas []*vr.Replica
	NumAliveReplicas int
	NumReplicas int
	Server_Locations []string
	RPC_Locations    []string
	ReplicaStatus []int
	log *level_log.Logger

	NumCalls int
	NumSuccessfulCalls int
	CreatedData map[string]string
	ClientLock sync.Mutex
	wg sync.WaitGroup
}

// StartNodes starts up n replica nodes
// Later on there should also be a super-client that can inject faults
func (t *TestMaster) Setup(nr int) {
	t.Server_Locations = make([]string, nr)
	t.RPC_Locations = make([]string, nr)
	t.Replicas = make([]*vr.Replica, nr)
	t.ReplicaStatus = make([]int, nr)
	t.NumReplicas = nr
	t.CreatedData = make(map[string]string)
	t.SetupLog()

	t.Debug(DEBUG, "Starting %d nodes\n", nr)

	for i := 0; i < nr; i++ {
		t.Server_Locations[i] = fmt.Sprintf("%s:%d", HOST, INIT_SERVER_PORT+i)
		t.RPC_Locations[i] = fmt.Sprintf("%s:%d", HOST, INIT_RPC_PORT+i)
	}

	for i := nr - 1; i >= 0; i-- {
		t.StartNode(i)
	}

	t.NumAliveReplicas = nr
}

// StartClient finds the first alive replica starting from i and connects a client to it
func (t *TestMaster) StartClient(i int, uid string) *phatclient.PhatClient {
	currNode := i
	for {
		if t.ReplicaStatus[currNode] == ALIVE {
			cli, err := phatclient.NewClient(t.RPC_Locations, uint(currNode), fmt.Sprintf("c%s", uid))
			if err != nil {
				t.Debug(DEBUG, "Unable connect client with request %d to replica %d\n", uid, currNode)
			}
			return cli
		}
		currNode = (currNode + 1) % t.NumReplicas
		if currNode == i {
			t.DieClean("Client unable to connect to any replica")
		}
	}
}

// ProcessCreate creates a new client and sends a create request
func (t *TestMaster) ProcessCreate(uid string) {
	loc := fmt.Sprintf("/c_%s", uid )
	data := generateRandomString()

	cli := t.StartClient(0, uid)
	_ , err := cli.Create(loc, data)
	if err != nil {
		t.Debug(DEBUG, "Client call failed with error: %v", err)
	} else {
		t.ClientLock.Lock()
		t.NumSuccessfulCalls += 1
		t.CreatedData[loc] = data
		t.ClientLock.Unlock()
	}
}

// StartNode starts a replica connected to all the other replicas
func (t *TestMaster) StartNode(i int) error {

	r := vr.RunAsReplica(uint(i), t.Server_Locations)
	phatRPC.StartServer(t.RPC_Locations[i], r)
	
	t.ReplicaStatus[i] = ALIVE
	t.Replicas[i] = r

	return nil
}

// SufficientReplicas returns true if there will be more than a quorum of replicas alive
// after killing one
func (t *TestMaster) SufficientReplicas() bool {
	return (t.NumAliveReplicas-1)*2 > t.NumReplicas
}


// KillNode kills the first node that is alive starting from index i
func (t *TestMaster) KillNode(i int) {
	currNode := i
	for {
		if !t.SufficientReplicas() {
			return
		}
		if t.ReplicaStatus[currNode] == ALIVE {
			t.Replicas[currNode].Shutdown()
			t.NumAliveReplicas -= 1
			t.ReplicaStatus[currNode] = KILLED
		}
		currNode = (currNode + 1) % t.NumReplicas
	}
	return
}

// StopNode stops the first node that is alive starting from index i
func (t *TestMaster) StopNode(i int) {
	currNode := i
	for {
		if !t.SufficientReplicas() {
			return
		}
		if t.ReplicaStatus[currNode] == ALIVE {
			t.Debug(DEBUG, "Stopping node %d\n", currNode)
			t.Replicas[currNode].Disconnect()
			t.NumAliveReplicas -= 1
			t.ReplicaStatus[currNode] = STOPPED
			return
		}
		currNode = (currNode + 1) % t.NumReplicas
	}
}

// ResumeNode resumes the first node that is stopped starting from index i
func (t *TestMaster) ResumeNode(i int) {
	currNode := i
	for {
		if t.ReplicaStatus[currNode] == STOPPED {
			t.Debug(DEBUG, "Resuming node %d\n", currNode)
			t.Replicas[currNode].Reconnect()
			t.NumAliveReplicas += 1
			t.ReplicaStatus[currNode] = ALIVE
			return
		}
		currNode = (currNode + 1) % t.NumReplicas
		if currNode == i { // no nodes were stopped, so can't resume any
			return
		}
	}
}

func (t *TestMaster) ProcessCall(s string) {
	command := strings.Split(s, " ")
	switch {
	case command[0] == "startnodes":
		numnodes, _ := strconv.Atoi(command[1])
		t.Setup(numnodes)
	case command[0] == "stopnode":
		node, _ := strconv.Atoi(command[1])
		t.StopNode(node)
	case command[0] == "resumenode":
		node, _ := strconv.Atoi(command[1])
		t.ResumeNode(node)
	case command[0] == "wait":
		ms, _ := strconv.Atoi(command[1])
		time.Sleep(time.Duration(ms) * time.Millisecond)
	case command[0] == "createfile":
		t.wg.Add(1)
		uid := t.NumCalls
		go func() {
			t.ProcessCreate(strconv.Itoa(uid))
			t.wg.Done()
		}()
		t.NumCalls += 1
	}
}

func (t *TestMaster) runFile(path string) {
	values, err := readLines(path)
	if err != nil {
		t.DieClean(err)
	}

	for _, v := range values {
		t.ProcessCall(v)
	}
}

// Verify checks that the replica state (database) agrees with the local database
// TODO: Make local database, and when client calls succeed, add to local database.
func (t *TestMaster) VerifyDB() {
	t.Debug(DEBUG, "Verifying correctness. Data created %v", t.CreatedData)
	num_failures := 0

	cli := t.StartClient(0, "tester")
	for loc, data := range t.CreatedData {
		res, err := cli.GetData(loc)
		if err != nil {
			t.Debug(DEBUG, "Get Data of %s failed with %s", loc, err)
		}
		str := res.Value
		t.Debug(DEBUG, "Getting data for %s. Expected %s, got %s", loc, data, str)
		if data != str {
			t.Debug(DEBUG, "FAILED!")
			num_failures++
		}
	}

	t.Debug(DEBUG, "Total number of failures: %d", num_failures)	
}



func (t *TestMaster) VerifyLog() {

	//returns just the DBCommand stored in the log, which includes the Command (e.g. CREATE),
	// the Path (e.g. "\dev\"), and the Value (e.g. "hello world!")
	mapfunc := func(c interface{}) interface{} {
		return c.(vr.VRCommand).C.(phatRPC.CommandFunctor).Command.Cmd
	}

	var hash string
	num_failures := 0

	for i, r := range t.Replicas {
		ops, commands := r.Phatlog.Map(mapfunc)
		h := t.Hash(commands)
		t.Debug(DEBUG, "R:%d, Log: %v, %s", i, ops, commands)
		t.Debug(DEBUG, "Hash: %s", t.Hash(commands))
		if i == 0 {
			hash = h
		} else {
			if hash != h {
				t.Debug(DEBUG, "Hash of log for replica %d disagrees! %s", i, h)
				num_failures++
			}
		}
	}
	t.Debug(DEBUG, "Number of log disagreements: %d", num_failures)

}

func (t *TestMaster) ResumeAll() {
	for currNode, _ := range t.Replicas {
		if t.ReplicaStatus[currNode] == STOPPED {
			t.Debug(DEBUG, "Resuming node %d in ResumeAll\n", currNode)
			t.Replicas[currNode].Reconnect()
			t.NumAliveReplicas += 1
			t.ReplicaStatus[currNode] = ALIVE
		}
	}
}

func main() {
	path := flag.String("path", "", "File path")
	testtype := flag.String("test", "none", "Test number to run")
	flag.Parse()
	t := new(TestMaster)

	switch {
	case *testtype == "none":
		if *path != "" {
			t.runFile(*path)
		}
	case *testtype == "1r":
		t.testReplicaFailure()
	case *testtype == "1m":
		t.testMasterFailure()
	case *testtype == "2m_o":
		t.testTwoMasterFailure()
	case *testtype == "2m_s":
		t.testDoubleMasterFailure()
	case *testtype == "cmf":
		t.testCascadingMasterFailure()
	}
	t.wg.Wait()
	t.Debug(DEBUG, "All clients finished, for better or worse")
	t.ResumeAll()
	time.Sleep(10*time.Second)

	// for hashing the log
	gob.Register(phatdb.DBCommand{})

	t.VerifyLog()
	//t.VerifyDB()

	
}
