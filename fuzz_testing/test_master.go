package main

import (
	"flag"
	"fmt"
	"github.com/mgentili/goPhat/phatclient"
	"github.com/mgentili/goPhat/level_log"
	"github.com/mgentili/goPhat/phatRPC"
	"github.com/mgentili/goPhat/vr"
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

	t.log.Printf(DEBUG, "Starting %d nodes\n", nr)

	for i := 0; i < nr; i++ {
		t.Server_Locations[i] = fmt.Sprintf("%s:%d", HOST, INIT_SERVER_PORT+i)
		t.RPC_Locations[i] = fmt.Sprintf("%s:%d", HOST, INIT_RPC_PORT+i)
	}

	for i := nr - 1; i >= 0; i-- {
		t.StartNode(i)
	}

	time.Sleep(time.Second)
	t.NumAliveReplicas = nr
}

// StartClient finds the first alive replica starting from i and connects a client to it
func (t *TestMaster) StartClient(i int, uid int) *phatclient.PhatClient {
	currNode := i
	for {
		if t.ReplicaStatus[currNode] == ALIVE {
			cli, err := phatclient.NewClient(t.RPC_Locations, uint(currNode), fmt.Sprintf("c%d", uid))
			if err != nil {
				t.log.Printf(DEBUG, "Unable connect client with request %d to replica %d\n", uid, currNode)
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
func (t *TestMaster) ProcessCreate(uid int) {
	loc := fmt.Sprintf("/c_%d", uid )
	data := generateRandomString()

	cli := t.StartClient(0, uid)
	_ , err := cli.Create(loc, data)
	if err != nil {
		t.log.Printf(DEBUG, "Client call failed :-(")
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
			t.log.Printf(DEBUG, "Stopping node %d\n", currNode)
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
			t.log.Printf(DEBUG, "Resuming node %d\n", currNode)
			t.Replicas[currNode].Reconnect()
			t.NumAliveReplicas += 1
			t.ReplicaStatus[currNode] = ALIVE
			return
		}
		currNode = (currNode + 1) % t.NumReplicas
		if currNode == i {
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
		go func() {
			t.ProcessCreate(t.NumCalls)
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
func (t *TestMaster) Verify() {
	t.log.Printf(DEBUG, "Verifying correctness. Data created %v", t.CreatedData)
	num_failures := 0

	cli := t.StartClient(0, 0)
	for loc, data := range t.CreatedData {
		res, err := cli.GetData(loc)
		if err != nil {
			t.log.Printf(DEBUG, "Get Data of %s failed with %s", loc, err)
		}
		str := res.Value
		t.log.Printf(DEBUG, "Getting data for %s. Expected %s, got %s", loc, data, str)
		if data != str {
			t.log.Printf(DEBUG, "FAILED!")
			num_failures += 1
		}
	}

	t.log.Printf(DEBUG, "Total number of failures: %d", num_failures)	
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
	time.Sleep(time.Second)
	for i, r := range t.Replicas {
		t.log.Printf(DEBUG, "R:%d, Log: %v", i, r.Phatlog)
	}
	//t.Verify()

	
}