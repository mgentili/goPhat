package main

import (
	"flag"
	"fmt"
	"github.com/mgentili/goPhat/phatclient"
	"github.com/mgentili/goPhat/level_log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
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

type ClientState struct {
	client *phatclient.PhatClient
	NumCreateMessages int
	requestChan chan string
	createdData map[string]string
}

type TestMaster struct {
	ReplicaProcesses []*exec.Cmd
	Clients     []*ClientState
	NumAliveReplicas int
	NumReplicas int
	NumClients int
	Server_Locations []string
	RPC_Locations    []string
	ReplicaStatus []int
	log *level_log.Logger
	wg sync.WaitGroup
}

// StartNodes starts up n replica nodes and connects a number of client to all of them.
// Later on there should also be a super-client that can inject faults
func (t *TestMaster) Setup(nr int, nc int) {
	t.Server_Locations = make([]string, nr)
	t.RPC_Locations = make([]string, nr)
	t.ReplicaProcesses = make([]*exec.Cmd, nr)
	t.ReplicaStatus = make([]int, nr)
	t.NumReplicas = nr
	t.Clients = make([]*ClientState, nc)
	t.NumClients = nc
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

	t.StartClients()
}

func (t *TestMaster) StartClients() {
	t.wg.Add(t.NumClients)

	var err error
	for i := 0; i < t.NumClients; i++ {
		t.Clients[i] = new(ClientState)
		cli := t.Clients[i]
		cli.client, err = phatclient.NewClient(t.RPC_Locations, 1, fmt.Sprintf("c%d", i))
		if err != nil {
			t.DieClean("Unable to start client")
		}
		cli.requestChan = make(chan string, 20)
		cli.createdData = make(map[string]string)
		// each request sent to a specific client will be serialized
		go t.ProcessClientCalls(i)
	}

	time.Sleep(time.Second)
}

// ProcessClientCalls serializes requests for one client
func (t *TestMaster) ProcessClientCalls(client_num int) {

	defer t.wg.Done()

	for {
		cli := t.Clients[client_num]
		r := <-cli.requestChan
		switch {
		case r == "CREATE":
			loc := fmt.Sprintf("/%s_%d",cli.client.Uid,cli.NumCreateMessages)
			data := generateRandomString()
			_, err := cli.client.Create(loc, data)
			if err != nil {
				t.log.Printf(DEBUG, "Client call failed :-(")
				break
			}
			cli.NumCreateMessages+=1
			cli.createdData[loc] = data
		default:
			return	
		}
	}
}

// StartNode starts a replica connected to all the other replicas
func (t *TestMaster) StartNode(i int) error {
	cmd := exec.Command(START_NODE_FILE, "--index", strconv.Itoa(i),
		"--replica_config", strings.Join(t.Server_Locations, ","),
		"--rpc_config", strings.Join(t.RPC_Locations, ","))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Start() //does command in background
	if err != nil {
		t.DieClean("Starting node failed")
		return err
	}
	t.ReplicaStatus[i] = ALIVE
	t.ReplicaProcesses[i] = cmd

	return nil
}

func (t *TestMaster) CheckValidCommand(i int) bool {
	return (t.NumAliveReplicas-1)*2 > t.NumReplicas && t.ReplicaStatus[i] == ALIVE
}

// KillNode kills a given node (kills the corresponding process)
func (t *TestMaster) KillNode(i int) error {
	if !t.CheckValidCommand(i) {
		return nil
	}
	t.log.Printf(DEBUG, "Killing node %d\n", i)
	err := t.ReplicaProcesses[i].Process.Kill()
	if err != nil {
		t.DieClean(err)
	}
	t.NumAliveReplicas -= 1
	t.ReplicaStatus[i] = KILLED
	return err
}

// StopNode stops a given node (stops the corresponding process)
func (t *TestMaster) StopNode(i int) error {
	
	if !t.CheckValidCommand(i) {
		return nil
	}
	t.log.Printf(DEBUG, "Stopping node %d!", i)
	t.ReplicaProcesses[i].Process.Signal(syscall.SIGSTOP)
	t.NumAliveReplicas -= 1
	t.ReplicaStatus[i] = STOPPED
	return nil
}

// ResumeNode resumes a given node (must have been stopped before)
func (t *TestMaster) ResumeNode(i int) error {
	
	if t.ReplicaStatus[i] != STOPPED {
		return nil
	}
	t.log.Printf(DEBUG, "Resuming node %d!", i)
	t.ReplicaProcesses[i].Process.Signal(syscall.SIGCONT)
	t.NumAliveReplicas += 1
	t.ReplicaStatus[i] = ALIVE
	return nil
}

func (t *TestMaster) ProcessCall(s string) {
	command := strings.Split(s, " ")
	switch {
	case command[0] == "startnodes":
		numnodes, _ := strconv.Atoi(command[1])
		numclients, _ := strconv.Atoi(command[2])
		t.Setup(numnodes, numclients)
	case command[0] == "stopnode":
		node, _ := strconv.Atoi(command[1])
		t.StopNode(node)
	case command[0] == "resumenode":
		node, _ := strconv.Atoi(command[1])
		t.ResumeNode(node)
	case command[0] == "wait":
		seconds, _ := strconv.Atoi(command[1])
		time.Sleep(time.Duration(seconds) * time.Second)
	case command[0] == "createfile":
		t.Clients[0].requestChan<-"CREATE"
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
	t.log.Printf(DEBUG, "Verifying correctness")
	num_failures := 0
	for _, c := range t.Clients {
		for loc, data := range c.createdData {
			res, err := c.client.GetData(loc)
			if err != nil {
				t.log.Printf(DEBUG, "Get Data of %s failed", loc)
			}
			str := res.Value
			t.log.Printf(DEBUG, "Getting data for %s. Expected %s, got %s", loc, data, str)
			if data != str {
				t.log.Printf(DEBUG, "FAILED!")
				num_failures += 1
			}
		}
	}

	t.log.Printf(DEBUG, "Total number of failures: %d", num_failures)	
}


func main() {
	path := flag.String("path", "", "File path")
	testtype := flag.String("test", "none", "Test number to run")
	flag.Parse()
	t := new(TestMaster)

	// Kill any nodes created before leaving
	defer t.cleanup()

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
	case *testtype == "fr":
		t.testFastRecover()
	}

	t.closeChannelsAndWait()
	t.Verify()
}