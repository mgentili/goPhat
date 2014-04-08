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
}

func (t *TestMaster) cleanup() {
	t.log.Printf(DEBUG, "Killing nodes in cleanup...\n")
	for i, proc := range t.ReplicaProcesses {
		t.log.Printf(DEBUG, "Killing node %d", i)
		err := proc.Process.Kill()
		if err != nil {
			t.log.Printf(DEBUG, "Failed to kill %d", i)
		}
	}
}

// Go doesn't have an atexit
// https://groups.google.com/d/msg/golang-nuts/qBQ0bK2zvQA/vmOu9uhkYH0J
func (t *TestMaster) DieClean(v ...interface{}) {
	t.cleanup()
	t.log.Fatal(DEBUG, v)
}

func (t *TestMaster) SetupLog() {
	levelsToLog := []int{DEBUG}
	t.log = level_log.NewLL(os.Stdout, "TM: ")
	t.log.SetLevelsToLog(levelsToLog)
}

// StartNodes starts up n replica nodes and connects a client to all of them.
// That client will be the one to send requests and testing functions (e.g. stop connnection)
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
	var err error
	for i := 0; i < t.NumClients; i++ {
		t.Clients[i] = new(ClientState)
		cli := t.Clients[i]
		cli.client, err = phatclient.NewClient(t.RPC_Locations, 1, fmt.Sprintf("c%d", i))
		if err != nil {
			t.DieClean("Unable to start client")
		}
		cli.requestChan = make(chan string, 20)
		// each request sent to a specific client will be serialized
		go t.ProcessClientCalls(i)
	}

	time.Sleep(time.Second)
}

func (t *TestMaster) ProcessClientCalls(client_num int) {
	for {
		cli := t.Clients[client_num]
		r := <-cli.requestChan
		switch {
		case r == "CREATE":
			_, err := cli.client.Create(fmt.Sprintf("/%s%d",cli.client.Uid,cli.NumCreateMessages), cli.client.Uid)
			cli.NumCreateMessages+=1
			if err != nil {
				t.log.Printf(DEBUG, "Client call failed :-(")
			}
			
		}
	}
}

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

func (t *TestMaster) SendGetDataMessage() {
	response, err := t.Clients[0].client.GetData("/dev/null")
	if err != nil {
		t.DieClean(err)
	}
	t.log.Printf(DEBUG, "Get Data Response: %d", response.Value)
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
		t.log.Printf(DEBUG,"Creating a file")
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

func main() {
	path := flag.String("path", "", "File path")
	testtype := flag.Int("test", -1, "Test number to run")
	flag.Parse()
	t := new(TestMaster)

	// Kill any nodes created before leaving
	defer t.cleanup()

	switch {
	case *testtype == -1:
		t.runFile(*path)
	case *testtype == 1:
		t.testReplicaFailure()
	case *testtype == 2:
		t.testMasterFailure()
	case *testtype == 3:
		t.testTwoMasterFailure()
	case *testtype == 4:
		t.testLotsofReplicas()
	}

	time.Sleep(3)
}