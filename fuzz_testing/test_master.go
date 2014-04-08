package main

import (
	"bufio"
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

type TestMaster struct {
	ReplicaProcesses []*exec.Cmd
	MasterClient     *phatclient.PhatClient
	NumAliveReplicas int
	NumReplicas int
	NumSentMessages  int
	Server_Locations []string
	RPC_Locations    []string
	ReplicaStatus []int
	log *level_log.Logger
}

var cleanup func()

// Go doesn't have an atexit
// https://groups.google.com/d/msg/golang-nuts/qBQ0bK2zvQA/vmOu9uhkYH0J
func (t *TestMaster) DieClean(v ...interface{}) {
	cleanup()
	t.log.Fatal(DEBUG, v)
}

func (t *TestMaster) SetupLog() {
	levelsToLog := []int{DEBUG}
	t.log = level_log.NewLL(os.Stdout, "TM: ")
	t.log.SetLevelsToLog(levelsToLog)
}

// StartNodes starts up n replica nodes and connects a client to all of them.
// That client will be the one to send requests and testing functions (e.g. stop connnection)
func (t *TestMaster) Setup(n int) {
	t.Server_Locations = make([]string, n)
	t.RPC_Locations = make([]string, n)
	t.ReplicaProcesses = make([]*exec.Cmd, n)
	t.ReplicaStatus = make([]int, n)
	t.NumReplicas = n
	t.SetupLog()

	t.log.Printf(DEBUG, "Starting %d nodes\n", n)

	for i := 0; i < n; i++ {
		t.Server_Locations[i] = fmt.Sprintf("%s:%d", HOST, INIT_SERVER_PORT+i)
		t.RPC_Locations[i] = fmt.Sprintf("%s:%d", HOST, INIT_RPC_PORT+i)
	}

	for i := n - 1; i >= 0; i-- {
		t.StartNode(i)
	}

	time.Sleep(time.Second)
	t.NumAliveReplicas = n
	t.StartMasterClient()
}

func (t *TestMaster) StartMasterClient() {
	var err error
	t.MasterClient, err = phatclient.NewClient(t.RPC_Locations, 1, "c1")
	if err != nil {
		t.DieClean("Unable to start master client")
	}

	time.Sleep(time.Second)
	//creates a database entry so that we can SetData on that entry without erroring
	t.SendCreateMessage()
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

func (t *TestMaster) SendCreateMessage() {
	_, err := t.MasterClient.Create("/dev/null", "-1")
	t.log.Printf(DEBUG, "Send create message succeeded!")
	if err != nil {
		t.DieClean(err)
	}
}

func (t *TestMaster) SendSetDataMessage() {
	t.MasterClient.SetData("/dev/null", strconv.Itoa(t.NumSentMessages))
	t.NumSentMessages += 1
	t.log.Printf(DEBUG, "Send set message succeeded!")
}

func (t *TestMaster) SendGetDataMessage() {
	response, err := t.MasterClient.GetData("/dev/null")
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
		t.Setup(numnodes)
	case command[0] == "stopnode":
		node, _ := strconv.Atoi(command[1])
		t.StopNode(node)
	case command[0] == "resumenode":
		node, _ := strconv.Atoi(command[1])
		t.ResumeNode(node)
	case command[0] == "wait":
		seconds, _ := strconv.Atoi(command[1])
		time.Sleep(time.Duration(seconds) * time.Second)
	case command[0] == "putfile":
		t.SendSetDataMessage()
	case command[0] == "getfile":
		t.SendGetDataMessage()
	}
}

// readLines reads a whole file into memory
// and returns a slice of its lines (assuming one int per line).
func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
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
	cleanup = func() {
		t.log.Printf(DEBUG, "Killing nodes in cleanup...\n")
		for i, proc := range t.ReplicaProcesses {
			t.log.Printf(DEBUG, "Killing node %d", i)
			err := proc.Process.Kill()
			if err != nil {
				t.log.Printf(DEBUG, "Failed to kill %d", i)
			}
		}
	}
	defer cleanup()

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
}

func (t *TestMaster) testReplicaFailure() {
	t.Setup(3)
	t.SendSetDataMessage()
	time.Sleep(time.Second)
	t.StopNode(2)
	t.SendSetDataMessage()
	t.ResumeNode(2)
	t.SendGetDataMessage()
}

func (t *TestMaster) testMasterFailure() {
	t.Setup(3)
	t.SendSetDataMessage()
	time.Sleep(time.Second)
	t.StopNode(0)
	t.SendSetDataMessage()
	t.ResumeNode(0)
	time.Sleep(time.Second*2)
	t.SendGetDataMessage()
	time.Sleep(time.Second*2)
}

func (t *TestMaster) testTwoMasterFailure() {
	t.Setup(5)
	t.SendSetDataMessage()
	time.Sleep(time.Second)
	t.StopNode(0)
	t.SendSetDataMessage()
	t.StopNode(1)
	t.SendSetDataMessage()
	t.ResumeNode(0)
	t.ResumeNode(1)
	time.Sleep(time.Second*2)
	t.SendGetDataMessage()
	time.Sleep(time.Second*2)
}

func (t *TestMaster) testLotsofReplicas() {
	t.Setup(5)
	t.SendSetDataMessage()
	t.SendGetDataMessage()
	time.Sleep(time.Second*4)
}