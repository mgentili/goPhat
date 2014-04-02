package main

import (
	"os"
	"os/exec"
	"fmt"
	"strings"
	"log"
	"syscall"
	"time"
	"strconv"
	"flag"
	"bufio"
	"github.com/mgentili/goPhat/phatclient"
)

const (
	HOST = "127.0.0.1"
	INIT_SERVER_PORT = 9000
	INIT_RPC_PORT = 6000
	START_NODE_FILE = "fuzz_testing_exec"
)

type TestMaster struct {
	ReplicaProcesses []*exec.Cmd
	MasterClient *phatclient.PhatClient
	NumAliveReplicas int
	NumSentMessages int
	Server_Locations []string
	RPC_Locations []string
}

// StartNodes starts up n replica nodes and connects a client to all of them.
// That client will be the one to send requests and testing functions (e.g. stop connnection)
func (t *TestMaster) Setup(n int) {
	log.Printf("Starting %d nodes\n", n)
	t.Server_Locations = make([]string, n)
	t.RPC_Locations = make([]string, n)
	t.ReplicaProcesses = make([]*exec.Cmd, n)

	for i:=0; i < n; i++ {
		t.Server_Locations[i] = fmt.Sprintf("%s:%d", HOST, INIT_SERVER_PORT + i)
		t.RPC_Locations[i] = fmt.Sprintf("%s:%d", HOST, INIT_RPC_PORT + i)
	}

	for i:=n-1; i >= 0; i-- {
		t.StartNode(i)
	}

	time.Sleep(time.Second)
	t.NumAliveReplicas = n
	t.StartMasterClient()
}

func (t *TestMaster) StartMasterClient() {
	var err error
	t.MasterClient, err = phatclient.NewClient(t.RPC_Locations, 0)
	if err != nil {
		log.Fatal("Unable to start master client")
	}

	//creates a database entry so that we can SetData on that entry without erroring
	t.SendCreateMessage();
}

func (t *TestMaster) StartNode(i int) error {
	cmd := exec.Command(START_NODE_FILE, "--index", strconv.Itoa(i), 
										 "--replica_config", strings.Join(t.Server_Locations, ","),
										 "--rpc_config", strings.Join(t.RPC_Locations, ","))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	
	err := cmd.Start() //does command in background
	if err != nil {
		log.Fatal("Starting node failed")
		return err
	}

	t.ReplicaProcesses[i] = cmd

	return nil
}

// KillNode kills a given node (kills the corresponding process)
func (t *TestMaster) KillNode(i int) error {
	log.Printf("Killing node %d\n", i)
	err := t.ReplicaProcesses[i].Process.Kill()
	if err != nil {
		log.Fatal(err)
	}
	t.NumAliveReplicas-=1
	return err
}

// StopNode stops a given node (stops the corresponding process)
func (t *TestMaster) StopNode(i int) {
	t.ReplicaProcesses[i].Process.Signal(syscall.SIGSTOP)
}

// ResumeNode resumes a given node (must have been stopped before)
func (t *TestMaster) ResumeNode(i int) {
	t.ReplicaProcesses[i].Process.Signal(syscall.SIGCONT)
}

func (t *TestMaster) SendCreateMessage() {
	_ , err := t.MasterClient.Create("/dev/null", "-1")
	log.Printf("Send create message succeeded!")
	if err != nil {
		log.Fatal(err)
	}
}

func (t *TestMaster) SendSetDataMessage() {
	t.MasterClient.SetData("/dev/null", strconv.Itoa(t.NumSentMessages))
	t.NumSentMessages+=1
	log.Printf("Send set message succeeded!")
}

func (t *TestMaster) SendGetDataMessage() {
	response, err := t.MasterClient.GetData("/dev/null")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Get Data Response: %d", response.Value)
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
			time.Sleep(time.Duration(seconds)*time.Second)
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
  log.Printf("%v",lines)
  return lines, scanner.Err()
}

func main() {
	path := flag.String("path", "", "File path")
	flag.Parse()
	time.Sleep(time.Second)
	t := new(TestMaster)

	if *path == "" {
		t.Setup(3)
		t.SendSetDataMessage()
		t.SendSetDataMessage()
		time.Sleep(time.Second)
		t.StopNode(2)
		time.Sleep(2*time.Second)
		t.StopNode(0)
		time.Sleep(3*time.Second)
		t.KillNode(0)
	} else {
		values, err := readLines(*path)
		if err != nil {
			log.Fatal(err)
		}

		for _, v := range values {
			t.ProcessCall(v)
		}
	}
}