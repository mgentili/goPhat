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

type TestMaster struct {
	ReplicaProcesses []*exec.Cmd
	MasterClient *phatclient.PhatClient
	NumAliveReplicas int
	SentMessages int
}

// StartNodes starts up n replica nodes and connects a client to all of them.
// That client will be the one to send requests and testing functions (e.g. stop connnection)
func (t *TestMaster) StartNodes(n int) {
	log.Printf("Starting %d nodes\n", n)
	server_locations := make([]string, n)
	rpc_locations := make([]string, n)

	host := "127.0.0.1"
	init_server_port := 9000
	init_rpc_port := 6000

	for i:=0; i < n; i++ {
		server_locations[i] = fmt.Sprintf("%s:%d", host, init_server_port + i)
		rpc_locations[i] = fmt.Sprintf("%s:%d", host, init_rpc_port + i)
	}

	all_server_locations := strings.Join(server_locations, ",")
	all_rpc_locations := strings.Join(rpc_locations, ",")
	start_node_file := "fuzz_testing_exec"
	//start_node_file := "vr_exec"
	t.ReplicaProcesses = make([]*exec.Cmd, n)

	for i:=n-1; i >= 0; i-- {
		cmd := exec.Command(start_node_file, "--index", strconv.Itoa(i), "--replica_config", all_server_locations, "--rpc_config", all_rpc_locations)
		//cmd := exec.Command(start_node_file, "-r", strconv.Itoa(i))
		cmd.Stdout = os.Stdout
    	cmd.Stderr = os.Stderr
		
		err := cmd.Start() //does command in background
		if err != nil {
			log.Fatal(err)
		}

		t.ReplicaProcesses[i] = cmd
	}

	time.Sleep(time.Second)
	var err error
	t.MasterClient, err = phatclient.NewClient(rpc_locations, 0)
	if err != nil {
		log.Fatal("Unable to start master client")
	}

	//creates a database entry so that we can SetData on that entry without erroring
	t.SendCreateMessage();
	t.NumAliveReplicas = n

	//err := t.ReplicaProcesses[0].Wait()
	//log.Printf("Command finished with error: %v", err)

	
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
// and then wakes it up after a given amount of time
func (t *TestMaster) StopNode(i int) {
	go func() {
		t.ReplicaProcesses[i].Process.Signal(syscall.SIGSTOP)
		time.Sleep(5*time.Second)
		t.ReplicaProcesses[i].Process.Signal(syscall.SIGCONT)
	}()
}

func (t *TestMaster) SendCreateMessage() {
	t.MasterClient.Create("/dev/null", "0")
	log.Printf("Send create message succeeded!")
}

func (t*TestMaster) SendSetDataMessage() {
	t.SentMessages+=1
	t.MasterClient.SetData("/dev/null", strconv.Itoa(t.SentMessages))
	log.Printf("Send set message succeeded!")
}

func (t *TestMaster) ProcessCall(i int) {
	switch {
		case i < 10:
			t.KillNode(0)
		case i < 20:
			t.StopNode(0)
		case i < 30:
			t.SendSetDataMessage()
	}
}

// readLines reads a whole file into memory
// and returns a slice of its lines (assuming one int per line).
func readLines(path string) ([]int, error) {
  file, err := os.Open(path)
  if err != nil {
    return nil, err
  }
  defer file.Close()

  var lines []int
  scanner := bufio.NewScanner(file)
  for scanner.Scan() {
  	input, err := strconv.Atoi(scanner.Text())
  	if err != nil {
  		log.Fatal(err)
  	}
    lines = append(lines, input)
  }
  return lines, scanner.Err()
}

func main() {
	path := flag.String("path", "", "File path")
	flag.Parse()
	time.Sleep(time.Second)
	t := new(TestMaster)

	if *path == "" {
		t.StartNodes(3)
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

		t.StartNodes(values[0])

		for _, v := range values[1:] {
			t.ProcessCall(v)
		}
	}
}