package main

import (
	"bufio"
	"github.com/mgentili/goPhat/level_log"
	"math/rand"
	"os"
	"strconv"
)

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

func generateRandomString() string {
	return strconv.Itoa(rand.Int())
}

func (t *TestMaster) SetupLog() {
	levelsToLog := []int{DEBUG}
	t.log = level_log.NewLL(os.Stdout, "TM: ")
	t.log.SetLevelsToLog(levelsToLog)
}

// cleanup kills all of the replica processes before exiting
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

// closeChannelsAndWait closes all of the client request channels
// and waits until all clients have finished their calls
func (t *TestMaster) closeChannelsAndWait() {
	for i:=0; i < t.NumClients; i++ {
		close(t.Clients[i].requestChan)
	}

	t.wg.Wait()
 }

// Go doesn't have an atexit
// https://groups.google.com/d/msg/golang-nuts/qBQ0bK2zvQA/vmOu9uhkYH0J
func (t *TestMaster) DieClean(v ...interface{}) {
	t.cleanup()
	t.log.Fatal(DEBUG, v)
}
