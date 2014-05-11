/* Windowed latency */

package main

import (	
    "fmt"
    "flag"
    "log"
    "strings"
    "time"
    "github.com/mgentili/goPhat/worker"
)

type Times struct {
	startTime time.Time
	duration time.Duration
}

type WorkerRequests struct {
	RequestTimes []Times
	RequestChan chan int
	NumMessages int
	WindowSize int
	Worker *worker.Worker
}

var Requests WorkerRequests

func makeCall(requestNum int) {
	start := time.Now()
	Requests.Worker.Push("work")
	end := time.Since(start)
	Requests.RequestTimes[requestNum] = Times{start, end}
	Requests.RequestChan <- 1
}
//sends specified number of messages to server, with a designated window size
func RunTest() {
	defer close(Requests.RequestChan)

	sent := 0
	// make initial windowSize calls
	for i := 0 ; i < Requests.WindowSize ; i++ {
		go makeCall(sent)
		sent++
	}

	//every time there's a response on the channel, take it off and make a new async call
	received := 0
	for received < Requests.NumMessages {
		select {
		case <-Requests.RequestChan:
			received++
			log.Printf("Received response for message %d", received)
			go makeCall(sent)
			sent++
		}
	}
}

func main() {
    nM := flag.Int("num_messages", 10, "number of messages a client should send")
    wS := flag.Int("window_size", 1, "window size (# of outstanding messages)")
    s := flag.String("servers", "", "Location of server to connect to")
    id := flag.Uint("id", 0, "Id of server to connect to")
    uid := flag.String("uid", "c1", "Unique id of client")

    flag.Parse()

    Requests.NumMessages = *nM
    Requests.WindowSize = *wS
    Requests.RequestTimes = make([]Times, *nM)
    Requests.RequestChan = make(chan int, *wS)

    var err error
    Requests.Worker, err = worker.NewWorker(strings.Fields(*s), *id, *uid)
    if (err != nil) {
    	log.Printf("Failed to create worker with error %v", err)
    }
    RunTest()
    

    fmt.Printf("Num Messages: %d, Window Size: %d\n",
    	Requests.NumMessages, Requests.WindowSize)

    RunTest()
}