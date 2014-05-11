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
	StartTime time.Time
	Duration time.Duration
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
	log.Printf("In make call with requestNum %d", requestNum)
	start := time.Now()
	Requests.Worker.Push("work")
	end := time.Since(start)
	Requests.RequestTimes[requestNum] = Times{start, end}
	Requests.RequestChan <- requestNum
}
//sends specified number of messages to server, with a designated window size
func RunTest() {
	defer close(Requests.RequestChan)

	sent := 0
	// make initial windowSize calls
	for i := 0 ; i < Requests.WindowSize ; i++ {
		curr := sent
		go makeCall(curr)
		sent++
	}

	//every time there's a response on the channel, take it off and make a new async call
	received := 0
	for {
		select {
		case c := <-Requests.RequestChan:
			received++
			log.Printf("Received response for message %d", c)
			if (sent < Requests.NumMessages) {
				curr := sent
				go makeCall(curr)
				sent++	
			}
			if (received == Requests.NumMessages) {
				return
			}
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

    fmt.Printf("Num Messages: %d, Window Size: %d\n",
    	Requests.NumMessages, Requests.WindowSize)

    var err error
    Requests.Worker, err = worker.NewWorker(strings.Fields(*s), *id, *uid)
    if (err != nil) {
    	log.Printf("Failed to create worker with error %v", err)
    }
    RunTest()

    start := Requests.RequestTimes[0].StartTime
    for i, v := range(Requests.RequestTimes) {
    	log.Printf("Request %d: Started at: %v, Duration: %v", i, v.StartTime.Sub(start), v.Duration)
    }

}