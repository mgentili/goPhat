package vr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"time"
)

const (
	F         = 1
	NREPLICAS = 2 * F // doesn't count the master as a replica
)

var clients [NREPLICAS]*rpc.Client

var rstate ReplicaState
var mstate MasterState

// a replica's possible states
const (
	Normal = iota
	Recovery
	ViewChange
)

type Command string

var phatlog []string

type ReplicaState struct {
	View          uint
	OpNumber      uint
	CommitNumber  uint
	ReplicaNumber uint
	Status        int
	NormalView    uint
	// list of replica addresses, in sorted order
	Config []string
}

type MasterState struct {
	A int
	// bit vector of what replicas have replied
	Replies        uint64
	ViewChangeMsgs uint
}

type PrepareArgs struct {
	View         uint
	Command      interface{}
	OpNumber     uint
	CommitNumber uint
}

type PrepareReply struct {
	View          uint
	OpNumber      uint
	ReplicaNumber uint
}

type CommitArgs struct {
	View         uint
	CommitNumber uint
}

func assert(b bool) {
	if !b {
		log.Fatal("assertion failed")
	}
}

func wrongView() error {
	return errors.New("view numbers don't match")
}

func addLog(command interface{}) {
	phatlog = append(phatlog, command.(string))
	log.Printf("replica %d logging\n", rstate.ReplicaNumber)
	//    phatlog.add(command)
}

func doCommit(cn uint) {
	if cn <= rstate.CommitNumber {
		log.Printf("Ignoring commit %d, already commited up to %d", cn, rstate.CommitNumber)
		return
	} else if cn > rstate.OpNumber {
		log.Printf("replica %d needs to do state transfer. only at op %d in log but got commit for %d\n", rstate.ReplicaNumber, rstate.OpNumber, cn)
		return
	} else if cn > rstate.CommitNumber+1 {
		log.Printf("replica %d needs to do extra commits", rstate.ReplicaNumber)
		// we're behind (but have all the log entries, so don't need to state
		// transfer), so catch up by committing up to the current commit
		for i := rstate.CommitNumber + 1; i < cn; i++ {
			doCommit(i)
		}
	}
	//    rstate.Commit()
	rstate.CommitNumber++
}

// RPCs
type Replica struct{}

func (t *Replica) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	log.Printf("Replica %d got prepare %d\n", rstate.ReplicaNumber, args.OpNumber)

	if rstate.Status != Normal {
		// TODO: ideally we should just not respond or something in this case
		return errors.New("not in normal mode")
	}

	if args.View != rstate.View {
		return wrongView()
	}

	if args.OpNumber != rstate.OpNumber+1 {
		// TODO: we should probably sleep or something til this is true??
		return fmt.Errorf("op numbers out of sync: got %d expected %d", args.OpNumber, rstate.OpNumber+1)
	}

	rstate.OpNumber++
	addLog(args.Command)

	reply.View = rstate.View
	reply.OpNumber = rstate.OpNumber
	reply.ReplicaNumber = rstate.ReplicaNumber

	return nil
}

func (t *Replica) Commit(args *CommitArgs, reply *int) error {
	if args.View != rstate.View {
		return wrongView()
	}

	doCommit(args.CommitNumber)

	return nil
}

func (rstate *ReplicaState) IsMaster() bool {
	return rstate.View%(NREPLICAS+1) == rstate.ReplicaNumber
}

func (mstate *MasterState) Reset() {
	mstate.A = 0
	mstate.Replies = 0
}

func RunAsReplica(i uint, config []string) {
	rstate.ReplicaNumber = i
	rstate.Config = config

	ln := ReplicaInit()

	go ReplicaRun(ln)
	if rstate.IsMaster() {
		MasterInit()
		for {
			RunVR("foo")
			RunVR("bar")
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func MasterInit() {
	j := 0
	var i uint = 0
	for ; i < NREPLICAS+1; i++ {
		if i == rstate.ReplicaNumber {
			continue
		}
		c, err := rpc.Dial("tcp", rstate.Config[i])
		if err != nil {
			// couldn't connect to some replica, try to just keep going
			// (could probably handle this better...)
			fmt.Println(err)
		}
		clients[j] = c
		j++
	}
}

func ReplicaInit() net.Listener {
	rpc.Register(new(Replica))
	ln, err := net.Listen("tcp", rstate.Config[rstate.ReplicaNumber])
	if err != nil {
		fmt.Println(err)
		return nil
	}
	return ln
}

// TODO: might not need to do this, e.g. if we handle client and server rpcs all on the same port
func ReplicaRun(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			continue
		}
		rpc.ServeConn(c)
	}
}

func RunVR(command interface{}) {
	assert(rstate.IsMaster() /*&& holdLease()*/)

	// FIXME: right now we enforce that the last operation has been committed before starting a new one
	assert(rstate.OpNumber == rstate.CommitNumber)

	mstate.Reset()

	rstate.OpNumber++
	addLog(command)

	args := PrepareArgs{rstate.View, command, rstate.OpNumber, rstate.CommitNumber}
	replyConstructor := func() interface{} { return new(PrepareReply) }
	sendAndRecv(NREPLICAS, "Replica.Prepare", args, replyConstructor, func(reply interface{}) bool {
		return handlePrepareOK(reply.(*PrepareReply))
	})
}

func handlePrepareOK(reply *PrepareReply) bool {
	log.Printf("got response: %+v\n", reply)
	if reply.View != rstate.View {
		return false
	}

	if reply.OpNumber != rstate.OpNumber {
		return false
	}

	if ((1 << reply.ReplicaNumber) & mstate.Replies) != 0 {
		return false
	}

	log.Printf("got suitable response\n")

	mstate.Replies |= 1 << reply.ReplicaNumber
	mstate.A++

	log.Printf("new master state: %v\n", mstate)

	// we've implicitly gotten a response from ourself already
	if mstate.A != F {
		return mstate.A >= F
	}

	// we've now gotten a majority
	doCommit(rstate.CommitNumber + 1)

	args := CommitArgs{rstate.View, rstate.CommitNumber}
	// TODO: technically only need to do this when we don't get another request from the client for a while
	go sendAndRecv(NREPLICAS, "Replica.Commit", args,
		func() interface{} { return nil },
		func(r interface{}) bool { return false })

	return true
}

func sendAndRecv(N int, msg string, args interface{}, newReply func() interface{}, handler func(reply interface{}) bool) {

	type ClientCall struct {
		call *rpc.Call
		// need to track which client so we can resend as needed
		client *rpc.Client
	}

	callChan := make(chan ClientCall)

	sendOne := func(client *rpc.Client) {
		call := client.Go(msg, args, newReply(), nil)
		go func(c ClientCall) {
			// wait for this call to complete
			<-c.call.Done
			// and now send it to the master channel
			callChan <- c
		}(ClientCall{call, client})
	}

	// send requests to the replicas
	for i := 0; i < N; i++ {
		sendOne(clients[i])
	}

	doneChan := make(chan int)

	go func() {
		callHandler := true
		// and now get the responses and retry if necessary
		for i := 0; i < N; {
			clientCall := <-callChan
			call := clientCall.call
			if call.Error != nil {
				// for now just resend failed messages indefinitely
				log.Printf("sendAndRecv message error: %v", call.Error)
				sendOne(clientCall.client)
				continue
			}
			if callHandler && handler(call.Reply) {
				// signals doneChan so that sendAndRecv can exit
				// (and the master can continue to the next request)
				// we still continue and resend messages as neccesary, however
				doneChan <- 0
				callHandler = false
			}

			i++
		}
	}()

	<-doneChan
}
