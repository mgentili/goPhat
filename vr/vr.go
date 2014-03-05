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
	F           = 1
	NREPLICAS   = 2 * F // doesn't count the master as a replica
	LEASE       = 5000 * time.Millisecond
	MAX_RENEWAL = LEASE / 2
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
    ViewChangeMsgs uint
	Timer         *time.Timer
	// list of replica addresses, in sorted order
	Config []string
}

type MasterState struct {
	A int
	// bit vector of what replicas have replied
	Replies uint64

	ViewChangeMsgs uint

	Timer            *time.Timer
	Heartbeats       int
	HeartbeatReplies uint64
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

	if args.View != rstate.View {
		return wrongView()
	}

	rstate.ExtendLease() // do we extend lease in non-normal mode??

	if rstate.Status != Normal {
		// TODO: ideally we should just not respond or something in this case
		return errors.New("not in normal mode")
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

func (t *Replica) Commit(args *CommitArgs, reply *uint) error {
	if args.View != rstate.View {
		return wrongView()
	}

	rstate.ExtendLease()

	*reply = rstate.ReplicaNumber

	doCommit(args.CommitNumber)

	return nil
}

func (rstate *ReplicaState) IsMaster() bool {
	return rstate.View%(NREPLICAS+1) == rstate.ReplicaNumber
}

func (mstate *MasterState) Reset() {
	mstate.A = 0
	mstate.Replies = 0
    mstate.ViewChangeMsgs = 0
}

func (m *MasterState) ExtendNeedsRenewal() {
	m.Timer.Reset(MAX_RENEWAL)
}

func (m *MasterState) Heartbeat(replica uint) {
	if ((1 << replica) & mstate.HeartbeatReplies) != 0 {
		return
	}

	m.HeartbeatReplies |= 1 << replica
	m.Heartbeats++

	// we've gotten a majority to renew our lease!
	if m.Heartbeats == F {
		m.Heartbeats = 0
		m.HeartbeatReplies = 0
		// don't need to try renewing for a while
		m.ExtendNeedsRenewal()
		// update our own lease
		rstate.ExtendLease()
	}
}

func (r *ReplicaState) ExtendLease() {
	r.Timer.Reset(LEASE)
}

func ReplicaTimeout() {
	if rstate.IsMaster() {
		log.Printf("we couldn't stay master :(\n")
		// can't handle read requests anymore
	}
	log.Printf("replica %d timed out, trying view change", rstate.ReplicaNumber)
	PrepareViewChange()
	// start counting again so we timeout if the new replica can't become master
	rstate.ExtendLease()
}

func MasterNeedsRenewal() {
	// TODO: master's been inactive for MAX_RENEWAL time, so we need to
	// send explicit commits to renew lease
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

// HAX: need to setup so each replica has a connection setup with each other replica
// but have to wait til all replicas are up to do this. so for now just assume that's
// true after receiving the first rpc
// this really needs to be more robust, e.g. handling reconnections
var hasInterconnect bool = false

// sets up connections with all other replicas
func InterconnectionInit() {
	if hasInterconnect {
		return
	}
	hasInterconnect = true
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

func MasterInit() {
	InterconnectionInit()
	mstate.Timer = time.AfterFunc(MAX_RENEWAL, MasterNeedsRenewal)
}

func ReplicaInit() net.Listener {
	rpc.Register(new(Replica))
	ln, err := net.Listen("tcp", rstate.Config[rstate.ReplicaNumber])
	if err != nil {
		fmt.Println(err)
		return nil
	}
	rstate.Timer = time.AfterFunc(LEASE, ReplicaTimeout)
	return ln
}

// TODO: might not need to do this, e.g. if we handle client and server rpcs all on the same port
func ReplicaRun(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			continue
		}
		InterconnectionInit()

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

	mstate.Heartbeat(reply.ReplicaNumber)

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
		func() interface{} { return new(uint) },
		func(r interface{}) bool { mstate.Heartbeat(*(r.(*uint))); return false })

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
				//TEMP:
				if call.Error.Error() == "connection is shut down" {
					continue
				}
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
