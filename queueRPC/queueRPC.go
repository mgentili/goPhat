package queueRPC

import (
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/mgentili/goPhat/level_log"
	queue "github.com/mgentili/goPhat/phatqueue"
	"github.com/mgentili/goPhat/vr"
	"net"
	"net/rpc"
	"os"
)

const DEBUG = 0

var server_log *level_log.Logger
var paxos = true
type Server struct {
	ReplicaServer *vr.Replica
	InputChan     chan queue.QCommandWithChannel
	ClientTable   map[string]ClientTableEntry
}

type ClientTableEntry struct {
	SeqNumber uint
	Response  *queue.QResponse
}

type ClientCommand struct {
	Uid       string
	SeqNumber uint
	Command   *queue.QCommand
}

type Null struct{}

// wraps a DB command to conform to the vr.Command interface
type CommandFunctor struct {
	Command queue.QCommandWithChannel
}

func (c CommandFunctor) CommitFunc(context interface{}) {
	server := context.(*Server)
	argsWithChannel := c.Command
	// we make our own QCommandWithChannel so we (VR) can make sure the DB has committed before continuing on
	newArgsWithChannel := queue.QCommandWithChannel{argsWithChannel.Cmd, make(chan *queue.QResponse)}
	server.InputChan <- newArgsWithChannel
	// wait til the DB has actually committed the transaction
	result := <-newArgsWithChannel.Done
	// and pass the result along to the server-side RPC
	// (if we're not master .Done will be nil since channels aren't passed over RPC)
	if argsWithChannel.Done != nil {
		argsWithChannel.Done <- result
	}
}

func SnapshotFunc(context interface{}, SnapshotHandle func() uint) ([]byte, uint, error) {
	s := context.(*Server)
	command := &queue.QCommand{"SNAPSHOT", SnapshotHandle}

	argsWithChannel := queue.QCommandWithChannel{command, make(chan *queue.QResponse)}
	s.InputChan <- argsWithChannel

	result := <-argsWithChannel.Done
	if result.Error != "" {
		return nil, 0, errors.New(result.Error)
	}
	snapshot := result.Reply.(queue.QSnapshot)
	return snapshot.Data, snapshot.SnapshotIndex, nil
}

func (s *Server) debug(level int, format string, args ...interface{}) {
	//return
	str := fmt.Sprintf("%d: %s", s.ReplicaServer.Rstate.ReplicaNumber, format)
	server_log.Printf(level, str, args...)
}

// startDB starts the queue for the server
func (s *Server) startQueue() {
	input := make(chan queue.QCommandWithChannel, 1000)
	s.InputChan = input
	go queue.QueueServer(input)
}

func SetupLog() {
	if server_log == nil {
		levelsToLog := []int{DEBUG}
		server_log = level_log.NewLL(os.Stdout, "s")
		server_log.SetLevelsToLog(levelsToLog)
	}
}

// startServer starts a TCP server that accepts client requests at the given port
// and has information about the replica server
func StartServer(address string, replica *vr.Replica) (*rpc.Server, error) {
	SetupLog()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	serve := new(Server)
	serve.ReplicaServer = replica
	serve.ClientTable = make(map[string]ClientTableEntry)
	serve.startQueue()
	replica.Context = serve
	replica.SnapshotFunc = SnapshotFunc

	newServer := rpc.NewServer()
	err = newServer.Register(serve)
	if err != nil {
		return nil, err
	}

	// have to gob.Register this struct so we can pass it through RPC
	// as a generic interface{} (I don't understand the details that well,
	// see http://stackoverflow.com/questions/21934730/gob-type-not-registered-for-interface-mapstringinterface)
	gob.Register(CommandFunctor{})
	gob.Register(queue.QCommandWithChannel{})
	// Need to register all types that are returned within the QResponse
	gob.Register(queue.QMessage{})

	serve.debug(DEBUG, "Server at %s trying to accept new client connections\n", address)
	go newServer.Accept(listener)
	//log.Println("Accepted new connection?")
	return newServer, nil
}

// makes sure that replica is in appropriate state to respond to client request
func (s *Server) checkState() error {
	if s.ReplicaServer.Rstate.Status != vr.Normal {
		return errors.New("My state isn't normal")
	}

	MasterId := s.ReplicaServer.GetMasterId()
	Id := s.ReplicaServer.Rstate.ReplicaNumber
	s.debug(DEBUG, "Master id: %d, My id: %d", MasterId, Id)
	// Temporary workaround to allow responses to SHA256 on non-master nodes
	if Id != MasterId {
		s.debug(DEBUG, "I'm not the master!")
		return errors.New("Not master node")
	}

	return nil
}

// returns the master id, as long as replica is in a normal state
func (s *Server) GetMaster(args *Null, reply *uint) error {
	//if in recovery state, error
	if s.ReplicaServer.Rstate.Status != vr.Normal {
		return errors.New("Master Failover")
	}

	*reply = s.ReplicaServer.GetMasterId()
	return nil
}

func (s *Server) checkClientTable(args *ClientCommand) (*queue.QResponse, error) {
	if res, ok := s.ClientTable[args.Uid]; ok {
		if args.SeqNumber < res.SeqNumber || res.Response == nil {
			return nil, errors.New("Old Request")
		}
		if args.SeqNumber == res.SeqNumber {
			return res.Response, nil
		}
	}

	return nil, nil
}

func (s *Server) Send(args *ClientCommand, reply *queue.QResponse) error {
	// check to make sure that server receiving client RPC is the master
	// and is in Normal condition
	if err := s.checkState(); err != nil {
		return err
	}
	
	s.debug(DEBUG, "Received message with %v", args)
	
	/*
	// check to see if client has already sent this request before
	res, err := s.checkClientTable(args)
	if err != nil {
		return err
	}
	if res != nil {
		reply = res
		return nil
	}

	// place a new "in progress" (nil) entry in the client table
	s.ClientTable[args.Uid] = ClientTableEntry{args.SeqNumber, nil}
	*/

	argsWithChannel := queue.QCommandWithChannel{args.Command, make(chan *queue.QResponse, 1)}

	if paxos {
		s.ReplicaServer.RunVR(CommandFunctor{argsWithChannel})
	} else {
		s.InputChan <- argsWithChannel
	}
	result := <-argsWithChannel.Done
	*reply = *result
	// place the response entry into the client table
	// s.ClientTable[args.Uid] = ClientTableEntry{s.ClientTable[args.Uid].SeqNumber, reply}

	return nil
}
	