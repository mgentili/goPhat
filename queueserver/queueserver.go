package queueserver

import (
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/mgentili/goPhat/level_log"
	"github.com/mgentili/goPhat/phatdb"
	"github.com/mgentili/goPhat/vr"
	"net"
	"net/rpc"
	"os"
)

const DEBUG = 0

var server_log *level_log.Logger

type Server struct {
	ReplicaServer   *vr.Replica
	InputChan       chan phatdb.DBCommandWithChannel
	ClientTable map[string]ClientTableEntry

}

type ClientTableEntry struct {
	SeqNumber uint
	Response *phatdb.DBResponse
}

type ClientCommand struct {
	Uid       string
	SeqNumber uint
	Command *phatdb.DBCommand
}

type Null struct{}

// wraps a DB command to conform to the vr.Command interface
type CommandFunctor struct {
	Command phatdb.DBCommandWithChannel
}

func (c CommandFunctor) CommitFunc(context interface{}) {
	server := context.(*Server)
	argsWithChannel := c.Command
	// we make our own DBCommandWithChannel so we (VR) can make sure the DB has committed before continuing on
	newArgsWithChannel := phatdb.DBCommandWithChannel{argsWithChannel.Cmd, make(chan *phatdb.DBResponse)}
	server.InputChan <- newArgsWithChannel
	// wait til the DB has actually committed the transaction
	result := <-newArgsWithChannel.Done
	// and pass the result along to the server-side RPC
	// (if we're not master .Done will be nil since channels aren't passed over RPC)
	if argsWithChannel.Done != nil {
		argsWithChannel.Done <- result
	}
}

func (s *Server) debug(level int, format string, args ...interface{}) {
	str := fmt.Sprintf("%d: %s", s.ReplicaServer.Rstate.ReplicaNumber, format)
	server_log.Printf(level, str, args...)
}

// startDB starts the database for the server
func (s *Server) startDB() {
	input := make(chan phatdb.DBCommandWithChannel)
	s.InputChan = input
	go phatdb.DatabaseServer(input)
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
	serve.startDB()
	replica.Context = serve

	newServer := rpc.NewServer()
	err = newServer.Register(serve)
	if err != nil {
		return nil, err
	}

	// have to gob.Register this struct so we can pass it through RPC
	// as a generic interface{} (I don't understand the details that well,
	// see http://stackoverflow.com/questions/21934730/gob-type-not-registered-for-interface-mapstringinterface)
	gob.Register(CommandFunctor{})
	gob.Register(phatdb.DBCommandWithChannel{})
	// Need to register all types that are returned within the DBResponse
	gob.Register(phatdb.DataNode{})
	gob.Register(phatdb.StatNode{})

	serve.debug(DEBUG, "Server at %s trying to accept new client connections\n", address)
	go newServer.Accept(listener)
	//log.Println("Accepted new connection?")
	return newServer, nil
}

// makes sure that replica is in appropriate state to respond to client request
func (s *Server) CheckState() error {
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

func (s *Server) CheckClientTable(args *ClientCommand) (*phatdb.DBResponse, error) {
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

func (s *Server) Send(args *ClientCommand, reply *phatdb.DBResponse) error {
	if err := s.CheckState(); err != nil {
		return err
	}
	res, err := s.CheckClientTable(args)
	if err != nil {
		return err
	}
	if res != nil {
		reply = res
		return nil
	}
	s.ClientTable[args.Uid] = ClientTableEntry{args.SeqNumber, nil}

	argsWithChannel := phatdb.DBCommandWithChannel{args.Command, make(chan *phatdb.DBResponse, 1)}
	s.ReplicaServer.RunVR(CommandFunctor{argsWithChannel})
	reply = <-argsWithChannel.Done
	s.ClientTable[args.Uid] = ClientTableEntry{s.ClientTable[args.Uid].SeqNumber, reply}

	return nil
}