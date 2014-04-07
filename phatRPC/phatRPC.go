package phatRPC

import (
	"encoding/gob"
	"errors"
	"github.com/mgentili/goPhat/phatdb"
	"github.com/mgentili/goPhat/vr"
	"github.com/mgentili/goPhat/level_log"
	"net"
	"net/rpc"
	"os"
)

const DEBUG = 0

var RPC_log *level_log.Logger

type Server struct {
	ReplicaServer   *vr.Replica
	InputChan       chan phatdb.DBCommandWithChannel
	ClientListeners map[int](chan int)
}

type Null struct{}

func Debug(format string, args ...interface{}) {
	RPC_log.Printf(DEBUG, format, args...)
}

// startDB starts the database for the server
func (s *Server) startDB() {
	input := make(chan phatdb.DBCommandWithChannel)
	s.InputChan = input
	go phatdb.DatabaseServer(input)
}

func SetupRPCLog() {
	if RPC_log == nil {
		levelsToLog := []int{DEBUG}
		RPC_log = level_log.NewLL(os.Stdout, "RPC: ")
		RPC_log.SetLevelsToLog(levelsToLog);
	}
}
// startServer starts a TCP server that accepts client requests at the given port
// and has information about the replica server
func StartServer(address string, replica *vr.Replica) (*rpc.Server, error) {
	SetupRPCLog()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	newServer := rpc.NewServer()

	serve := new(Server)
	serve.ReplicaServer = replica
	serve.startDB()

	// have to gob.Register this struct so we can pass it through RPC
	// as a generic interface{} (I don't understand the details that well,
	// see http://stackoverflow.com/questions/21934730/gob-type-not-registered-for-interface-mapstringinterface)
	gob.Register(phatdb.DBCommandWithChannel{})

	// closure to be called whenever VR wants to do a DB commit
	replica.CommitFunc = func(command interface{}) {
		argsWithChannel := command.(phatdb.DBCommandWithChannel)
		// we make our own DBCommandWithChannel so we (VR) can make sure the DB has committed before continuing on
		newArgsWithChannel := phatdb.DBCommandWithChannel{argsWithChannel.Cmd, make(chan *phatdb.DBResponse)}
		serve.InputChan <- newArgsWithChannel
		// wait til the DB has actually committed the transaction
		result := <-newArgsWithChannel.Done
		// and pass the result along to the server-side RPC
		// (if we're not master .Done will be nil since channels aren't passed over RPC)
		if argsWithChannel.Done != nil {
			argsWithChannel.Done <- result
		}
	}

	err = newServer.Register(serve)
	if err != nil {
		return nil, err
	}

	Debug("Server at %s trying to accept new client connections\n", address)
	go newServer.Accept(listener)
	//log.Println("Accepted new connection?")
	return newServer, nil
}

func (s *Server) getMasterId() uint {
	return s.ReplicaServer.Rstate.View % (vr.NREPLICAS)
}

// GetMaster returns the address of the current master replica
func (s *Server) GetMaster(args *Null, reply *uint) error {

	//if in recovery state, error
	if s.ReplicaServer.Rstate.Status != vr.Normal {
		return errors.New("Master Failover")
	}

	*reply = s.getMasterId()
	return nil
}

// RPCDB processes an RPC call sent by client
func (s *Server) RPCDB(args *phatdb.DBCommand, reply *phatdb.DBResponse) error {
	if s.ReplicaServer.Rstate.Status != vr.Normal {
		return errors.New("Master Failover")
	}

	//if the server isn't the master, the respond with an error, and send over master's address
	MasterId := s.getMasterId()
	Id := s.ReplicaServer.Rstate.ReplicaNumber
	Debug("Master id: %d, My id: %d", MasterId, Id)
	if Id != MasterId {
		Debug("I'm not the master!")
		reply.Error = "Not master node"
		reply.Reply = MasterId
		return nil
	} else {
		argsWithChannel := phatdb.DBCommandWithChannel{args, make(chan *phatdb.DBResponse, 1)}
		switch args.Command {
		//if the command is a write, then we need to go through paxos
		case "CREATE", "DELETE", "SET":
			Debug("Need to send command via Paxos")
			s.ReplicaServer.RunVR(argsWithChannel)
			Debug("Command committed, waiting for DB response")
			result := <-argsWithChannel.Done
			//Debug("Result is: %v", *result.Error, *result.Reply)
			*reply = *result
			Debug("Finished write-only")
			//paxos(args)
		default:
			//for reads we can go directly to the DB
			//TODO: make sure we have the master lease?
			// (probably just requires making sure Rstate.Status==Normal because otherwise we wouldn't
			// be considered master anymore)
			Debug("Read-only command skips Paxos")
			s.InputChan <- argsWithChannel
			result := <-argsWithChannel.Done
			*reply = *result
			Debug("Finished read-only")
		}
	}
	Debug("Got to end :-(")
	return nil
}
