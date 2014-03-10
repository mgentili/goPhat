package phatRPC

import (
	"github.com/mgentili/goPhat/phatdb"
	"github.com/mgentili/goPhat/vr"
	"log"
	"net"
	"net/rpc"
	"os"
)

const DEBUG = false
var RPC_log *log.Logger 

type Server struct {
	ReplicaServer *vr.Replica
	InputChan       chan phatdb.DBCommandWithChannel
}

type Null struct{}

/*func (s *Server) Debug(format string, args ...interface{}) {
	RPC_log.Printf(format, args...)
}*/

// startDB starts the database for the server
func (s *Server) startDB() {
	RPC_log.Printf("Starting DB")
	input := make(chan phatdb.DBCommandWithChannel)
	s.InputChan = input
	go phatdb.DatabaseServer(input)
}

// startServer starts a TCP server that accepts client requests at the given port
// and has information about the replica server
func StartServer(address string, replica *vr.Replica) (*rpc.Server, error) {
	if RPC_log == nil {
		RPC_log = log.New(os.Stdout, "RPC: ", log.Ltime|log.Lmicroseconds)
	}
	listener, err := net.Listen("tcp", address)
	if (err != nil) {
		return nil, err
	}

	newServer := rpc.NewServer()

	serve := new(Server)
	serve.ReplicaServer = replica
	serve.startDB()

	err = newServer.Register(serve)
	if (err != nil) {
		return nil, err
	}

	RPC_log.Printf("Server at %s trying to accept new client connections\n", address)
	go newServer.Accept(listener)
	//log.Println("Accepted new connection?")
	return newServer, nil
}

// GetMaster returns the address of the current master replica
func (s *Server) GetMaster(args *Null, reply *uint) error {
	//TODO: If in recovery state, respond with error
	*reply = s.ReplicaServer.Rstate.View % (vr.NREPLICAS+1)
	return nil
}

// RPCDB processes an RPC call sent by client
func (s *Server) RPCDB(args *phatdb.DBCommand, reply *phatdb.DBResponse) error {
	//if the server isn't the master, the respond with an error, and send over master's address
	MasterId := s.ReplicaServer.Rstate.View % (vr.NREPLICAS+1)
	Id := s.ReplicaServer.Rstate.ReplicaNumber
	RPC_log.Printf("Master id: %d, My id: %d", MasterId, Id)
	if Id != MasterId {
		RPC_log.Printf("I'm not the master!")
		reply.Error = "Not master node"
		reply.Reply = MasterId
		return nil
	} else {
		argsWithChannel := phatdb.DBCommandWithChannel{args, make(chan *phatdb.DBResponse)}
		switch args.Command {
		//if the command is a write, then we need to go through paxos
		case "CREATE", "DELETE", "SET":
			RPC_log.Printf("Need to send command via Paxos")
			if !DEBUG {
				RPC_log.Printf("Not debugging, so using Paxos")
				s.ReplicaServer.RunVR(argsWithChannel)
			} else {
				RPC_log.Printf("Debugging, so skipping paxos")
				s.InputChan <- argsWithChannel
			}
			RPC_log.Printf("Command committed, waiting for DB response")
			result := <-argsWithChannel.Done
			*reply = *result
			RPC_log.Printf("Finished write-only")
			//paxos(args)
		default:
			//for reads we can go directly to the DB
			RPC_log.Printf("Read-only command skips Paxos")
			s.InputChan <- argsWithChannel
			result := <-argsWithChannel.Done
			*reply = *result
			RPC_log.Printf("Finished read-only")
		}
	}
	return nil
}
