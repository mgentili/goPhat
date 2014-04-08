package vr

import (
	"github.com/mgentili/goPhat/phatlog"
	"math/rand"
)

type RecoveryState struct {
	RecoveryResponseMsgs    [NREPLICAS]RecoveryResponse
	RecoveryResponseReplies uint64
	RecoveryResponses       uint
	Nonce                   uint
}

type RecoveryArgs struct {
	ReplicaNumber uint
	Nonce         uint
}

type RecoveryResponse struct {
	View          uint
	Nonce         uint
	Log           *phatlog.Log
	OpNumber      uint
	CommitNumber  uint
	ReplicaNumber uint
}

func (r *Replica) resetRcvstate() {
	r.Rcvstate = RecoveryState{}
}

//A replica notices that it needs a recovery
func (r *Replica) PrepareRecovery() {

	//timeout occurs but we are already in recovery
	if r.Rstate.Status == Recovery {
		return
	}

    //change state to recovery
	r.Rstate.Status = Recovery
	r.Debug(STATUS, "Starting Recovery")

    //fill RPC args
	r.Rcvstate.Nonce = uint(rand.Uint32())
	args := RecoveryArgs{r.Rstate.ReplicaNumber, r.Rcvstate.Nonce}

    //send Recovery RPCs
	go r.sendAndRecv(NREPLICAS-1, "RPCReplica.Recovery", args,
		func() interface{} { return new(RecoveryResponse) },
		func(reply interface{}) bool { return r.handleRecoveryResponse(reply.(*RecoveryResponse)) })

}

func (t *RPCReplica) Recovery(args *RecoveryArgs, reply *RecoveryResponse) error {
	r := t.R

	r.Debug(STATUS, "Got Recovery RPC")

    //only send a response if our state is normal
	if r.Rstate.Status != Normal {
		return nil
	}

	//TODO:only send log and everything else if master
	*reply = RecoveryResponse{r.Rstate.View, args.Nonce, r.Phatlog, r.Rstate.OpNumber, r.Rstate.CommitNumber, r.Rstate.ReplicaNumber}

	return nil
}

func (r *Replica) handleRecoveryResponse(reply *RecoveryResponse) bool {
	r.Debug(STATUS, "got recoveryresponse from replica %d", reply.ReplicaNumber)

	//already recieved a recovery response message from this replica
	if ((1 << reply.ReplicaNumber) & r.Rcvstate.RecoveryResponseReplies) != 0 {
		return false
	}

	//check nonce
	if r.Rcvstate.Nonce != reply.Nonce {
		return false
	}

	r.Rcvstate.RecoveryResponseReplies |= 1 << reply.ReplicaNumber
	r.Rcvstate.RecoveryResponses++
	r.Rcvstate.RecoveryResponseMsgs[reply.ReplicaNumber] = *reply

	// update our view number
	if reply.View > r.Rstate.View {
		r.Rstate.View = reply.View
	}

	// this could be outdated, but it WON'T be outdated once we have F+1 responses
	var masterId uint = r.Rstate.View % (NREPLICAS)

	//We have recived enough Recovery messages and have recieved from master
	if r.Rcvstate.RecoveryResponses >= F+1 && ((1<<masterId)&r.Rcvstate.RecoveryResponseReplies) != 0 {
		r.Rstate.View = r.Rcvstate.RecoveryResponseMsgs[masterId].View
		r.Rstate.CommitNumber = r.Rcvstate.RecoveryResponseMsgs[masterId].CommitNumber
		r.Rstate.OpNumber = r.Rcvstate.RecoveryResponseMsgs[masterId].OpNumber
		r.Phatlog = r.Rcvstate.RecoveryResponseMsgs[masterId].Log
		r.Rstate.Status = Normal
		r.resetRcvstate()
		r.Debug(STATUS, "Done with Recovery!")

		return true
	}

	return false
}
