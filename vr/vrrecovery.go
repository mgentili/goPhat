package vr

import (
	"github.com/mgentili/goPhat/phatlog"
	"math/rand"
)

type RecoveryState struct {
	RecoveryResponseMsgs    []RecoveryResponse
	RecoveryResponseReplies uint64
	EmptyLogs               uint
	RecoveryResponses       uint
	Nonce                   uint
}

type RecoveryArgs struct {
	ReplicaNumber uint
	Nonce         uint
	// index of our snapshot (aka only need to send after this point)
	SnapshotIndex uint
}

type RecoveryResponse struct {
	View          uint
	Nonce         uint
	Log           *phatlog.Log
	Snapshot      []byte
	OpNumber      uint
	CommitNumber  uint
	ReplicaNumber uint
	Normal        bool
}

func (r *Replica) resetRcvstate() {
	r.Rcvstate = RecoveryState{}
	r.Rcvstate.RecoveryResponseMsgs = make([]RecoveryResponse, NREPLICAS)
}

//A replica notices that it needs a recovery
func (r *Replica) PrepareRecovery() {

	// already in recovery
	if r.Rstate.Status == Recovery {
		return
	}

	//change state to recovery
	r.Rstate.Status = Recovery
	r.Debug(STATUS, "Starting Recovery")

	r.resetRcvstate()

	//fill RPC args
	r.Rcvstate.Nonce = uint(rand.Uint32())
	args := RecoveryArgs{r.Rstate.ReplicaNumber, r.Rcvstate.Nonce, r.SnapshotIndex}

	//send Recovery RPCs
	go r.sendAndRecv(NREPLICAS-1, "RPCReplica.Recovery", args,
		func() interface{} { return new(RecoveryResponse) },
		func(reply interface{}) bool { return r.handleRecoveryResponse(reply.(*RecoveryResponse)) })

}

func (t *RPCReplica) Recovery(args *RecoveryArgs, reply *RecoveryResponse) error {
	r := t.R

	r.Debug(STATUS, "Got Recovery RPC")

	var log *phatlog.Log = nil
	var snapshot []byte = nil
	if r.IsMaster() {
		log, snapshot = r.RecoverInfoFromOpNumber(args.SnapshotIndex)
	}
	*reply = RecoveryResponse{r.Rstate.View, args.Nonce, log, snapshot, r.Rstate.OpNumber,
		r.Rstate.CommitNumber, r.Rstate.ReplicaNumber, r.Rstate.Status == Normal}

	return nil
}

func (r *Replica) handleRecoveryResponse(reply *RecoveryResponse) (done bool) {
	r.Debug(STATUS, "Got recoveryresponse from replica %d", reply.ReplicaNumber)

	done = false

	defer func() {
		if done {
			r.resetRcvstate()
		}
	}()

	//already recieved a recovery response message from this replica
	if ((1 << reply.ReplicaNumber) & r.Rcvstate.RecoveryResponseReplies) != 0 {
		return
	}

	//check nonce
	if r.Rcvstate.Nonce != reply.Nonce {
		return
	}

	r.Rcvstate.RecoveryResponseReplies |= 1 << reply.ReplicaNumber
	if reply.Normal {
		r.Rcvstate.RecoveryResponses++
	}

	// TODO: how does this work with snapshots?
	if reply.OpNumber == 0 {
		r.Rcvstate.EmptyLogs++
	}

	r.Rcvstate.RecoveryResponseMsgs[reply.ReplicaNumber] = *reply

	// if majority of replicas respond with empty logs, then we've just started
	// so we go into view change
	if r.Rcvstate.EmptyLogs >= F+1 {
		r.PrepareViewChange()
		r.Debug(STATUS, "Received quorum of empty logs, going to Normal")
		done = true
		return
	}

	if !reply.Normal {
		return
	}

	// update our view number
	if reply.View > r.Rstate.View {
		r.Rstate.View = reply.View
	}

	// this could be outdated, but it WON'T be outdated once we have F+1 responses
	var masterId uint = r.Rstate.View % NREPLICAS

	//We have recived enough Recovery messages and have recieved from master
	if r.Rcvstate.RecoveryResponses >= F+1 && ((1<<masterId)&r.Rcvstate.RecoveryResponseReplies) != 0 {
		assert(r.Rcvstate.RecoveryResponseMsgs[masterId].CommitNumber >= r.SnapshotIndex)
		r.Rstate.View = r.Rcvstate.RecoveryResponseMsgs[masterId].View
		r.Phatlog = r.Rcvstate.RecoveryResponseMsgs[masterId].Log
		if r.Phatlog == nil {
			r.Phatlog = phatlog.EmptyLog()
		}
		r.Rstate.OpNumber = r.Rcvstate.RecoveryResponseMsgs[masterId].OpNumber
		r.doCommit(r.Rcvstate.RecoveryResponseMsgs[masterId].CommitNumber)
		assert(r.Rstate.CommitNumber == r.Rcvstate.RecoveryResponseMsgs[masterId].CommitNumber)
		r.Rstate.Status = Normal
		done = true
		r.Debug(STATUS, "Done with Recovery!")
	}

	return
}
