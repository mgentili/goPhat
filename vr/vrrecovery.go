package vr

import (
	"log"
	"math/rand"
)

//A replica notices that it needs a recovery (not sure how yet!)
func (r *Replica) PrepareRecovery() {

	//timeout occurs but we are already in recovery (might not be possible to hit this)
	if r.Rstate.Status == Recovery {
		return
	}

	r.Rstate.Status = Recovery
	log.Printf("Recovery")

	//I wanted a uint since everything else is, but there isn't a rand.Uint function?
	r.Rcvstate.Nonce = uint(rand.Uint32())
	args := RecoveryArgs{r.Rstate.ReplicaNumber, r.Rcvstate.Nonce}

	go r.sendAndRecv(NREPLICAS, "RPCReplica.Recovery", args,
		func() interface{} { return nil },
		func(r interface{}) bool { return false })

}

func (t *RPCReplica) Recovery(args *RecoveryArgs, reply *int) error {
	r := t.R

	r.Debug("Got Recovery RPC")
	//only send a response if our state is normal
	if r.Rstate.Status != Normal {
		return nil
	}

	//TODO:only send log and everything else if master (can i pass nil values for these params?)
	RRargs := RecoveryResponseArgs{r.Rstate.View, args.Nonce, r.Phatlog, r.Rstate.OpNumber, r.Rstate.CommitNumber, r.Rstate.ReplicaNumber}

	r.SendSync(args.ReplicaNumber, "RPCReplica.RecoveryResponse", RRargs, nil)

	return nil
}

func (t *RPCReplica) RecoveryResponse(args *RecoveryResponseArgs, reply *int) error {
	r := t.R

	r.Debug("got recoveryresponse")

	//already recieved a recovery response message from this replica
	if ((1 << args.ReplicaNumber) & r.Rcvstate.RecoveryResponseReplies) != 0 {
		return nil
	}

	//check nonce
	if r.Rcvstate.Nonce != args.Nonce {
		return nil
	}

	//these variable names are a little silly..
	r.Rcvstate.RecoveryResponseReplies |= 1 << args.ReplicaNumber
	r.Rcvstate.RecoveryResponses++
	r.Rcvstate.RecoveryResponseMsgs[args.ReplicaNumber] = *args

	var masterId uint = r.Rstate.View % (NREPLICAS + 1)

	//We have recived enough Recovery messages and have recieved from master
	if r.Rcvstate.RecoveryResponses >= F+1 && ((1<<masterId)&r.Rcvstate.RecoveryResponseReplies) == 1 {
		r.Rstate.View = r.Rcvstate.RecoveryResponseMsgs[masterId].View
		r.Rstate.CommitNumber = r.Rcvstate.RecoveryResponseMsgs[masterId].CommitNumber
		r.Rstate.OpNumber = r.Rcvstate.RecoveryResponseMsgs[masterId].OpNumber
		r.Phatlog = r.Rcvstate.RecoveryResponseMsgs[masterId].Log
		r.Rstate.Status = Normal
		log.Printf("Done with Recovery!")

	}

	return nil
}
