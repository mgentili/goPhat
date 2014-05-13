package vr

import (
	"github.com/mgentili/goPhat/phatlog"
)

type GetStateArgs struct {
	View     uint
	OpNumber uint
}

type GetStateResponse struct {
	View         uint
	Log          *phatlog.Log
	OpNumber     uint
	CommitNumber uint
}

//A replica notices that it needs a recovery
func (r *Replica) StartStateTransfer() {

	if r.Rstate.Status != Normal {
		return
	}

	r.Debug(STATUS, "Starting State Transfer")

	//fill RPC args
	args := GetStateArgs{r.Rstate.View, r.Rstate.OpNumber}

	//send State Transfer RPC to master
	r.sendAndRecvTo([]uint{r.Rstate.View % NREPLICAS}, "RPCReplica.GetState", args,
		func() interface{} { return new(GetStateResponse) },
		func(reply interface{}) bool { return r.handleGetStateResponse(reply.(*GetStateResponse)) })
}

func (t *RPCReplica) GetState(args *GetStateArgs, reply *GetStateResponse) error {
	r := t.R

	r.Debug(STATUS, "Got GetState RPC")

	if r.Rstate.Status != Normal || r.Rstate.View != args.View {
		return nil
	}

	//TODO: Only need to send new part of log
	*reply = GetStateResponse{r.Rstate.View, r.Phatlog, r.Rstate.OpNumber,
		r.Rstate.CommitNumber}

	return nil
}

func (r *Replica) handleGetStateResponse(reply *GetStateResponse) bool {
	r.Debug(STATUS, "Got NewState")

	if reply.View != r.Rstate.View {
		return true
	}

	r.Phatlog = reply.Log
	r.Rstate.OpNumber = reply.OpNumber
	r.doCommit(reply.CommitNumber)

	return true
}
