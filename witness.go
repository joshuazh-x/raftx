package raftx

import (
	"slices"

	"github.com/joshuazh-x/raftx/v3/raftxpb"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/quorum"
	"go.etcd.io/raft/v3/raftpb"
)

type witness struct {
	id             uint64
	rn             *raft.RawNode
	logTerm        uint64
	logSubterm     uint64
	replicationSet JointReplicationSet
	voters         quorum.JointConfig
	prevHardState  raftxpb.WitnessHardState

	received []raftpb.Message
	msg      []raftpb.Message
}

func (w *witness) Step(m raftpb.Message) error {
	w.received = append(w.received, m)

}

func (w *witness) hardState() raftxpb.WitnessHardState {
	st := w.rn.BasicStatus()
	return raftxpb.WitnessHardState{
		State:                  st.HardState,
		LastLogTerm:            w.logTerm,
		LastLogSubterm:         w.logSubterm,
		ReplicationSet:         w.replicationSet[0].ToArray(),
		ReplicationSetOutgoing: w.replicationSet[1].ToArray(),
	}
}

func (w *witness) unpackWitnessContext(m raftpb.Message) (raftpb.Message, raftxpb.WitnessContext, error) {
	ctx := raftxpb.WitnessContext{}
	if err := ctx.Unmarshal(m.Context); err != nil {
		return raftpb.Message{}, ctx, err
	}

	result := m
	result.Context = ctx.Context
	return result, ctx, nil
}

func (w *witness) processInRaft(m raftpb.Message) (*raftpb.Message, error) {
	if err := w.rn.Step(m); err != nil {
		return nil, err
	}

	rd := w.rn.Ready()
	defer w.rn.Advance(rd)
	if len(rd.Messages) == 0 {
		return nil, nil
	}

	if len(rd.Messages) > 1 {
		panic("multiple raft responses after one step")
	}

	return &rd.Messages[0], nil
}

func (w *witness) handleAppendEntries(m raftpb.Message) (*raftpb.Message, error) {
	m, wctx, err := w.unpackWitnessContext(m)
	if err != nil {
		return nil, err
	}
	index := m.Index
	m.Index = 0

	resp, err := w.processInRaft(m)
	if err != nil || resp == nil {
		return nil, err
	}

	if w.logTerm == wctx.LogTerm && w.logSubterm == wctx.LogSubterm {
		return resp, nil
	}
	w.logTerm = wctx.LogTerm
	w.logSubterm = wctx.LogSubterm
	w.replicationSet = JointReplicationSet{NewReplicationSet(wctx.ReplicationSet), NewReplicationSet(wctx.ReplicationSetOutgoing)}

	resp.Index = index
	return resp, nil
}

func (w *witness) handleVote(m raftpb.Message) (*raftpb.Message, error) {
	m, wctx, err := w.unpackWitnessContext(m)
	if err != nil {
		return nil, err
	}
	m.LogTerm = 0
	m.Index = 0

	resp, err := w.processInRaft(m)
	if err != nil || resp == nil {
		return nil, err
	}

	resp.Context = wctx.Context
	return resp, nil
}

func (w *witness) canVote(logTerm, logSubterm uint64, votes []uint64, from uint64) bool {
	if logTerm > w.logTerm || (logTerm == w.logTerm && logSubterm > w.logSubterm) {
		return true
	}

	canVote := false
	if logTerm == w.logTerm && logSubterm == w.logSubterm {
		for i, rs := range w.replicationSet {
			if !rs.Contains(from) || !rs.Contains(w.id) {
				continue
			}
			vs := map[uint64]bool{}
			for _, v := range votes {
				if rs.Contains(v) {
					vs[v] = true
				}
			}
			vs[w.id] = true
			if w.voters[i].VoteResult(vs) != quorum.VoteWon {
				return false
			}
			canVote = true
		}
	}

	return canVote
}

func (w *witness) getReplicationSetScopedVoteResult(votes []uint64, incoming bool) quorum.VoteResult {
	i := 0
	if !incoming {
		i = 1
	}
	vs := map[uint64]bool{}
	for _, v := range votes {
		vs[v] = true
	}
	vs[w.id] = true
	return w.voters[i].VoteResult(vs)
}

func isWitnessHardStateEqual(a, b raftxpb.WitnessHardState) bool {
	return a.State.Term == b.State.Term &&
		a.State.Vote == b.State.Vote &&
		a.State.Commit == b.State.Commit &&
		a.LastLogTerm == b.LastLogTerm &&
		a.LastLogSubterm == b.LastLogSubterm &&
		slices.Compare(a.ReplicationSet, b.ReplicationSet) == 0 &&
		slices.Compare(a.ReplicationSetOutgoing, b.ReplicationSetOutgoing) == 0
}
