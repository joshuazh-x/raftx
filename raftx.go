package raftx

import (
	"github.com/joshuazh-x/raftx/v3/raftxpb"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	EntryTypeSubterm = 0xFF
	WitnessRejection = "witness-rejection"
)

type outboundMessagesInSameTerm map[uint64]raftpb.Message

func (ms outboundMessagesInSameTerm) addAndPurgeOld(m raftpb.Message) outboundMessagesInSameTerm {
	result := ms
	if ms.term() != m.Term {
		result = map[uint64]raftpb.Message{}
	}
	result[m.To] = m
	return result
}

func (ms outboundMessagesInSameTerm) term() uint64 {
	for _, m := range ms {
		return m.Term
	}
	return 0
}

type raftx struct {
	id           uint64
	term         uint64
	subterm      uint64
	state        raft.StateType
	cs           *raftpb.ConfState
	tracker      *peerTracker
	cfg          Config
	pendingVotes outboundMessagesInSameTerm
	msgs         []raftpb.Message
	logger       raft.Logger
	slicer       SubtermSlicer
}

func (r *raftx) maybeStartNewSubterm(rn *raft.RawNode) bool {
	if changed := r.tracker.adjustReplicationSet(); !changed {
		return false
	}

	r.subterm++

	// append subterm entry
	subtermEntry := raftpb.Entry{
		Type: EntryTypeSubterm,
		Data: encodeUint64(r.subterm),
	}

	if err := rn.Step(raftpb.Message{
		Type:    raftpb.MsgProp,
		Entries: []raftpb.Entry{subtermEntry},
		From:    r.id,
		To:      r.id,
	}); err != nil {
		r.logger.Panic("subterm entry was dropped")
	}

	r.logger.Infof("%x starts new subterm. Term: %d, Subterm: %d, Replication Set: %s", r.id, r.term, r.subterm, r.tracker.replicationSet)
	return true
}

func (r *raftx) applyConfChange(cs *raftpb.ConfState, rn *raft.RawNode) {
	r.tracker.applyConfChange(cs)

	r.maybeStartNewSubterm(rn)
}

func (r *raftx) becomeLeader(term uint64) {
	r.term = term
	r.subterm = 0
	r.state = raft.StateLeader
	r.tracker = newPeerTracker(r.cs, r.cfg.StaleNodeThreshold)
}

func (r *raftx) tick() {
	r.tracker.tick()
}

func (r *raftx) maybeSendAppendToWitness(rn *raft.RawNode) {
	term := rn.BasicStatus().Term
	commitIndex, witness := r.tracker.committedPendingWitnessAck(rn)
	commitSubterm := r.slicer.GetSubterm(commitIndex)
	if commitSubterm.Term == r.term && commitSubterm.Subterm == r.subterm {
		for _, id := range witness {
			witnessContext := raftxpb.WitnessContext{
				Context:                nil,
				LogTerm:                commitSubterm.Term,
				LogSubterm:             commitSubterm.Subterm,
				ReplicationSet:         r.tracker.replicationSet[0].ToArray(),
				ReplicationSetOutgoing: r.tracker.replicationSet[1].ToArray(),
			}
			if ctx, err := witnessContext.Marshal(); err == nil {
				m := raftpb.Message{
					Type:    raftpb.MsgApp,
					To:      id,
					From:    r.id,
					Term:    term,
					Index:   commitIndex,
					Context: ctx,
				}
				r.msgs = append(r.msgs, m)
			}
		}
	}
}

func (r *raftx) patchVoteMessageToWitness(m *raftpb.Message, votes map[uint64]bool) {
	voteArray := make([]uint64, 0, len(votes))
	for v, g := range votes {
		if g {
			voteArray = append(voteArray, v)
		}
	}
	subterm := r.slicer.GetSubterm(m.Index)
	witnessContext := raftxpb.WitnessContext{
		Context:    m.Context,
		LogTerm:    subterm.Term,
		LogSubterm: subterm.Subterm,
		Votes:      voteArray,
	}
	ctx, err := witnessContext.Marshal()
	if err != nil {
		r.logger.Panic("failed to marshal witness context")
	}
	m.Context = ctx
}

func (r *raftx) maybeSendPendingVotes(rn *raft.RawNode) {
	st := rn.BasicStatus()
	var voteTerm uint64
	switch st.RaftState {
	case raft.StatePreCandidate:
		voteTerm = st.Term + 1
	case raft.StateCandidate:
		voteTerm = st.Term
	default:
		r.pendingVotes = outboundMessagesInSameTerm{}
		return
	}
	if r.pendingVotes.term() != voteTerm {
		r.pendingVotes = outboundMessagesInSameTerm{}
		return
	}

	votes := rn.GetVotes()
	okInIncoming := r.tracker.hasSubQuorumVotes(votes, true)
	okInOutgoing := r.tracker.hasSubQuorumVotes(votes, false)
	for id, m := range r.pendingVotes {
		if (st.RaftState == raft.StatePreCandidate && m.Type != raftpb.MsgPreVote) ||
			(st.RaftState == raft.StateCandidate && m.Type != raftpb.MsgVote) {
			delete(r.pendingVotes, id)
			continue
		}
		if (okInIncoming && r.tracker.isVoter(id, true)) ||
			(okInOutgoing && r.tracker.isVoter(id, false)) {
			r.patchVoteMessageToWitness(&m, votes)
			r.msgs = append(r.msgs, m)
			delete(r.pendingVotes, id)
		}
	}
}

func (r *raftx) patchReadyMessages(msgs []raftpb.Message, rn *raft.RawNode) []raftpb.Message {
	output := make([]raftpb.Message, 0, len(msgs)+len(r.msgs)+len(r.pendingVotes))

	// filter ready messages
	for _, m := range msgs {
		if r.tracker.isWitness(m.To) {
			switch m.Type {
			case raftpb.MsgPreVote, raftpb.MsgVote:
				// hold (pre)vote requests sent by raft to witness .
				// raftx will resend them after enough (subquorum) votes are granted.
				r.pendingVotes = r.pendingVotes.addAndPurgeOld(m)
				continue
			case raftpb.MsgApp:
				// ignore append messages sent by raft to witness
				// raftx will send special append messages to witness when needed.
				continue
			}
		}
		output = append(output)
	}

	// send pending votes when we've got subquorum grants
	r.maybeSendPendingVotes(rn)

	// send out raftx messages
	output = append(output, r.msgs...)

	return output
}

func (r *raftx) setWitness(id uint64) bool {
	return r.tracker.setWitness(id)
}
