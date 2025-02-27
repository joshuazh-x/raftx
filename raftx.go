package raftx

import (
	"github.com/joshuazh-x/raftx/v3/raftxpb"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/quorum"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	EntryTypeSubterm = 0xFF
	WitnessRejection = "witness-rejection"
)

type raftx struct {
	id          uint64
	term        uint64
	subterm     uint64
	state       raft.StateType
	cs          *raftpb.ConfState
	tracker     *peerTracker
	cfg         Config
	witnessMsgs []raftpb.Message
	logger      raft.Logger
	slicer      SubtermSlicer
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

	// get subterm entry location

	r.slicer.AddSubterm(r.term, r.subterm, rn.BasicStatus().LastLogIndex)

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
	commitIndex, witness := r.tracker.committedPendingWitnessAck(rn)
	commitSubterm := r.slicer.GetSubterm(r.term, commitIndex)
	if commitSubterm == r.subterm {
		for _, id := range witness {
			witnessContext := raftxpb.WitnessContext{
				Context:                nil,
				Subterm:                r.subterm,
				ReplicationSet:         r.tracker.replicationSet[0].ToArray(),
				ReplicationSetOutgoing: r.tracker.replicationSet[1].ToArray(),
			}
			if ctx, err := witnessContext.Marshal(); err == nil {
				m := raftpb.Message{
					Type:    raftpb.MsgApp,
					To:      id,
					From:    r.id,
					Term:    r.term,
					Context: ctx,
				}
				r.witnessMsgs = append(r.witnessMsgs, m)
			}
		}
	}
}

func (r *raftx) patchAndTransferMessages(msgs []raftpb.Message, rn *raft.RawNode) []raftpb.Message {
	hold := make([]raftpb.Message, 0, len(msgs)+len(r.witnessMsgs))
	output := make([]raftpb.Message, 0, len(msgs)+len(r.witnessMsgs))
	votes := rn.GetVotes()
	witnesses := map[uint64]struct{}{}

	for _, m := range msgs {
		switch m.Type {
		case raftpb.MsgPreVote, raftpb.MsgVote:
			// hold (pre)vote requests sent by raft to witness if we have not got enough grants.
			// raftx will resend them after enough (subquorum) votes are granted.
			if r.tracker.isWitness(m.To) {
				witnesses[m.To] = struct{}{}
				switch r.tracker.voteResultWithExtraVote(votes, m.To) {
				case quorum.VoteWon:
					// send out raftx pending witness vote if we've got enough grants
					witnessContext := raftxpb.WitnessContext{
						Context: nil,
						Subterm: r.subterm,
						Votes:   votes,
					}
					if ctx, err := witnessContext.Marshal(); err == nil {
						m := raftpb.Message{
							Type:    raftpb.MsgApp,
							To:      id,
							From:    r.id,
							Term:    r.term,
							Context: ctx,
						}
						r.witnessMsgs = append(r.witnessMsgs, m)
					}
					output = append(output, m)
				case quorum.VoteLost:
					// or throw it if the candidate already losts election
					continue
				case quorum.VotePending:
					// hold it back for next round of check if we did not get enought grants
					hold = append(hold, m)
				}
			}
		case raftpb.MsgApp:
			// ignore append messages sent by raft to witness
			// raftx will send special append messages to witness when needed.
			if r.tracker.isWitness(m.To) {
				continue
			}
		}
		output = append(output)
	}

	for _, m := range r.witnessMsgs {
		if !r.tracker.isWitness(m.To) {
			// throw messages that are no longer sent to a witness
			continue
		}
		switch m.Type {
		case raftpb.MsgPreVote, raftpb.MsgVote:
			if _, exist := witnesses[m.To]; exist {
				// we only hold or send the latest vote messages to each witness.
				continue
			}
			switch r.tracker.voteResultWithExtraVote(votes, m.To) {
			case quorum.VoteWon:
				// send out raftx pending witness vote if we've got enough grants
				output = append(output, m)
			case quorum.VoteLost:
				// or throw it if the candidate already losts election
				continue
			case quorum.VotePending:
				// hold it back for next round of check if we did not get enought grants
				hold = append(hold, m)
			}
		}
		output = append(output)
	}

	r.witnessMsgs = hold
	return output
}

func (r *raftx) setWitness(id uint64) bool {
	return r.tracker.setWitness(id)
}
