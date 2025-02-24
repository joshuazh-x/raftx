package raftx

import (
	"context"

	raft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type node struct {
	rn raft.Node

	rx raftx
}

func (n *node) Tick() {
	n.rn.Tick()
	n.rx.tick()
}

func (n *node) Campaign(ctx context.Context) error {
	return n.rn.Campaign(ctx)
}

func (n *node) Propose(ctx context.Context, data []byte) error {
	return n.rn.Propose(ctx, data)
}

func (n *node) ProposeConfChange(ctx context.Context, cc raftpb.ConfChangeI) error {
	return n.rn.ProposeConfChange(ctx, cc)
}

func (n *node) Step(ctx context.Context, msg raftpb.Message) error {
	switch msg.Type {
	case raftpb.MsgAppResp:
		n.DoCustomCommand(ctx, NewChangeSubtermIfRequiredCommand(&n.rx, msg.From))

	case raftpb.MsgHeartbeatResp:
		n.DoCustomCommand(ctx, NewChangeSubtermIfRequiredCommand(&n.rx, msg.From))
	}
	return n.rn.Step(ctx, msg)
}

func (n *node) Ready() <-chan raft.Ready {
	return n.rn.Ready()
}

func (n *node) Advance() {
	n.rn.Advance()
}

func (n *node) ApplyConfChange(cc raftpb.ConfChangeI) *raftpb.ConfState {
	cs := n.rn.ApplyConfChange(cc)
	n.DoCustomCommand(context.Background(), NewApplyConfChangeCommand(&n.rx, cs))
	return cs
}

func (n *node) Stop() {
	n.rn.Stop()
}

func (n *node) Status() raft.Status {
	return n.rn.Status()
}

func (n *node) ReportUnreachable(id uint64) {
	n.rn.ReportUnreachable(id)
}

func (n *node) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	n.rn.ReportSnapshot(id, status)
}

func (n *node) DoCustomCommand(ctx context.Context, cmd raft.CustomCommand) error {
	return n.rn.DoCustomCommand(ctx, cmd)
}
