package raftx

import "go.etcd.io/raft/v3"

type Config struct {
	raft.Config

	// The number of ticks to wait before considering a node as stale.
	// Default is 5.
	StaleNodeThreshold uint64
}
