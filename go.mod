module github.com/joshuazh-x/raftx/v3

go 1.23.4

replace go.etcd.io/raft/v3 => github.com/joshuazh-x/raft/v3 v3.0.0-20250302121736-059d8d549c76

require (
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.5.4
	go.etcd.io/raft/v3 v3.6.0
)

require google.golang.org/protobuf v1.36.5 // indirect
