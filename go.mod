module github.com/joshuazh-x/raftx/v3

go 1.23.4

replace go.etcd.io/raft/v3 => github.com/joshuazh-x/raft/v3 v3.0.0-20250105070905-cc456a5edb8e

require go.etcd.io/raft/v3 v3.6.0

require (
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	google.golang.org/protobuf v1.33.0 // indirect
)
