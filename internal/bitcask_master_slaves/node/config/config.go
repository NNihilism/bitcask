package config

import "time"

type Role int

const (
	Master Role = iota
	Slave
)

var RoleNameMap = map[Role]string{
	Master: "master",
	Slave:  "slave",
}

type Topology int8

const (
	Star Topology = iota
	Line
)

type SyncTypeCode int8

const (
	Synchronous     SyncTypeCode = iota // 同步
	SemiSynchronous                     // 半同步
	Asynchronous                        // 异步
)

const (
	BaseDBPath                  = "/home/tmp/"
	RpcTimeOut                  = time.Second // rpc超时时间
	NodeTopology                = Star
	SyncType                    = SemiSynchronous
	SemiSynchronousRate float64 = 0.5 // 需要进行同步更新的比例
)

type NodeConfig struct {
	Addr                    string
	ID                      string
	Role                    Role
	Path                    string
	ConnectedSlaves         int
	MasterReplicationOffset int
	CurReplicationOffset    int
}
