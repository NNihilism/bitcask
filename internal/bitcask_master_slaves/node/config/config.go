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

// type SyncStatus int8

// const (
// 	SyncIdle SyncStatus = iota //未进行同步中
// 	SyncBusy                   // 正在进行同步
// )

const (
	BaseDBPath                  = "/home/tmp/"
	RpcTimeOut                  = time.Second // rpc超时时间
	NodeTopology                = Star
	SyncType                    = SemiSynchronous
	SemiSynchronousRate float64 = 0.5 // 需要进行同步更新的比例
	SyncChanSize                = 100
)

var (
	// 用于记录master写入数据库中的key以及field
	MasterConfigMap = map[string]string{
		"key":              "MasterConfig",
		"field_cur_offset": "cur_offset",
	}
)

type NodeConfig struct {
	// TODO 添加字段 标识是否要删除原有目录
	Addr                    string
	ID                      string
	MasterId                string
	Role                    Role
	Path                    string
	ConnectedSlaves         int
	MasterReplicationOffset int
	CurReplicationOffset    int
}

type PSyncRespCode int8

const (
	FullReplSync PSyncRespCode = iota // 告诉从节点，准备全量复制
	IncreReplSync
	Fail
)
