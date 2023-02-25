// 数据一致性
package config

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
	Star = iota
	Line
)
const (
	NodeTopology = Star
)

const (
	BaseDBPath = "/home/tmp/"
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
