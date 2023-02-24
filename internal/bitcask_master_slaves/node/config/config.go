// 数据一致性
package config

type Role int

const (
	Master Role = iota
	Slave
)

type NodeConfig struct {
	Addr string
	Role Role
}
