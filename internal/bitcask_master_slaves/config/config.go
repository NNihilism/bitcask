// 数据一致性
package config

type Role int

const (
	master Role = iota
	slave
)
