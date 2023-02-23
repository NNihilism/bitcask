// bitcask-cluster的节点
package node

import (
	"bitcaskDB/internal/bitcask"
	"bitcaskDB/internal/bitcask_master_slaves/config"
)

type bitcaskNode struct {
	db   *bitcask.BitcaskDB
	role config.Role
}
