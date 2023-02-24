// bitcask-cluster的节点
package main

import (
	"bitcaskDB/internal/bitcask"
	"bitcaskDB/internal/bitcask_master_slaves/node/config"
)

type bitcaskNode struct {
	db   *bitcask.BitcaskDB
	role config.Role
}
