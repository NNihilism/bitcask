// bitcask-cluster的节点
package nodeCore

import (
	"bitcaskDB/internal/bitcask"
	"bitcaskDB/internal/bitcask_master_slaves/node/config"
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node/nodeservice"
	"bitcaskDB/internal/bitcask_master_slaves/node/util/lru"
	"bitcaskDB/internal/options"
	"fmt"
	"sync"
)

type nodeSynctatusCode int8

const (
	nodeInFullRepl nodeSynctatusCode = iota //正在跟子节点进行全量复制
	nodeInIncrRepl                          // 正在跟子节点进行增量复制
	nodeInIdle                              // 跟子节点正常通信
)

type BitcaskNode struct {
	db *bitcask.BitcaskDB
	cf *config.NodeConfig

	slavesRpc    map[string]nodeservice.Client
	slavesStatus map[string]nodeSynctatusCode
	masterRpc    nodeservice.Client

	cacheMu *sync.Mutex
	opCache *lru.Cache // 主节点用于存储最近收到的写命令，供从节点进行增量复制

	synctatus nodeSynctatusCode
}

func NewBitcaskNode(nodeConfig *config.NodeConfig) (*BitcaskNode, error) {
	opts := options.DefaultOptions(nodeConfig.Path)
	db, err := bitcask.Open(opts)
	if err != nil {
		fmt.Printf("open bitcaskdb err: %v", err)
		return nil, err
	}
	// defer db.Close()

	node := &BitcaskNode{
		db:        db,
		cf:        nodeConfig,
		cacheMu:   new(sync.Mutex),
		opCache:   lru.New(51200, nil),
		synctatus: nodeInIdle,
	}
	return node, nil
}

func (node *BitcaskNode) GetConfig() *config.NodeConfig {
	return node.cf
}
