// bitcask-cluster的节点
package nodeCore

import (
	"bitcaskDB/internal/bitcask"
	"bitcaskDB/internal/bitcask_master_slaves/node/config"
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node"
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node/nodeservice"
	"bitcaskDB/internal/bitcask_master_slaves/node/util/lru"
	"bitcaskDB/internal/options"
	"context"
	"fmt"
	"sync"
)

type nodeSynctatusCode int8

const (
	nodeInFullRepl nodeSynctatusCode = iota // 全量复制
	nodeInIncrRepl                          // 增量复制
	nodeInIdle                              // 命令传输
)

type syncChanItem struct {
	req     *node.LogEntryRequest
	slaveId string
}
type BitcaskNode struct {
	db *bitcask.BitcaskDB
	cf *config.NodeConfig

	// slavesRpc    map[string]nodeservice.Client
	// slavesStatus map[string]nodeSynctatusCode
	slavesRpc    sync.Map
	slavesStatus sync.Map
	masterRpc    nodeservice.Client

	cacheMu *sync.Mutex
	opCache *lru.Cache // 主节点用于存储最近收到的写命令，供从节点进行增量复制

	synctatus nodeSynctatusCode
	syncChan  chan syncChanItem

	Ctx    context.Context
	cancel context.CancelFunc
}

func NewBitcaskNode(nodeConfig *config.NodeConfig) (*BitcaskNode, error) {
	opts := options.DefaultOptions(nodeConfig.Path)
	db, err := bitcask.Open(opts)
	if err != nil {
		fmt.Printf("open bitcaskdb err: %v", err)
		return nil, err
	}
	// defer db.Close()

	ctx, cancelFunc := context.WithCancel(context.Background())
	node := &BitcaskNode{
		db:        db,
		cf:        nodeConfig,
		cacheMu:   new(sync.Mutex),
		opCache:   lru.New(51200, nil),
		synctatus: nodeInIdle,
		syncChan:  make(chan syncChanItem, config.SyncChanSize),
		Ctx:       ctx,
		cancel:    cancelFunc,
	}

	return node, nil
}

func (node *BitcaskNode) GetConfig() *config.NodeConfig {
	return node.cf
}
