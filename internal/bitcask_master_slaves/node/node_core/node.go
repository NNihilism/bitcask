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

	slavesRpc    sync.Map
	slavesStatus sync.Map
	masterRpc    nodeservice.Client

	cacheMu *sync.Mutex
	opCache *lru.Cache // 主节点用于存储最近收到的写命令，供从节点进行增量复制

	// role == master : syncStatus为nodeInFullRepl时,所有写命令需要放置缓冲区,待syncStatus为Incr或者Idle时才将缓冲区内容写入
	syncStatus nodeSynctatusCode // 对于Master节点,这个变量可以用来快速判断是否正在与某个slave进行全量复制,对于slave节点,这个变量用于判断目前自身所处状态
	// syncChan   chan syncChanItem

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
		db:         db,
		cf:         nodeConfig,
		cacheMu:    new(sync.Mutex),
		opCache:    lru.New(51200, nil),
		syncStatus: nodeInIdle,
		// syncChan:   make(chan syncChanItem, config.SyncChanSize),
		Ctx:    ctx,
		cancel: cancelFunc,
	}

	return node, nil
}

func (node *BitcaskNode) GetConfig() *config.NodeConfig {
	return node.cf
}

func (node *BitcaskNode) resetReplication() {
	// 重置 包括清空数据库数据以及相关offset
	// TODO 可以根据role进行不同程度的reset?
	node.cf.CurReplicationOffset = 0
	// node.db = NewBitcaskNode
}
