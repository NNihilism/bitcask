// bitcask-cluster的节点
package nodeCore

import (
	"bitcaskDB/internal/bitcask"
	"bitcaskDB/internal/bitcask_master_slaves/node/config"
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node"
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node/nodeservice"
	"bitcaskDB/internal/bitcask_master_slaves/node/util/lru"
	"bitcaskDB/internal/log"
	"bitcaskDB/internal/options"
	"context"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"
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

type slaveInfo struct {
	address string
	id      string
	status  nodeSynctatusCode
	weight  int
	rpc     nodeservice.Client
	// replicationBuffer
}

type BitcaskNode struct {
	db *bitcask.BitcaskDB
	cf *config.NodeConfig

	slaveInfoMu         *sync.RWMutex
	slavesInfo          sync.Map
	infosLastUpdateTime int64

	masterRpc nodeservice.Client

	cacheMu       *sync.Mutex
	replBakBuffer *lru.Cache // 主节点用于存储最近收到的写命令，供从节点进行增量复制

	// role == master : syncStatus为nodeInFullRepl时,所有写命令需要放置缓冲区,待syncStatus为Incr或者Idle时才将缓冲区内容写入
	syncStatus nodeSynctatusCode // 对于Master节点,这个变量可以用来快速判断是否正在与某个slave进行全量复制,对于slave节点,这个变量用于判断目前自身所处状态
	// syncChan   chan syncChanItem

	Ctx    context.Context
	cancel context.CancelFunc
	once   *sync.Once
}

func NewBitcaskNode(nodeConfig *config.NodeConfig, opts options.Options) (*BitcaskNode, error) {
	// 参数为空，则使用默认配置
	if reflect.DeepEqual(opts, options.Options{}) {
		opts = options.DefaultOptions(nodeConfig.Path)
	}
	opts.RemakeDir = nodeConfig.RemakeDir

	// 创建数据库
	db, err := bitcask.Open(opts)
	if err != nil {
		fmt.Printf("open bitcaskdb err: %v", err)
		return nil, err
	}

	// 读取数据库中的偏移字段
	strOffset, _ := db.HGet(config.MasterConfigMap["key"], config.MasterConfigMap["cur_offset"])
	var offset int
	if strOffset != nil {
		offset, err := strconv.Atoi(string(strOffset))
		if err != nil {
			log.Errorf("strconv.Atoi(%s) err [%v]", offset, err)
		}
	}
	nodeConfig.CurReplicationOffset = offset

	ctx, cancelFunc := context.WithCancel(context.Background())
	// 创建node节点
	node := &BitcaskNode{
		db:                  db,
		cf:                  nodeConfig,
		slaveInfoMu:         new(sync.RWMutex),
		infosLastUpdateTime: time.Now().Unix(),
		cacheMu:             new(sync.Mutex),
		replBakBuffer:       lru.New(51200, nil),
		syncStatus:          nodeInIdle,
		once:                new(sync.Once),
		// syncChan:   make(chan syncChanItem, config.SyncChanSize),
		Ctx:    ctx,
		cancel: cancelFunc,
	}

	go node.checkSlavesAlive(node.Ctx, *time.NewTicker(time.Second * config.MasterHeartBeatFreq))

	return node, nil
}

func (node *BitcaskNode) GetConfig() *config.NodeConfig {
	return node.cf
}

func (node *BitcaskNode) resetReplication(resetDB bool) {
	// 重置 包括清空数据库数据以及相关offset
	// TODO 可以根据role进行不同程度的reset?
	node.cf.CurReplicationOffset = 0

	if resetDB {
		opts := options.DefaultOptions(node.cf.Path)
		opts.RemakeDir = true
		db, err := bitcask.Open(opts)
		log.Infof("MASTER <-> REPLICA sync: Flushing old data")
		if err != nil {
			fmt.Printf("open bitcaskdb err: %v", err)
			return
		}
		node.db = db
	}
}

func (node *BitcaskNode) Close() {
	node.db.Close()
	node.cancel()
}
