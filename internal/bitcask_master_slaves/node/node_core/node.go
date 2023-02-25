// bitcask-cluster的节点
package nodeCore

import (
	"bitcaskDB/internal/bitcask"
	"bitcaskDB/internal/bitcask_master_slaves/node/config"
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node/nodeservice"
	"bitcaskDB/internal/options"
	"fmt"
)

type BitcaskNode struct {
	db *bitcask.BitcaskDB
	cf *config.NodeConfig

	slavesRpc map[string]nodeservice.Client
	masterRpc *nodeservice.Client
}

func NewBitcaskNode(nodeConfig *config.NodeConfig) (*BitcaskNode, error) {
	// 初始化配置
	// nodeConfig := &config.NodeConfig{
	// 	Role: config.Master,
	// 	Addr: consts.NodeAddr,
	// 	Path: config.BaseDBPath + string(os.PathListSeparator) + consts.NodeAddr,
	// }

	// 打开数据库
	opts := options.DefaultOptions(nodeConfig.Path)
	db, err := bitcask.Open(opts)
	if err != nil {
		fmt.Printf("open bitcaskdb err: %v", err)
		return nil, err
	}
	// defer db.Close()

	node := &BitcaskNode{
		db: db,
		cf: nodeConfig,
	}
	return node, nil
}

func (node *BitcaskNode) GetConfig() *config.NodeConfig {
	return node.cf
}
