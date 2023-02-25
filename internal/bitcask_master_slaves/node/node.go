// bitcask-cluster的节点
package main

import (
	"bitcaskDB/internal/bitcask_master_slaves/node/config"
	"bitcaskDB/internal/bitcask_master_slaves/node/node"
	"bitcaskDB/internal/bitcask_master_slaves/pkg/consts"
	"bitcaskDB/internal/log"
	"os"
)

func init() {
	nodeConfig := &config.NodeConfig{
		Role: config.Master,
		Addr: consts.NodeAddr,
		Path: config.BaseDBPath + string(os.PathListSeparator) + consts.NodeAddr,
	}

	var err error
	bitcaskNode, err = node.NewBitcaskNode(nodeConfig)
	if err != nil {
		log.Errorf("create bitcasknode err : %v", err)
	}
}

var bitcaskNode *node.BitcaskNode
