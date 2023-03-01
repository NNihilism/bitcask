// bitcask-cluster的节点
package main

import (
	"bitcaskDB/internal/bitcask_master_slaves/node/config"
	nodeCore "bitcaskDB/internal/bitcask_master_slaves/node/node_core"
	"bitcaskDB/internal/bitcask_master_slaves/pkg/consts"
	"bitcaskDB/internal/log"
	"bitcaskDB/internal/options"
	"strings"
)

func init() {
	parts := strings.Split(consts.NodeAddr, ":") // []string{"ip", "port"}
	nodeConfig := &config.NodeConfig{
		Role:      config.Master,
		Addr:      consts.NodeAddr,
		Path:      config.BaseDBPath + parts[1],
		ID:        consts.NodeAddr,
		RemakeDir: config.RemakeDir,
	}

	var err error
	bitcaskNode, err = nodeCore.NewBitcaskNode(nodeConfig, options.Options{})
	if err != nil {
		log.Errorf("create bitcasknode err : %v", err)
	}

}

var bitcaskNode *nodeCore.BitcaskNode
