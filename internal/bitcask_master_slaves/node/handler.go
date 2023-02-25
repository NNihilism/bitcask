package main

import (
	"bitcaskDB/internal/bitcask_master_slaves/node/config"
	node "bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node"
	"context"
)

// NodeServiceImpl implements the last service interface defined in the IDL.
type NodeServiceImpl struct{}

// SlaveOf implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) SlaveOf(ctx context.Context, req *node.SlaveOfRequest) (resp *node.SlaveOfRespone, err error) {
	// TODO: Your code here...
	return
}

// PSync implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) PSync(ctx context.Context, req *node.PSyncRequest) (resp *node.PSyncResponse, err error) {
	// TODO: Your code here...
	return
}

// OpLogEntry implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) OpLogEntry(ctx context.Context, req *node.LogEntryRequest) (resp *node.LogEntryRequest, err error) {
	// TODO: Your code here...
	return
}

// Ping implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) Ping(ctx context.Context) (resp *node.PingResponse, err error) {
	// TODO: Your code here...
	return
}

// Info implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) Info(ctx context.Context) (resp *node.InfoResponse, err error) {
	// TODO: Your code here...
	return &node.InfoResponse{
		Role:                    config.RoleNameMap[bitcaskNode.GetConfig().Role],
		ConnectedSlaves:         int64(bitcaskNode.GetConfig().ConnectedSlaves),
		MasterReplicationOffset: int64(bitcaskNode.GetConfig().MasterReplicationOffset),
		CurReplicationOffset:    int64(bitcaskNode.GetConfig().CurReplicationOffset),
	}, nil
}
