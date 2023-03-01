package main

import (
	"bitcaskDB/internal/bitcask_master_slaves/node/config"
	node "bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node"
	"context"
)

// NodeServiceImpl implements the last service interface defined in the IDL.
type NodeServiceImpl struct{}

// PSync implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) PSync(ctx context.Context, req *node.PSyncRequest) (resp *node.PSyncResponse, err error) {
	return bitcaskNode.HandlePSyncReq(req)
}

// Ping implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) Ping(ctx context.Context) (resp *node.PingResponse, err error) {
	return &node.PingResponse{Status: true}, nil
}

// Info implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) Info(ctx context.Context) (resp *node.InfoResponse, err error) {
	return &node.InfoResponse{
		Role:                    config.RoleNameMap[bitcaskNode.GetConfig().Role],
		ConnectedSlaves:         int64(bitcaskNode.GetConfig().ConnectedSlaves),
		MasterReplicationOffset: int64(bitcaskNode.GetConfig().MasterReplicationOffset),
		CurReplicationOffset:    int64(bitcaskNode.GetConfig().CurReplicationOffset),
	}, nil
}

// SendSlaveof implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) SendSlaveof(ctx context.Context, req *node.SendSlaveofRequest) (resp *node.SendSlaveofResponse, err error) {
	return bitcaskNode.SendSlaveOfReq(req)
}

// RegisterSlave implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) RegisterSlave(ctx context.Context, req *node.RegisterSlaveRequest) (resp *node.RegisterSlaveResponse, err error) {
	return bitcaskNode.HandleSlaveOfReq(req)
}

// OpLogEntry implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) OpLogEntry(ctx context.Context, req *node.LogEntryRequest) (resp *node.LogEntryResponse, err error) {
	return bitcaskNode.HandleOpLogEntryRequest(req)
}

// // IncrReplFailNotify implements the NodeServiceImpl interface.
// func (s *NodeServiceImpl) IncrReplFailNotify(ctx context.Context, masterId string) (resp bool, err error) {
// 	return bitcaskNode.HandleRepFailNotify(masterId)
// }

// ReplFinishNotify implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) ReplFinishNotify(ctx context.Context, req *node.ReplFinishNotifyReq) (resp bool, err error) {
	return bitcaskNode.HandleReplFinishNotify(req)
}

// PSyncReq implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) PSyncReq(ctx context.Context, req *node.PSyncRequest) (resp *node.PSyncResponse, err error) {
	return bitcaskNode.HandlePSyncReq(req)
}

// PSyncReady implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) PSyncReady(ctx context.Context, req *node.PSyncRequest) (resp *node.PSyncResponse, err error) {
	return bitcaskNode.HandlePSyncReady(req)
}

// GetAllNodesInfo implements the NodeServiceImpl interface.
func (s *NodeServiceImpl) GetAllNodesInfo(ctx context.Context, req *node.GetAllNodesInfoReq) (resp *node.GetAllNodesInfoResp, err error) {
	return bitcaskNode.GetAllNodesInfo(req)
}
