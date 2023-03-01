package main

import (
	prxyservice "bitcaskDB/internal/bitcask_master_slaves/proxy/kitex_gen/prxyService"
	"context"
)

// ProxyServiceImpl implements the last service interface defined in the IDL.
type ProxyServiceImpl struct{}

// Ping implements the ProxyServiceImpl interface.
func (s *ProxyServiceImpl) Ping(ctx context.Context, req bool) (resp bool, err error) {
	return true, nil
}

// OpLogEntry implements the ProxyServiceImpl interface.
func (s *ProxyServiceImpl) OpLogEntry(ctx context.Context, req *prxyservice.LogEntryRequest) (resp *prxyservice.LogEntryResponse, err error) {
	return nodeProxy.RedirOpReq(req)
}

// Proxy implements the ProxyServiceImpl interface.
func (s *ProxyServiceImpl) Proxy(ctx context.Context, masterAddr string) (resp bool, err error) {
	return nodeProxy.HandleProxyReq(masterAddr)
}
