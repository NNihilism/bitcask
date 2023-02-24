// Code generated by Kitex v0.4.4. DO NOT EDIT.

package nodeservice

import (
	node "bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node"
	"context"
	client "github.com/cloudwego/kitex/client"
	callopt "github.com/cloudwego/kitex/client/callopt"
)

// Client is designed to provide IDL-compatible methods with call-option parameter for kitex framework.
type Client interface {
	SlaveOf(ctx context.Context, req *node.SlaveOfRequest, callOptions ...callopt.Option) (r *node.SlaveOfRespone, err error)
	PSync(ctx context.Context, req *node.PSyncRequest, callOptions ...callopt.Option) (r *node.PSyncResponse, err error)
	OpLogEntry(ctx context.Context, req *node.LogEntryRequest, callOptions ...callopt.Option) (r *node.LogEntryRequest, err error)
	Ping(ctx context.Context, callOptions ...callopt.Option) (r *node.PingResponse, err error)
	Info(ctx context.Context, callOptions ...callopt.Option) (r *node.InfoResponse, err error)
}

// NewClient creates a client for the service defined in IDL.
func NewClient(destService string, opts ...client.Option) (Client, error) {
	var options []client.Option
	options = append(options, client.WithDestService(destService))

	options = append(options, opts...)

	kc, err := client.NewClient(serviceInfo(), options...)
	if err != nil {
		return nil, err
	}
	return &kNodeServiceClient{
		kClient: newServiceClient(kc),
	}, nil
}

// MustNewClient creates a client for the service defined in IDL. It panics if any error occurs.
func MustNewClient(destService string, opts ...client.Option) Client {
	kc, err := NewClient(destService, opts...)
	if err != nil {
		panic(err)
	}
	return kc
}

type kNodeServiceClient struct {
	*kClient
}

func (p *kNodeServiceClient) SlaveOf(ctx context.Context, req *node.SlaveOfRequest, callOptions ...callopt.Option) (r *node.SlaveOfRespone, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.SlaveOf(ctx, req)
}

func (p *kNodeServiceClient) PSync(ctx context.Context, req *node.PSyncRequest, callOptions ...callopt.Option) (r *node.PSyncResponse, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.PSync(ctx, req)
}

func (p *kNodeServiceClient) OpLogEntry(ctx context.Context, req *node.LogEntryRequest, callOptions ...callopt.Option) (r *node.LogEntryRequest, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.OpLogEntry(ctx, req)
}

func (p *kNodeServiceClient) Ping(ctx context.Context, callOptions ...callopt.Option) (r *node.PingResponse, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.Ping(ctx)
}

func (p *kNodeServiceClient) Info(ctx context.Context, callOptions ...callopt.Option) (r *node.InfoResponse, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.Info(ctx)
}