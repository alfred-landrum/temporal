package history

import (
	"sync"

	"go.temporal.io/server/common/ownership"
	"google.golang.org/grpc"
)

type (
	clientConnection[C any] struct {
		grpcClient C
		grpcConn   *grpc.ClientConn
	}

	rpcAddress string

	connectionPoolImpl[C any] struct {
		mu struct {
			sync.RWMutex
			conns map[rpcAddress]clientConnection[C]
		}

		historyShardWatcher ownership.HistoryShardWatcher
		rpcFactory          RPCFactory
		clientCtor          func(grpc.ClientConnInterface) C
	}

	// RPCFactory is a subset of the [go.temporal.io/server/common/rpc.RPCFactory] interface to make testing easier.
	RPCFactory interface {
		CreateHistoryGRPCConnection(rpcAddress string) *grpc.ClientConn
	}

	connectionPool[C any] interface {
		getOrCreateClientConn(addr rpcAddress) clientConnection[C]
		getAllClientConns() []clientConnection[C]
		resetConnectBackoff(clientConnection[C])
	}
)

func NewConnectionPool[C any](
	historyShardWatcher ownership.HistoryShardWatcher,
	rpcFactory RPCFactory,
	clientCtor func(grpc.ClientConnInterface) C,
) *connectionPoolImpl[C] {
	c := &connectionPoolImpl[C]{
		historyShardWatcher: historyShardWatcher,
		rpcFactory:          rpcFactory,
		clientCtor:          clientCtor,
	}
	c.mu.conns = make(map[rpcAddress]clientConnection[C])
	return c
}

func (c *connectionPoolImpl[C]) getOrCreateClientConn(addr rpcAddress) clientConnection[C] {
	c.mu.RLock()
	cc, ok := c.mu.conns[addr]
	c.mu.RUnlock()
	if ok {
		return cc
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if cc, ok = c.mu.conns[addr]; ok {
		return cc
	}
	grpcConn := c.rpcFactory.CreateHistoryGRPCConnection(string(addr))
	cc = clientConnection[C]{
		grpcClient: c.clientCtor(grpcConn),
		grpcConn:   grpcConn,
	}

	c.mu.conns[addr] = cc
	return cc
}

func (c *connectionPoolImpl[C]) getAllClientConns() []clientConnection[C] {
	owners := c.historyShardWatcher.Owners()

	var clientConns []clientConnection[C]
	for _, owner := range owners {
		cc := c.getOrCreateClientConn(rpcAddress(owner))
		clientConns = append(clientConns, cc)
	}

	return clientConns
}

func (c *connectionPoolImpl[C]) resetConnectBackoff(cc clientConnection[C]) {
	cc.grpcConn.ResetConnectBackoff()
}
