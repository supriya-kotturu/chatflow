package client

import (
	"log"
	"sync"
	"sync/atomic"

	"github.com/gorilla/websocket"
)

type PoolMetrics struct {
	TotalConnections   atomic.Int64
	Reconnections      atomic.Int64
	FailedConnections  atomic.Int64
	SuccessfulMessages atomic.Int64
	FailedMessages     atomic.Int64
}

// Pool manages a bounded set of WebSocket connections keyed by "userId:roomId".
// A semaphore limits the total number of concurrent connections.
type Pool struct {
	Conns      map[string]*WsClient
	Sem        chan struct{}
	Mu         sync.RWMutex
	ServerHost string
	Port       string
	PoolMetrics
}

// NewWsClientPool creates a Pool with the given concurrency limit and server port.
func NewWsClientPool(size int, serverHost string, port string) *Pool {
	return &Pool{
		Port:       port,
		ServerHost: serverHost,
		Conns:      make(map[string]*WsClient),
		Mu:         sync.RWMutex{},
		Sem:        make(chan struct{}, size),
	}
}

// CloseAll closes and removes all connections in the pool.
func (p *Pool) CloseAll() {
	p.Mu.Lock()
	for k, c := range p.Conns {
		_ = c.Close()
		delete(p.Conns, k)
	}
	p.Mu.Unlock()
}

// Remove sends a close frame, closes the connection, and releases the semaphore slot.
func (p *Pool) Remove(userId string, roomId string) {
	p.Mu.Lock()

	key := p.getKey(userId, roomId)
	c, ok := p.Conns[key]
	delete(p.Conns, key)
	p.Mu.Unlock()

	if ok {
		c.Conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		_ = c.Close()
		c.CloseSend()
		<-p.Sem
	}
}

func (p *Pool) getKey(userId, roomId string) string {
	return userId + ":" + roomId
}

// GetOrCreateNewWsClient returns an existing connection or creates a new one.
// Uses double-check locking to avoid duplicate connections under concurrency.
func (p *Pool) GetOrCreateNewWsClient(userId string, roomId string) (*WsClient, error) {
	key := p.getKey(userId, roomId)
	p.Mu.RLock()

	if conn, ok := p.Conns[key]; ok {
		p.Mu.RUnlock()
		log.Printf("Reusing existing connection for user %s in room %s", userId, roomId)
		return conn, nil
	}

	p.Mu.RUnlock()
	p.Sem <- struct{}{}

	c, err := NewWsClient(1024, p.ServerHost, p.Port, roomId)

	if err != nil {
		p.FailedConnections.Add(1)
		<-p.Sem
		return nil, err
	}
	p.TotalConnections.Add(1)
	if c.Retries > 2 {
		p.Reconnections.Add(1)
	}
	c.OwnerUser = userId

	p.Mu.Lock()
	if existingConn, ok := p.Conns[key]; ok {
		p.Mu.Unlock()
		_ = c.Close()
		<-p.Sem
		return existingConn, nil
	}

	p.Conns[key] = c
	p.Mu.Unlock()

	return c, nil
}
