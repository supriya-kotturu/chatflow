package ws

import (
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

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
	Conns         map[string]*WsClient
	Sem           chan struct{}
	Mu            sync.RWMutex
	ServerHost    string
	Port          string
	MessageBuffer int
	PoolMetrics
}

// NewWsClientPool creates a Pool with the given concurrency limit, server port, and per-connection send buffer size.
func NewWsClientPool(size int, serverHost string, port string, messageBuffer int) *Pool {
	return &Pool{
		Port:          port,
		ServerHost:    serverHost,
		Conns:         make(map[string]*WsClient),
		Mu:            sync.RWMutex{},
		Sem:           make(chan struct{}, size),
		MessageBuffer: messageBuffer,
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
		<-p.Sem
	}
}

func (p *Pool) getKey(userId, roomId string) string {
	return userId + ":" + roomId
}

// Reconnect closes the current connection for userId:roomId and opens a fresh
// one, keeping the semaphore slot so the pool size stays consistent.
// A small random jitter (0–200ms) is applied before dialing to desynchronize
// reconnect bursts when many users finish a sub-session at the same time.
func (p *Pool) Reconnect(userId, roomId string) (*WsClient, error) {
	time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond)
	key := p.getKey(userId, roomId)

	p.Mu.Lock()
	old, hadConn := p.Conns[key]
	if hadConn {
		old.Conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		_ = old.Close()
		delete(p.Conns, key)
	}
	p.Mu.Unlock()

	c, err := NewWsClient(p.MessageBuffer, p.ServerHost, p.Port, roomId)
	if err != nil {
		if hadConn {
			<-p.Sem // release the slot we were holding
		}
		p.FailedConnections.Add(1)
		return nil, err
	}

	p.Reconnections.Add(1)
	c.OwnerUser = userId

	p.Mu.Lock()
	p.Conns[key] = c
	p.Mu.Unlock()

	return c, nil
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

	c, err := NewWsClient(p.MessageBuffer, p.ServerHost, p.Port, roomId)

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
