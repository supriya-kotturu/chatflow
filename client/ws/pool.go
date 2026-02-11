package client

import (
	"log"
	"sync"

	"github.com/gorilla/websocket"
)

type Pool struct {
	Port  string
	Conns map[string]*WsClient
	Sem   chan struct{}
	Mu    sync.RWMutex
}

func NewWsClientPool(size int, port string) *Pool {
	return &Pool{
		Port:  port,
		Conns: make(map[string]*WsClient),
		Mu:    sync.RWMutex{},
		Sem:   make(chan struct{}, size),
	}
}

func (p *Pool) CloseAll() {
	p.Mu.Lock()
	for k, c := range p.Conns {
		_ = c.Close()
		delete(p.Conns, k)
	}
	p.Mu.Unlock()
}

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

	c, err := NewWsClient(1024, p.Port, roomId)

	if err != nil {
		<-p.Sem
		return nil, err
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
