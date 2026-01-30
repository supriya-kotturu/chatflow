package client

import "log"

type Pool struct {
	clients chan *WsClient
	all     []*WsClient
}

func NewWsClientPool(size int, port string) (*Pool, error) {
	ch := make(chan *WsClient, size)
	var all []*WsClient

	for i := 0; i < size; i++ {
		c, err := NewWsClient(port)
		if err != nil {
			// close connections what we already created
			for _, cc := range all {
				_ = cc.Close()
			}
			return nil, err
		}
		ch <- c
		all = append(all, c)
	}
	return &Pool{clients: ch, all: all}, nil
}

func (p *Pool) Get() *WsClient {
	c := <-p.clients
	c.Reset()
	return c
}
func (p *Pool) Put(c *WsClient) { p.clients <- c }
func (p *Pool) CloseAll() {
	for _, c := range p.all {
		if err := c.Close(); err != nil {
			log.Panicln("close error: ", err)
		}
	}
	close(p.clients)
}
