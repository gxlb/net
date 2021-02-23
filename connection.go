package net

import (
	"net"
	"sync"
	"time"
)

type Connection struct {
	id          int
	manager     *connectionManager
	conn        *net.TCPConn
	wbuf        wbuffer
	rbuf        rbuffer
	wg          sync.WaitGroup
	chevent     chan struct{}
	chexit      chan struct{}
	cherr       chan error
	nonBlocking bool
	epoch       time.Time
}

func newConnection(id int, manager *connectionManager, conn *net.TCPConn, param *BufferParam, coder *stdCoder) *Connection {
	if param.RecvSize <= 0 || param.SendSize <= 0 {
		panic("invalid buffer size")
	}
	c := &Connection{
		id:      id,
		manager: manager,
		conn:    conn,
		chevent: make(chan struct{}, 1),
		chexit:  make(chan struct{}),
		cherr:   make(chan error, 1),
		epoch:   time.Now(),
		rbuf: rbuffer{
			chunksize: param.RecvSize,
			coder:     coder,
		},
		wbuf: wbuffer{
			maxsize:   param.SendMaxSize,
			chunksize: param.SendSize,
			coder:     coder,
		},
	}
	c.wg.Add(1)
	go c.send()
	return c
}

func (c *Connection) Close() {
	close(c.chexit)
	c.conn.Close()
	c.wg.Wait()
}

func (c *Connection) SetNonBlocking(nonBlocking bool) {
	c.nonBlocking = nonBlocking
}

func (c *Connection) notifyErr(err error) {
	select {
	case c.cherr <- err:
	default:
	}
}

func (c *Connection) send() {
	defer func() {
		if _, ok := c.manager.removeClient(c.id); ok {
			c.conn.Close()
		}
		c.wg.Done()
	}()
	for {
		select {
		case <-c.chexit:
			return
		case err := <-c.cherr:
			logger.Printf("shutdown client[%s]: %s", c.conn.RemoteAddr().String(), err)
			return
		case <-c.chevent:
			c.wbuf.WriteTo(c.conn)
		}
	}
}

func (c *Connection) Flush() {
	if len(c.chevent) > 0 {
		return
	}
	select {
	case c.chevent <- struct{}{}:
	default:
	}
}

func (c *Connection) NeedFlushing() bool {
	// quick check
	return c.wbuf.dirty && !c.wbuf.locked
}

func (c *Connection) Read() ([]byte, error) {
	return c.rbuf.ReadFrom(c.conn)
}

func (c *Connection) ReadBatch(list [][]byte) (int, error) {
	return c.rbuf.ReadBatch(c.conn, list)
}

func (c *Connection) Write(data []byte) error {
	if err := c.wbuf.Push(data); err != nil {
		c.notifyErr(err)
		return err
	}
	if c.nonBlocking {
		return nil
	}
	return c.wbuf.WriteTo(c.conn)
}

func (c *Connection) WriteBatch(msgList [][]byte) error {
	if err := c.wbuf.PushBatch(msgList); err != nil {
		c.notifyErr(err)
		return err
	}
	if c.nonBlocking {
		return nil
	}
	return c.wbuf.WriteTo(c.conn)
}

func (c *Connection) WriteParts(parts [][]byte) error {
	if err := c.wbuf.PushParts(parts); err != nil {
		c.notifyErr(err)
		return err
	}
	if c.nonBlocking {
		return nil
	}
	return c.wbuf.WriteTo(c.conn)
}

func (c *Connection) RemoteAddr() *net.TCPAddr {
	return c.conn.RemoteAddr().(*net.TCPAddr)
}

func (c *Connection) LocalAddr() *net.TCPAddr {
	return c.conn.LocalAddr().(*net.TCPAddr)
}

func (c *Connection) SetReadDeadline(timeout time.Time) error {
	return c.conn.SetReadDeadline(timeout)
}

func (c *Connection) SetWriteDeadline(timeout time.Time) error {
	return c.conn.SetWriteDeadline(timeout)
}
