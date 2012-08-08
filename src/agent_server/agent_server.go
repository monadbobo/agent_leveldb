package agent_server

import (
	"log"
	"net"
	"bufio"
	"leveldb"
	"bytes"
	"fmt"
)

type conn struct {
	remoteAddr string
	rwc        net.Conn
	buf        *bufio.ReadWriter
	body       []byte
	db        *leveldb.Db
}

type response struct {
	conn          *conn
	status     string // e.g. "200 OK"
	statusCode int    // e.g. 200
	req *Request
}

func Run_server(laddr string) error {
	listen_sock, err := net.Listen("tcp", laddr)
	defer listen_sock.Close()
	if err != nil {
		panic(err)
	}

	for {
		conn, err := listen_sock.Accept()
		if err != nil {
			log.Println(err)
			return err
		}

		var db *leveldb.Db
		c, err := newConn(conn, db)
		if err != nil {
			continue
		}
		go c.serve()
	}
	panic("not reached")
}

func newConn(rwc net.Conn, db *leveldb.Db) (c *conn, err error) {
	c = new(conn)
	c.remoteAddr = rwc.RemoteAddr().String()
	c.db = db
	c.rwc = rwc
	c.body = make([]byte, SniffLen)
	br := bufio.NewReader(rwc)
	bw := bufio.NewWriter(rwc)
	c.buf = bufio.NewReadWriter(br, bw)
	return c, nil
}


func (c *conn) serve() {
	defer func() {
		err := recover()
		if err == nil {
			return
		}

		var buf bytes.Buffer
		fmt.Fprintf(&buf, "panic serving %v: %v\n", c.remoteAddr, err)
		log.Print(buf.String())

		if c.rwc != nil { // may be nil if connection hijacked
			c.rwc.Close()
		}
	}()

	for {
		w, err := c.readRequest()
		if err != nil {
			break
		}
		w.dumb()
	}
	c.close()
}


func (c *conn) readRequest() (w *response, err error) {
	var req *Request
	if req, err = ReadRequest(c.buf.Reader); err != nil {
		return nil, err
	}

	w = new(response)
	w.conn = c
	w.req = req
	c.body = c.body[:0]
	return w, nil
}


func (c *conn) close() {
	if c.rwc != nil {
		c.rwc.Close()
		c.rwc = nil
	}
}

func (res *response) dumb() {
	return
}
