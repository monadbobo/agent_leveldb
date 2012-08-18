package agent_server

import (
	"bufio"
	"bytes"
	"fmt"
	"leveldb"
	"log"
	"net"
)

type response struct {
	req         *request
	status_code int
	status_text string
	body        []byte
	error       bool
}

type store struct {
	db   *leveldb.Db
	opts *leveldb.Options
	wo   *leveldb.Writeoptions
	ro   *leveldb.Readoptions
}

type conn struct {
	remoteAddr string
	rwc        net.Conn
	rw         *bufio.ReadWriter
	s          *store
}

func newLeveldb() (*store, error) {
	s := new(store)
	opts := leveldb.Create_options()
	opts.Set_create_if_missing(true)
	db, err := leveldb.Open("/var/tmp/l.db", opts)
	if err != nil {
		return nil, err
	}

	wo := leveldb.Create_write_options()
	ro := leveldb.Create_read_options()
	s.opts = opts
	s.wo = wo
	s.ro = ro
	s.db = db

	return s, nil
}

func Run_server(laddr string) error {
	listen_sock, err := net.Listen("tcp", laddr)
	defer listen_sock.Close()
	if err != nil {
		panic(err)
	}

	store, err := newLeveldb()
	if err != nil {
		panic(err)
	}

	defer store.db.Close()
	for {
		conn, err := listen_sock.Accept()
		if err != nil {
			log.Println(err)
			return err
		}

		c := newConn(conn, store)

		log.Print("accept successed, client ip is %s", c.remoteAddr)
		go c.serve()
	}
	panic("not reached")
}

func newConn(rwc net.Conn, s *store) (c *conn) {
	c = new(conn)
	c.remoteAddr = rwc.RemoteAddr().String()
	c.s = s
	c.rwc = rwc
	c.rw = bufio.NewReadWriter(bufio.NewReader(rwc), bufio.NewWriter(rwc))
	return c
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

		if c.rwc != nil {
			c.rwc.Close()
		}
	}()

	for {
		res, err := c.readRequest()
		if err != nil && res == nil {
			log.Println("read request error %s", err)
			break
		}

		if err == nil && res.req.noreply {
			continue
		}

		c.handle_request(res)
		err = c.output(res)
		if err != nil {
			log.Println("output failed %s", err)
		}
	}
	c.close()
}

func (c *conn) readRequest() (res *response, err error) {
	res = new(response)

	if res.req, err = readRequest(c.rw.Reader); err != nil {
		if res.req != nil {
			res.error = true
			return res, err
		}
		return nil, err
	}

	return res, nil
}

func (c *conn) close() {
	if c.rwc != nil {
		c.rwc.Close()
		c.rwc = nil
	}
}

func (c *conn) handle_request(res *response) {

	if res.error {
		res.status_code = 403
		res.status_text = "CLIENT_ERROR"
		return
	}

	switch {
	case res.req.method == "get":
		data, err := c.s.db.Get([]byte(res.req.key), c.s.ro)
		if err != nil {
			res.status_code = 500
			res.status_text = "SERVER_ERROR"
			res.error = true
			return
		}

		if data == nil {
			res.status_code = 404
			res.status_text = "NOT_FOUND"
		} else {
			res.status_code = 200
			res.status_text = "OK"
			res.body = data
		}

	case res.req.method == "set":
		err := c.s.db.Put([]byte(res.req.key), res.req.value, c.s.wo)
		if err != nil {
			res.status_code = 500
			res.status_text = "SERVER_ERROR"
			res.error = true
			return
		}

		res.status_code = 200
		res.status_text = "STORED"
	case res.req.method == "delete":
		err := c.s.db.Delete([]byte(res.req.key), c.s.wo)
		if err != nil {
			res.status_code = 500
			res.status_text = "SERVER_ERROR"
			res.error = true
			return
		}
		res.status_code = 200
		res.status_text = "DELETED"
	}
}

func (c *conn) output(res *response) error {
	_, err := c.rw.WriteString(res.status_text)
	if err != nil {
		return err
	}
	_, err = c.rw.WriteString("\r\n")
	if err != nil {
		return err
	}
	if len(res.body) != 0 {
		_, err = c.rw.Write(res.body)
		if err != nil {
			return err
		}
		_, err = c.rw.WriteString("\r\n")
		if err != nil {
			return err
		}
	}

	c.rw.Flush()
	return nil
}
