package agent_server

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"leveldb"
	"log"
	"net"
	"strconv"
	"time"
)

type action struct {
	key     string
	exptime time.Duration
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
		c.close()
	}()

	for {
		req, err := readRequest(c.rw.Reader)

		if err != nil {
			msg := "CLIENT_ERROR"
			if err == io.EOF {
				break // Don't reply
			} else if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
				break // Don't reply
			}
			fmt.Fprintf(c.rwc, "%s %s\r\n", msg, err)
			continue
		}

		err = c.handle_request(req)
		if err != nil {
			log.Print("handle requesr error(%s)", err)
			break
		}

		c.rw.Flush()
	}
}

func (c *conn) close() {
	if c.rwc != nil {
		c.rwc.Close()
		c.rwc = nil
	}
}

func (c *conn) write_status(status string) error {
	_, err := c.rw.WriteString(status)
	if err != nil {
		return err
	}
	_, err = c.rw.WriteString("\r\n")
	return err
}

func (c *conn) handle_request(req *request) error {

	switch {
	case req.method == "get":
		data, err := c.s.db.Get([]byte(req.key), c.s.ro)
		if err != nil {
			err := c.write_status("SERVER_ERROR")
			return err
		}

		if data == nil {
			err := c.write_status("NOT_FOUND")
			return err
		}

		_, err = c.rw.WriteString(req.key)
		if err != nil {
			return err
		}

		_, err = c.rw.WriteString(" ")
		if err != nil {
			return err
		}

		_, err = c.rw.WriteString(strconv.Itoa(len(data)))
		if err != nil {
			return err
		}
		_, err = c.rw.WriteString("\r\n")
		if err != nil {
			return err
		}
		_, err = c.rw.WriteString("END\r\n")
		return err

	case req.method == "set":
		err := c.s.db.Put([]byte(req.key), req.value, c.s.wo)
		if err != nil {
			err := c.write_status("SERVER_ERROR")
			return err
		}

		err = c.write_status("STORED")
		if req.exptime != 0 {
			ac := make(chan action)
			go process_action(ac, c.s)
			ac <- action{req.key, req.exptime}
		}

		return err
	case req.method == "delete":
		err := c.s.db.Delete([]byte(req.key), c.s.wo)
		if err != nil {
			err := c.write_status("NOT_FOUND")
			return err
		}
		err = c.write_status("DELETED")
		return err
	}
	err := c.write_status("ERROR")
	return err
}

func process_action(ac chan action, s *store) {
	ch := make(chan bool, 2)
	a := <-ac
	timer := time.AfterFunc(a.exptime, func() {
		s.db.Delete([]byte(a.key), s.wo)
		ch <- true
	})
	defer timer.Stop()
	<-ch
}
