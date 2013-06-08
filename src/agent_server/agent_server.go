package agent_server

import (
	"bufio"
	"code.google.com/p/vitess/go/relog"
	"fmt"
	"io"
	"leveldb"
	"net"
	"strconv"
	"sync"
	"time"
)

type server struct {
	s    *store
	lock *sync.RWMutex
	kt   map[string]chan action
}

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
	sv         *server
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
	if err != nil {
		panic(err)
	}

	defer listen_sock.Close()

	sv := new(server)
	store, err := newLeveldb()
	if err != nil {
		panic(err)
	}

	sv.s = store
	sv.kt = make(map[string]chan action)
	sv.lock = new(sync.RWMutex)
	defer store.db.Close()
	for {
		conn, err := listen_sock.Accept()
		if err != nil {
			relog.Warning("%s", err)
			continue
		}

		c := newConn(conn, sv)

		relog.Info("accept successed, client ip is %s", c.remoteAddr)

		go c.serve()
	}
	panic("not reached")
}

func newConn(rwc net.Conn, sv *server) (c *conn) {
	c = new(conn)
	c.remoteAddr = rwc.RemoteAddr().String()
	c.sv = sv
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

		relog.Error("panic serving %v: %v\n", c.remoteAddr, err)

		if c.rwc != nil {
			c.rwc.Close()
		}
		c.close()
	}()

	for {

		c.rwc.SetReadDeadline(time.Now().Add(agent_read_timeout))
		c.rwc.SetWriteDeadline(time.Now().Add(agent_write_timeout))

		req, err := readRequest(c.rw.Reader)

		if err != nil {
			msg := "CLIENT_ERROR"
			if err == io.EOF {
				break
			} else if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
				break
			}
			fmt.Fprintf(c.rwc, "%s %s\r\n", msg, err)
			continue
		}

		err = c.handle_request(req)
		if err != nil {
			relog.Error("handle requesr error(%s)", err)
			break
		}

		c.rw.Flush()
	}

	c.close()
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

	switch req.method {
	case "get":
		for _, key := range req.key {
			data, err := c.sv.s.db.Get([]byte(key), c.sv.s.ro)
			if err != nil {
				err := c.write_status("SERVER_ERROR")
				if err != nil {
					return err
				}
				continue
			}

			if data == nil {
				err := c.write_status("NOT_FOUND")
				return err
			}

			_, err = c.rw.WriteString(key)
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

			_, err = c.rw.Write(data)
			if err != nil {
				return err
			}

			_, err = c.rw.WriteString("\r\n")
			if err != nil {
				return err
			}
		}

		_, err := c.rw.WriteString("END\r\n")
		return err

	case "add":
		data, err := c.sv.s.db.Get([]byte(req.key[0]), c.sv.s.ro)
		if err != nil {
			err := c.write_status("SERVER_ERROR")
			if err != nil {
				return err
			}
			break
		}

		if data == nil {
			return c.set(req)
		}

		return c.write_status("NOT_STORED")
	case "replace":

		data, err := c.sv.s.db.Get([]byte(req.key[0]), c.sv.s.ro)
		if err != nil {
			err := c.write_status("SERVER_ERROR")
			if err != nil {
				return err
			}
			break
		}

		if data != nil {
			return c.set(req)
		}
		return c.write_status("NOT_STORED")

	case "set":
		return c.set(req)
	case "mset":
		batch := leveldb.New_writebatch()
		for i, key := range req.key {
			batch.Put([]byte(key), req.value[i])
		}
		c.sv.s.db.Write(c.sv.s.wo, batch)

		return c.write_status("STORED")
	case "delete":
		err := c.sv.s.db.Delete([]byte(req.key[0]), c.sv.s.wo)
		if err != nil {
			err := c.write_status("NOT_FOUND")
			return err
		}
		err = c.write_status("DELETED")
		return err

	case "touch":
		data, err := c.sv.s.db.Get([]byte(req.key[0]), c.sv.s.ro)
		if err != nil {
			err := c.write_status("SERVER_ERROR")
			if err != nil {
				return err
			}
			break
		}

		if data != nil {
			c.sv.lock.RLock()
			ac := c.sv.kt[req.key[0]]
			ac <- action{req.key[0], req.exptime}
			c.sv.lock.RUnlock()
			err = c.write_status("TOUCHED")
			return err
		}
		return c.write_status("NOT_FOUND")
	}
	err := c.write_status("ERROR")
	return err
}

func process_action(ac chan action, sv *server) {
	var a action
	var timer *time.Timer
	ch := make(chan bool, 2)
	defer func() {
		if timer != nil {
			timer.Stop()
		}
		sv.lock.Lock()
		delete(sv.kt, a.key)
		close(ac)
		sv.lock.Unlock()
		close(ch)
	}()
	for {
		select {
		case a = <-ac:
			if timer != nil {
				timer.Stop()
			}
			timer = time.AfterFunc(a.exptime, func() {
				sv.s.db.Delete([]byte(a.key), sv.s.wo)
				ch <- true
			})

		case <-ch:
			relog.Info("delete successed")
			return
		}
	}
}

func (c *conn) set(req *request) error {

	err := c.sv.s.db.Put([]byte(req.key[0]), req.value[0], c.sv.s.wo)
	if err != nil {
		err := c.write_status("SERVER_ERROR")
		return err
	}

	err = c.write_status("STORED")
	if req.exptime != 0 {
		ac := make(chan action)
		c.sv.kt[req.key[0]] = ac
		go process_action(ac, c.sv)
		ac <- action{req.key[0], req.exptime}
	}

	return err
}
