package agent_server

import (
	"bufio"
	"fmt"
	"net/textproto"
	"io"
	"strings"
	"strconv"
	"io/ioutil"
)

type request struct {
	method string
	key string
	value_len int
	value []byte
}

type badStringError struct {
	what string
	str  string
}


func (e *badStringError) Error() string { return fmt.Sprintf("%s %q", e.what, e.str) }


func readRequest(b *bufio.Reader) (req *request, err error) {

	tp := textproto.NewReader(b)
	req = new(request)

	var s string
	if s, err = tp.ReadLine(); err != nil {
		return nil, err
	}
	defer func() {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
	}()

	var f []string
	if f = strings.Split(s, " "); len(f) < 2 {
		return nil, &badStringError{"malformed agent request", s}
	}

	req.method, req.key = f[0], f[1]

	if req.method == "set" {
		var err error
		req.value_len, err = strconv.Atoi(f[2])
		if err != nil {
			return nil, &badStringError{"data size is invalid", ""}
		}

		req.value, err = ioutil.ReadAll(io.LimitReader(b, int64(req.value_len)))
		if err != nil {
			return nil, nil
		}
	}

	return req, nil
}
