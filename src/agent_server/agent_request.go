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

type Request struct {
	Method string
	Key string
	value_len int
	Body io.ReadCloser
}

type badStringError struct {
	what string
	str  string
}


func (e *badStringError) Error() string { return fmt.Sprintf("%s %q", e.what, e.str) }


func ReadRequest(b *bufio.Reader) (req *Request, err error) {

	tp := textproto.NewReader(b)
	req = new(Request)

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

	req.Method, req.Key = f[0], f[1]

	if req.Method == "set" {
		var err error
		req.value_len, err = strconv.Atoi(f[2])
		if err != nil {
			return nil, &badStringError{"data size is invalid", ""}
		}

		ioutil.ReadAll(req.Body)
	}

	return req, nil
}
