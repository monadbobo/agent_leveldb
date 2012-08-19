package agent_server

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"net/textproto"
	"strconv"
	"strings"
)

type request struct {
	method    string
	key       string
	value_len int
	value     []byte
	noreply   bool
}

type badStringError struct {
	what string
	str  string
}

func (e *badStringError) Error() string { return fmt.Sprintf("%s %q", e.what, e.str) }

func (req *request) set_noreply(s string) error {
	if s == "noreply" {
		req.noreply = true
		return nil
	}
	return &badStringError{"invalid request", s}
}

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
		return req, &badStringError{"malformed agent request", s}
	}

	param_count := len(f)
	req.method, req.key = f[0], f[1]

	if req.method == "set" {
		var err error

		if param_count < 3 || param_count > 4 {
			return req, &badStringError{"invalid request param count", string(param_count)}
		}

		req.value_len, err = strconv.Atoi(f[2])
		if err != nil {
			return req, &badStringError{"data size is invalid", f[2]}
		}

		if param_count == 4 {
			err = req.set_noreply(f[3])
			if err != nil {
				return req, err
			}
		}

		if req.value_len > max_value_size {
			return req, &badStringError{"invalid data size", string(req.value_len)}
		}

		req.value, err = ioutil.ReadAll(io.LimitReader(b, int64(req.value_len)))
		if err != nil {
			return nil, err
		}
	} else {
		if param_count > 3 {
			return req, &badStringError{"invalid request param count", string(param_count)}
		}

		if param_count == 3 {
			err = req.set_noreply(f[2])
			if err != nil {
				return req, err
			}
		}
	}

	return req, nil
}
