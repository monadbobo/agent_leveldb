package agent_server

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"net/textproto"
	"strconv"
	"strings"
	"time"
)

type request struct {
	method    string
	key       []string
	exptime   time.Duration
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
	req.method = f[0]

	switch req.method {
	case "set", "add", "replace":
		var err error

		if param_count < 4 || param_count > 5 {
			return req, &badStringError{"invalid request param count", string(param_count)}
		}

		req.key = make([]string, 1)
		req.key[0] = f[1]
		req.exptime, err = time.ParseDuration(f[2])
		if err != nil {
			return req, &badStringError{"expire time is invalid", f[2]}
		}

		req.value_len, err = strconv.Atoi(f[3])
		if err != nil {
			return req, &badStringError{"data size is invalid", f[3]}
		}

		if param_count == 5 {
			err = req.set_noreply(f[4])
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

	case "get":
		if param_count > (2 + max_key_count) {
			return req, &badStringError{"invalid request param count", string(param_count)}
		}

		req.key = f[1:]

	case "delete":
		if param_count > 3 {
			return req, &badStringError{"invalid request param count", string(param_count)}
		}

		req.key = make([]string, 1)
		req.key[0] = f[1]
		if param_count == 3 {
			err = req.set_noreply(f[2])
			if err != nil {
				return req, err
			}
		}
	case "touch":
		if param_count > 3 {
			return req, &badStringError{"invalid request param count", string(param_count)}
		}
		req.key = make([]string, 1)
		req.key[0] = f[1]
		req.exptime, err = time.ParseDuration(f[2])
		if err != nil {
			return req, &badStringError{"expire time is invalid", f[2]}
		}
	}

	return req, nil
}
