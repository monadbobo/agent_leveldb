package agent_server

import "time"

const (
	Version = 0.1
	Agent_read_timeout = 30 * time.Second
	Agent_write_timeout = 30 * time.Second
	Agent_read_buf_size = 1024
	SniffLen = 512
)


