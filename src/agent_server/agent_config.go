package agent_server

import "time"

const (
	Version                   = 0.1
	agent_read_timeout        = 30 * time.Second
	agent_write_timeout       = 30 * time.Second
	agent_read_buf_size       = 1024
	max_value_size            = 4096
	max_key_count             = 10
	noLimit             int64 = (1 << 63) - 1
)
