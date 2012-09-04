package agent_server

import (
	"code.google.com/p/vitess/go/logfile"
	"code.google.com/p/vitess/go/relog"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"syscall"
	"time"
)

const (
	Version                   = 0.1
	agent_read_timeout        = 30 * time.Second
	agent_write_timeout       = 30 * time.Second
	agent_read_buf_size       = 1024
	max_value_size            = 4096
	max_key_count             = 10
	noLimit             int64 = (1 << 63) - 1
)

type Config struct {
	Listen         string
	Read_timeout   time.Duration
	Write_timeout  time.Duration
	Value_max_size uint64
	Key_max_count  int32
}

var config = Config{
	Listen:         "127.0.0.1:8046",
	Read_timeout:   30 * time.Second,
	Write_timeout:  30 * time.Second,
	Value_max_size: 4096,
	Key_max_count:  10,
}

var (
	logfileName  = flag.String("logfile", "/dev/stderr", "base log file name")
	logLevel     = flag.String("log.level", "WARNING", "set log level")
	logFrequency = flag.Int64("logfile.frequency", 0,
		"rotation frequency in seconds")
	logMaxSize  = flag.Int64("logfile.maxsize", 0, "max file size in bytes")
	logMaxFiles = flag.Int64("logfile.maxfiles", 0, "max number of log files")

	maxOpenFds = flag.Uint64("max-open-fds", 32768, "max open file descriptors")
	gomaxprocs = flag.Int("gomaxprocs", 0, "Sets GOMAXPROCS")
)

func Init(logPrefix string) {
	if logPrefix != "" {
		logPrefix += " "
	}

	logPrefix += fmt.Sprintf("[%v]", os.Getpid())
	f, err := logfile.Open(*logfileName, *logFrequency, *logMaxSize, *logMaxFiles)
	if err != nil {
		panic(fmt.Sprintf("unable to open logfile %s: %v", *logfileName, err))
	}
	logger := relog.New(f, logPrefix+" ",
		log.Ldate|log.Lmicroseconds|log.Lshortfile, relog.LogNameToLogLevel(*logLevel))
	relog.SetLogger(logger)

	if *gomaxprocs != 0 {
		runtime.GOMAXPROCS(*gomaxprocs)
		relog.Info("set GOMAXPROCS = %v", *gomaxprocs)
	}

	fdLimit := &syscall.Rlimit{*maxOpenFds, *maxOpenFds}
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, fdLimit); err != nil {
		relog.Fatal("can't Setrlimit %#v: err %v", *fdLimit, err)
	} else {
		relog.Info("set max-open-fds = %v", *maxOpenFds)
	}
}

func Parse_config(config_file string) (cfg *Config) {
	unmarshalFile(config_file, &config)
	return &config
}

func unmarshalFile(name string, val interface{}) {
	if name != "" {
		data, err := ioutil.ReadFile(name)
		if err != nil {
			relog.Fatal("could not read %v: %v", val, err)
		}
		if err = json.Unmarshal(data, val); err != nil {
			relog.Fatal("could not read %s: %v", val, err)
		}
	}
	data, _ := json.MarshalIndent(val, "", "  ")
	relog.Info("config: %s\n", data)
}
