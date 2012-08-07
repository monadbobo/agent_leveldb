package agent_server

import (
	"log"
	"net"
)

func Run_server(laddr string) error {
	listen_sock, err := net.Listen("tcp", laddr)
	defer listen_sock.Close()
	if err != nil {
		panic(err)
	}

	for {
		conn, err := listen_sock.Accept()
		if err != nil {
			log.Println(err)
			return err
		}

		go server_handle(conn)
	}
	panic("not reached")
}

func server_handle(conn net.Conn) {
	defer conn.Close()
	buf := make([] byte,1024)
	ret, err := conn.Read(buf[0:])
	if err != nil {
		log.Println(err)
		return
	}
	log.Println(ret)
	return
}
