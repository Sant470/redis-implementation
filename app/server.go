package main

import (
	"fmt"
	"net"
	"os"
)

const (
	STRING_PREFIX = "+"
	CRLF = "\r\n"
)

func respConversion (res, category string) string{
	switch category {
	case "string":
		return STRING_PREFIX+res+CRLF
	default:
		return STRING_PREFIX+res+CRLF
	}
}


func handleConn(conn net.Conn) {
	req := make([]byte, 1024)
	for {
		_, err := conn.Read(req)
		if err != nil {
			fmt.Println("error reading from incoming stream", err)
			break
		}
		conn.Write([]byte(respConversion("PONG", "string")))
	}
	conn.Close()
}



func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379", err)
		os.Exit(1)
	}
	for {
		// listening for all incomming connection ...
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConn(conn)
	}

}
