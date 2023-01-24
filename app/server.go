package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

const (
	STRING_PREFIX = "+"
	CRLF = "\r\n"
)

type commandDetails struct {
	command string
	count int 
	data interface{}
}

func respConversion(res, category string) string {
	switch category {
	case "string":
		return STRING_PREFIX+res+CRLF
	default:
		return STRING_PREFIX+res+CRLF
	}
}

func decodeArray(buf string) []string {
	cmds := []string{}
	arr := strings.Split(buf, CRLF)
	for _, ele := range arr {
		if strings.HasPrefix(ele, "*") || strings.HasPrefix(ele, "$"){
			continue
		} else {
			cmds = append(cmds, ele)
		}
	}
	return cmds
}

func decodeResp(buf string) ([]string) {
	cmds := []string{}
	if strings.HasPrefix(buf, "*") {
		cmds = decodeArray(buf)
	}
	return cmds
}




func handleConn(conn net.Conn) {
	defer conn.Close()
	req := make([]byte, 64)
	for {
		_, err := conn.Read(req)
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("error reading from incoming stream", err)
			break
		}
		cmds := decodeResp(string(bytes.TrimSpace(req)))
		if len(cmds) >=1 {
			switch strings.ToUpper(cmds[0]) {
			case "PING":
				conn.Write([]byte(respConversion("PONG", "string")))
			case "ECHO":
				conn.Write([]byte(respConversion(cmds[1], "string")))
			}
		}
		if err != nil {
			fmt.Println("error parsing resp")
		}
	}
}



func main() {	
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


// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
