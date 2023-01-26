package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

const (
	// Decoding ...
	ARRAY byte = '*'
	BULK_STRING byte = '$'
	STRING byte = '+'

	// Encoding ...
	STRING_PREFIX = "+"
	CRLF          = "\r\n"
)

func encode(res string) string {
	return fmt.Sprintf("%s%s%s", STRING_PREFIX, res, CRLF)
}

func decode(r *bufio.Reader) ([]string, error) {
	b, err := r.ReadByte()
	if err != nil {
		return nil, err  
	}
	switch b {
	case ARRAY:
		return decodeArray(r)
	case STRING:
		return decodeString(r)
	case BULK_STRING:
		return decodeBulkString(r)
	default:
		return nil, fmt.Errorf("could not decode the stream")
	}
}

func decodeString(r *bufio.Reader) ([]string, error) {
	barr, err := validByteCount(r)
	if err != nil {
		return nil, err
	}
	return []string{string(barr)}, nil 
}

func decodeBulkString(r *bufio.Reader) ([]string, error) {
	barr, err := validByteCount(r)
	if err != nil {
		return nil, err 
	}
	count, err:= strconv.Atoi(string(barr))
	if err != nil {
		return nil, err 
	}
	buf := make([]byte, count+2)
	_, err = io.ReadFull(r, buf)
	if err != nil && err != io.EOF {
		return nil, err 
	}
	return []string{string(buf)}, nil 
}

func decodeArray(r *bufio.Reader) ([]string, error) {
	byteCounts, err := validByteCount(r)
	if err != nil {
		return nil, err
	}
	count, err:= strconv.Atoi(string(byteCounts))
	if err != nil {
		return nil, err 
	}
	vals := []string{}
	for i:=0; i<count; i++ {
		current, err := decode(r)
		if err == io.EOF {
			return vals, nil
		}
		if err != nil {
			return nil, err 
		}
		vals = append(vals, current...)
	}
	return vals, nil  
}

func validByteCount(r *bufio.Reader) ([]byte, error) {
	barr, err := r.ReadBytes('\n')
	if err != nil {
		return nil,err
	}
	return barr[:len(barr)-2], nil 
}

func handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		r := bufio.NewReader(conn)
		cmds, err := decode(r)
		if err != nil {
			conn.Write([]byte(encode("invaid request")))
		}
		if len(cmds) >= 1 {
			cmd := strings.ToUpper(cmds[0])
			fmt.Printf("fucking command: %s type :%T", cmd, cmd)
			switch  {
			case cmd == "PING":
				conn.Write([]byte(encode("PONG")))
			case cmd == "ECHO":
				conn.Write([]byte(encode(cmds[1])))
			}
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
