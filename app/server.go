package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
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

var mu sync.RWMutex
var datastore = map[string]string{}

func encode(resp string) string {
	return fmt.Sprintf("%s%s%s", STRING_PREFIX, resp, CRLF)
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
	return []string{string(bytes.TrimSpace(buf))}, nil 
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
		if err == io.EOF {
			break
		}
		if len(cmds) >= 1 {
			cmd := strings.ToUpper(cmds[0])
			switch string(cmd){
			case "PING":
				conn.Write([]byte(encode("PONG")))
			case "ECHO":
				conn.Write([]byte(encode(cmds[1])))
			case "SET":
				fmt.Println(" Set command key and values: ", cmds[1:])
				set(cmds[1], cmds[2])
				conn.Write([]byte(encode("OK")))
			case "GET":
				fmt.Println("Get Command key and values: ", cmds[1:])
				val, _ := get(cmds[1])
				conn.Write([]byte(encode(val)))
			default:
				conn.Write([]byte(encode("PONG")))
			}
		}
	}
}

// datastore related functions
func set(key, val string) error {
	mu.Lock()
	{
		datastore[key] = val
	}
	mu.Unlock()
	return nil 
}

func get(key string) (string, error) {
	mu.RLock()
	val := datastore[key]
	mu.RUnlock()
	return val, nil 
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

