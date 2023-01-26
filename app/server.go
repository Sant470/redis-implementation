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
	"time"
)

const (
	// Decoding ...
	ARRAY       byte = '*'
	BULK_STRING byte = '$'
	STRING      byte = '+'
	// Encoding ...
	STRING_PREFIX = "+"
	CRLF          = "\r\n"
	EMPTY         = "$-1"
)

var mu sync.RWMutex
var datastore = map[string]string{}

func encode(resp string) string {
	return fmt.Sprintf("%s%s%s", STRING_PREFIX, resp, CRLF)
}

func encodeNonEmptyResponse() string {
	return fmt.Sprintf("%s%s", EMPTY, CRLF)
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
	count, err := strconv.Atoi(string(barr))
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
	count, err := strconv.Atoi(string(byteCounts))
	if err != nil {
		return nil, err
	}
	vals := []string{}
	for i := 0; i < count; i++ {
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
		return nil, err
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
			switch string(cmd) {
			case "PING":
				conn.Write([]byte(encode("PONG")))
			case "ECHO":
				conn.Write([]byte(encode(cmds[1])))
			case "SET":
				set(cmds[1:]...)
				conn.Write([]byte(encode("OK")))
			case "GET":
				val, nonempty := get(cmds[1])
				if !nonempty {
					conn.Write([]byte(encodeNonEmptyResponse()))
					break
				}
				conn.Write([]byte(encode(val)))
			default:
				conn.Write([]byte(encode("PONG")))
			}
		}
	}
}

func expire(key string, ttm time.Duration) {
	tick := time.NewTicker(ttm)
	<-tick.C
	mu.Lock()
	delete(datastore, key)
	mu.Unlock()
	tick.Stop()
}

// datastore related functions
func set(keys ...string) error {
	fmt.Println("keys: ", keys)
	if len(keys) >= 2 {
		mu.Lock()
		datastore[keys[0]] = keys[1]
		mu.Unlock()
		if len(keys) == 4 {
			ttm, _ := strconv.Atoi(keys[3])
			go expire(keys[0], time.Duration(1000*ttm))
		}
	}
	return fmt.Errorf("invalid args ...")
}

func get(key string) (string, bool) {
	mu.RLock()
	defer mu.RUnlock()
	val, OK := datastore[key]
	return val, OK
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
