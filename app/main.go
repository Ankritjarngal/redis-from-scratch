package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	store  = make(map[string]string)
	expiry = make(map[string]time.Time)
	lists=make(map[string][]string)
	mu     sync.RWMutex // protects store and expiry
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "127.0.0.1:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379:", err)
		os.Exit(1)
	}
	defer l.Close()

	// background goroutine for expiry cleanup
	go func() {
		for {
			time.Sleep(1 * time.Second)
			now := time.Now()

			mu.Lock()
			for key, exp := range expiry {
				if now.After(exp) {
					delete(store, key)
					delete(expiry, key)
					fmt.Println("Expired key removed:", key)
				}
			}
			mu.Unlock()
		}
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		// Read array header: *<n>
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "*") {
			conn.Write([]byte("-ERR Protocol error\r\n"))
			return
		}

		// parse array length
		var n int
		fmt.Sscanf(line, "*%d", &n)

		parts := make([]string, 0, n)
		for i := 0; i < n; i++ {
			// skip bulk string header: $<len>
			_, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			// read actual string
			part, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			parts = append(parts, strings.TrimSpace(part))
		}

		if len(parts) == 0 {
			continue
		}

		command := strings.ToUpper(parts[0])

		switch command {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))

		case "SET":
			if len(parts) == 3 {
				key := parts[1]
				value := parts[2]

				mu.Lock()
				store[key] = value
				delete(expiry, key) // reset expiry if re-set
				mu.Unlock()

				conn.Write([]byte("+OK\r\n"))

			} else if len(parts) == 5 && strings.ToUpper(parts[3]) == "PX" {
				key := parts[1]
				value := parts[2]
				expiryMs := parts[4]

				ms, err := strconv.Atoi(expiryMs)
				if err != nil {
					conn.Write([]byte("-ERR invalid expire time\r\n"))
					return
				}

				mu.Lock()
				store[key] = value
				expiry[key] = time.Now().Add(time.Millisecond * time.Duration(ms))
				mu.Unlock()

				conn.Write([]byte("+OK\r\n"))
			}else if len(parts) == 5 && strings.ToUpper(parts[3]) == "EX" {
				key := parts[1]
				value := parts[2]
				expirySec := parts[4]

				sec, err := strconv.Atoi(expirySec)
				if err != nil {
					conn.Write([]byte("-ERR invalid expire time\r\n"))
					return
				}


				mu.Lock()
				store[key] = value
				expiry[key] = time.Now().Add(time.Second * time.Duration(sec))
				mu.Unlock()
				conn.Write([]byte("+OK\r\n") )

			}else {
				conn.Write([]byte("-ERR wrong number of arguments for 'SET'\r\n"))
			}

		case "GET":
			if len(parts) >= 2 {
				key := parts[1]

				mu.RLock()
				val, ok := store[key]
				exp, hasExpiry := expiry[key]
				mu.RUnlock()

				if !ok {
					conn.Write([]byte("$-1\r\n"))
					continue
				}
				if hasExpiry && time.Now().After(exp) {
					// expired → delete under write lock
					mu.Lock()
					delete(store, key)
					delete(expiry, key)
					mu.Unlock()
					conn.Write([]byte("$-1\r\n"))
					continue
				}

				reply := fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
				conn.Write([]byte(reply))
			} else {
				conn.Write([]byte("-ERR wrong number of arguments for 'GET'\r\n"))
			}

		case "ECHO":
			if len(parts) >= 2 {
				arg := parts[1]
				reply := fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
				conn.Write([]byte(reply))
			} else {
				conn.Write([]byte("-ERR wrong number of arguments for 'ECHO'\r\n"))
			}
		case "RPUSH":
			if len(parts)>=3{
				name:=parts[1]
				values:=parts[2:]
				mu.Lock()
				for _, value := range values {
					lists[name] = append(lists[name], value)
				}
				length:=len(lists[name])
				mu.Unlock()
				conn.Write([]byte(fmt.Sprintf(":%d\r\n",length)))
				
				
			}
		case "LPUSH":
			if len(parts)>=3{
				name:=parts[1]
				values:=parts[2:]
				mu.Lock()
				for _, value := range values {
					lists[name] = append([]string{value}, lists[name]...)
				}
				length:=len(lists[name])
				mu.Unlock()
				conn.Write([]byte(fmt.Sprintf(":%d\r\n",length)))
			}
		case "LRANGE":
			if len(parts) >= 4 {
				key := parts[1]
				start, _ := strconv.Atoi(parts[2])
				end, _ := strconv.Atoi(parts[3])
		
				mu.RLock()
				list, exists := lists[key]
				mu.RUnlock()
		
				if !exists || len(list) == 0 {
					conn.Write([]byte("*0\r\n"))
					break
				}
		
				n := len(list)
		
				// Handle negative start
				if start < 0 {
					start = n + start
				}
				if start < 0 {
					start = 0
				}
		
				// Handle negative end
				if end < 0 {
					end = n + end
				}
				if end < 0 {
					end = 0
				}
				if end >= n {
					end = n - 1
				}
		
				// If range invalid → empty array
				if start > end || start >= n {
					conn.Write([]byte("*0\r\n"))
					break
				}
		
				// Number of elements
				count := end - start + 1
				conn.Write([]byte(fmt.Sprintf("*%d\r\n", count)))
		
				// Write elements
				for i := start; i <= end; i++ {
					elem := list[i]
					conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(elem), elem)))
				}
			} else {
				conn.Write([]byte("-ERR wrong number of arguments for 'LRANGE'\r\n"))
			}
		case "LLEN":
			if len(parts)>=2{
				name:=parts[1]
				mu.RLock()
				length:=len(lists[name])
				mu.RUnlock()
				conn.Write([]byte(fmt.Sprintf(":%d\r\n",length)))
			}
		case "LPOP":
			if len(parts) >= 2 {
				name := parts[1]
				count := 1
				if len(parts) >= 3 {
					c, err := strconv.Atoi(parts[2])
					if err == nil && c > 0 {
						count = c
					} else {
						conn.Write([]byte("*0\r\n")) 
						break
					}
				}
		
				mu.Lock()
				list, exists := lists[name]
				if !exists || len(list) == 0 {
					mu.Unlock()
					conn.Write([]byte("$-1\r\n"))
					break
				}
		
				if count > len(list) {
					count = len(list)
				}
		
				values := list[:count]
				lists[name] = list[count:]
				mu.Unlock()
		
				if count == 1 {
					val := values[0]
					conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)))
				} else {
					conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(values))))
					for _, v := range values {
						conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v)))
					}
				}
			}
		
		case "RPOP":
			if len(parts) >= 2 {
				name := parts[1]
				count := 1
				if len(parts) >= 3 {
					c, err := strconv.Atoi(parts[2])
					if err == nil && c > 0 {
						count = c
					} else {
						conn.Write([]byte("*0\r\n")) 
						break
					}
				}
		
				mu.Lock()
				list, exists := lists[name]
				if !exists || len(list) == 0 {
					mu.Unlock()
					conn.Write([]byte("$-1\r\n"))
					break
				}
		
				if count > len(list) {
					count = len(list)
				}
		
				values := list[len(list)-count:]
				lists[name] = list[:len(list)-count]
				mu.Unlock()
		
				if count == 1 {
					val := values[0]
					conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)))
				} else {
					conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(values))))
					for _, v := range values {
						conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v)))
					}
				}
			}
		
		default:
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}
