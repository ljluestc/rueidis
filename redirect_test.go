package rueidis

import (
    "context"
    "net"
    "strings"
    "testing"
    "time"
)

func TestRedirectSupport(t *testing.T) {
    // Start a mock server that sends a REDIRECT response
    addr := "localhost:6380"
    go runMockServer(addr)
    time.Sleep(1 * time.Second) // Wait for server to start

    // Create a client with EnableRedirect
    client, err := NewClient(ClientOption{
        InitAddress:       []string{addr},
        Standalone:        StandaloneOption{EnableRedirect: true},
        ForceSingleClient: true, // Ensure standalone mode
        Dialer:            net.Dialer{Timeout: 5 * time.Second},
        ConnWriteTimeout:  5 * time.Second,
    })
    if err != nil {
        t.Fatalf("Failed to create client: %v", err)
    }
    defer client.Close()

    // Test a GET command with redirect
    ctx := context.Background()
    resp := client.Do(ctx, client.B().Get().Key("key").Build())
    if resp.Error() != nil {
        t.Errorf("Command failed: %v", resp.Error())
    } else {
        t.Logf("Command succeeded: %v", resp.ToString())
    }

    // Test with redirect to a non-existent address
    resp = client.Do(ctx, client.B().Set().Key("key").Value("value").Build())
    if resp.Error() == nil || resp.Error().Error() != ErrRedirectFailed.Error() {
        t.Errorf("Expected ErrRedirectFailed, got: %v", resp.Error())
    }
}

func runMockServer(addr string) {
    listener, err := net.Listen("tcp", addr)
    if err != nil {
        panic(err)
    }
    defer listener.Close()

    for {
        conn, err := listener.Accept()
        if err != nil {
            return
        }
        go func(c net.Conn) {
            defer c.Close()
            buf := make([]byte, 1024)
            n, _ := c.Read(buf)
            cmd := string(buf[:n])

            // Handle CLIENT CAPA redirect
            if cmd == "CLIENT CAPA redirect\r\n" {
                c.Write([]byte("+OK\r\n"))
                return
            }

            // Respond with REDIRECT for GET, OK for SET
            if strings.HasPrefix(cmd, "GET ") {
                c.Write([]byte("*2\r\n$8\r\nREDIRECT\r\n$14\r\nlocalhost:6381\r\n"))
            } else {
                c.Write([]byte("+OK\r\n"))
            }
        }(conn)
    }
}