// Command glyphastore-interop is the Go helper for scripts/test-sdk-interop.sh.
package main

import (
	"bytes"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/gpicchiarelli/GlyphaStore/sdk/go/client"
	"github.com/gpicchiarelli/GlyphaStore/sdk/go/protocol"
)

func main() {
	host := flag.String("host", "127.0.0.1", "server host")
	port := flag.Int("port", 0, "server port")
	keyHex := flag.String("key-hex", "", "key as hex")
	valueHex := flag.String("value-hex", "", "value as hex")
	dest := flag.String("dest", "", "UTF-8 destination path for backup")
	expireAtNs := flag.Uint64("expire-at-ns", 0, "absolute expire_at_ns")
	tlsEnable := flag.Bool("tls", false, "opt-in TLS 1.3")
	tlsCA := flag.String("tls-ca", "", "PEM CA / trust anchor")
	tlsCert := flag.String("tls-cert", "", "client certificate for mTLS")
	tlsKey := flag.String("tls-key", "", "client private key for mTLS")
	serverName := flag.String("server-name", "", "SNI / hostname verification name")
	insecure := flag.Bool("insecure-skip-verify", false, "lab escape: skip cert/hostname verify")
	burst := flag.Int("burst", 32, "PUT attempts for burst-expect-overloaded")
	flag.Parse()
	if *port == 0 || flag.NArg() != 1 || *burst < 1 || *burst > 10000 {
		fmt.Fprintln(os.Stderr, "usage: glyphastore-interop --port N <put|get|erase|backup|pipeline-put-get|expect-not-found|expect-permission-denied|burst-expect-overloaded|expect-frame-limit> [tls flags...]")
		os.Exit(2)
	}
	command := flag.Arg(0)
	key, err := parseHex(*keyHex)
	if err != nil {
		fail(err)
	}
	value, err := parseHex(*valueHex)
	if err != nil {
		fail(err)
	}

	cfg := client.Config{
		Host: *host,
		Port: *port,
		TLS: client.TLSConfig{
			Enable:             *tlsEnable,
			CAFile:             *tlsCA,
			CertFile:           *tlsCert,
			KeyFile:            *tlsKey,
			ServerName:         *serverName,
			InsecureSkipVerify: *insecure,
		},
	}
	c, err := client.Connect(cfg)
	if err != nil {
		fail(err)
	}
	defer c.Close()

	switch command {
	case "put":
		result := c.Put(key, value, *expireAtNs)
		if !result.Committed() {
			fail(fmt.Errorf("put not committed: %v", result.Err))
		}
	case "get":
		got, err := c.Get(key)
		if err != nil {
			fail(err)
		}
		fmt.Println(hex.EncodeToString(got))
	case "erase":
		result := c.Erase(key)
		if !result.Committed() {
			fail(fmt.Errorf("erase not committed: %v", result.Err))
		}
	case "backup":
		if *dest == "" {
			fail(fmt.Errorf("backup requires --dest PATH"))
		}
		report, err := c.Backup(*dest)
		if err != nil {
			fail(err)
		}
		fmt.Print(string(report))
	case "pipeline-put-get":
		responses, err := c.ExecutePipeline([]client.PipelineRequest{
			{Opcode: client.PipelinePut, Key: key, Value: value},
			{Opcode: client.PipelineGet, Key: key},
		})
		if err != nil {
			fail(err)
		}
		if len(responses) != 2 || !responses[0].Succeeded() || !responses[1].Succeeded() {
			fail(fmt.Errorf("pipeline outcomes failed"))
		}
		if string(responses[1].Value) != string(value) {
			fail(fmt.Errorf("pipeline value mismatch"))
		}
		fmt.Println(hex.EncodeToString(responses[1].Value))
	case "expect-not-found":
		_, err := c.Get(key)
		structured, ok := err.(*client.Error)
		if !ok || structured.Category != client.CategoryNotFound ||
			structured.Retryability != client.RetryNewAttempt {
			fail(fmt.Errorf("GET did not produce structured not_found: %v", err))
		}
	case "expect-permission-denied":
		result := c.Put(key, value, *expireAtNs)
		structured, ok := result.Err.(*client.Error)
		if result.Outcome != client.MutationRejected || !ok ||
			structured.Category != client.CategoryPermissionDenied ||
			structured.Retryability != client.RetryNever {
			fail(fmt.Errorf("PUT did not produce structured permission_denied: %v", result.Err))
		}
	case "burst-expect-overloaded":
		for index := 0; index < *burst; index++ {
			result := c.Put(key, value, *expireAtNs)
			structured, ok := result.Err.(*client.Error)
			if result.Outcome == client.MutationRejected && ok &&
				structured.Category == client.CategoryOverloaded &&
				structured.Retryability == client.RetryNever {
				return
			}
			if !result.Committed() {
				fail(fmt.Errorf("PUT produced an unexpected result before OVERLOADED: %v", result.Err))
			}
		}
		fail(fmt.Errorf("burst of %d PUTs did not observe OVERLOADED", *burst))
	case "expect-frame-limit":
		value := bytes.Repeat([]byte{0xA5}, protocol.MaxFrameBytes)
		result := c.Put([]byte("limit"), value, 0)
		structured, ok := result.Err.(*client.Error)
		if result.Outcome != client.MutationRejected || !ok ||
			structured.Category != client.CategoryInvalidArgument || structured.BytesSent != 0 ||
			structured.Retryability != client.RetryNever {
			fail(fmt.Errorf("oversized PUT did not produce the expected local rejection: %v", result.Err))
		}
	default:
		fail(fmt.Errorf("unknown command %q", command))
	}
}

func parseHex(text string) ([]byte, error) {
	cleaned := strings.Join(strings.Fields(text), "")
	if cleaned == "" {
		return nil, nil
	}
	if len(cleaned)%2 != 0 {
		return nil, fmt.Errorf("odd hex length")
	}
	return hex.DecodeString(cleaned)
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	os.Exit(1)
}
