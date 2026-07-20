// Command glyphastore-interop is the Go helper for scripts/test-sdk-interop.sh.
package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/gpicchiarelli/GlyphaStore/sdk/go/client"
)

func main() {
	host := flag.String("host", "127.0.0.1", "server host")
	port := flag.Int("port", 0, "server port")
	keyHex := flag.String("key-hex", "", "key as hex")
	valueHex := flag.String("value-hex", "", "value as hex")
	expireAtNs := flag.Uint64("expire-at-ns", 0, "absolute expire_at_ns")
	tlsEnable := flag.Bool("tls", false, "opt-in TLS 1.3")
	tlsCA := flag.String("tls-ca", "", "PEM CA / trust anchor")
	tlsCert := flag.String("tls-cert", "", "client certificate for mTLS")
	tlsKey := flag.String("tls-key", "", "client private key for mTLS")
	serverName := flag.String("server-name", "", "SNI / hostname verification name")
	insecure := flag.Bool("insecure-skip-verify", false, "lab escape: skip cert/hostname verify")
	flag.Parse()
	if *port == 0 || flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: glyphastore-interop --port N <put|get|erase|pipeline-put-get> [tls flags...]")
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
