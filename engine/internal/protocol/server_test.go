package protocol

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestServerWaitsForInFlightHandlersOnEOF(t *testing.T) {
	request := `{"jsonrpc":"2.0","id":1,"method":"ping","params":{}}` + "\n"
	var output bytes.Buffer

	server := &Server{
		handlers:     make(map[string]Handler),
		reader:       bufio.NewReader(strings.NewReader(request)),
		writer:       &output,
		handlerSlots: make(chan struct{}, 1),
	}

	server.RegisterHandler("ping", func(ctx context.Context, params json.RawMessage) (interface{}, error) {
		time.Sleep(25 * time.Millisecond)
		return map[string]string{"status": "pong"}, nil
	})

	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected ready + success responses, got %d lines: %q", len(lines), output.String())
	}

	var resp Response
	if err := json.Unmarshal([]byte(lines[1]), &resp); err != nil {
		t.Fatalf("failed to parse handler response: %v", err)
	}

	if resp.ID != 1 {
		t.Fatalf("expected response id 1, got %d", resp.ID)
	}

	if resp.Error != nil {
		t.Fatalf("unexpected response error: %+v", resp.Error)
	}
}
