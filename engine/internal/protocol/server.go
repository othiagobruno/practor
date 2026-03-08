package protocol

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
)

// ============================================================================
// JSON-RPC Server — Reads from stdin, writes to stdout
// ============================================================================

// Handler is a function that handles a JSON-RPC request.
type Handler func(ctx context.Context, params json.RawMessage) (interface{}, error)

// Server manages JSON-RPC communication over stdin/stdout.
type Server struct {
	handlers     map[string]Handler
	mu           sync.RWMutex
	writeMu      sync.Mutex
	reader       *bufio.Reader
	writer       io.Writer
	handlerSlots chan struct{}
}

// NewServer creates a new JSON-RPC server.
func NewServer() *Server {
	return &Server{
		handlers:     make(map[string]Handler),
		reader:       bufio.NewReader(os.Stdin),
		writer:       os.Stdout,
		handlerSlots: make(chan struct{}, 32),
	}
}

// RegisterHandler registers a handler for a JSON-RPC method.
func (s *Server) RegisterHandler(method string, handler Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[method] = handler
}

// Start begins listening for JSON-RPC requests on stdin.
func (s *Server) Start(ctx context.Context) error {
	// Signal readiness to the client
	s.writeResponse(&Response{
		JSONRPC: "2.0",
		ID:      0,
		Result:  map[string]string{"status": "ready", "version": "0.1.0"},
	})

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			line, err := s.reader.ReadBytes('\n')
			if err != nil {
				if err == io.EOF {
					return nil // Client closed the connection
				}
				return fmt.Errorf("read error: %w", err)
			}

			// Skip empty lines
			if len(line) <= 1 {
				continue
			}

			select {
			case s.handlerSlots <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}

			go func(message []byte) {
				defer func() { <-s.handlerSlots }()
				s.handleMessage(ctx, message)
			}(line)
		}
	}
}

func (s *Server) handleMessage(ctx context.Context, data []byte) {
	var req Request
	if err := json.Unmarshal(data, &req); err != nil {
		s.writeResponse(NewErrorResponse(0, ErrCodeParseError, "Parse error", err.Error()))
		return
	}

	if req.JSONRPC != "2.0" {
		s.writeResponse(NewErrorResponse(req.ID, ErrCodeInvalidRequest, "Invalid JSON-RPC version", nil))
		return
	}

	s.mu.RLock()
	handler, exists := s.handlers[req.Method]
	s.mu.RUnlock()

	if !exists {
		s.writeResponse(NewErrorResponse(req.ID, ErrCodeMethodNotFound,
			fmt.Sprintf("Method '%s' not found", req.Method), nil))
		return
	}

	result, err := handler(ctx, req.Params)
	if err != nil {
		s.writeResponse(NewErrorResponse(req.ID, ErrCodeInternal, err.Error(), nil))
		return
	}

	s.writeResponse(NewSuccessResponse(req.ID, result))
}

func (s *Server) writeResponse(resp *Response) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	data, err := json.Marshal(resp)
	if err != nil {
		// Last resort: write a minimal error
		fmt.Fprintf(s.writer, `{"jsonrpc":"2.0","id":0,"error":{"code":-32603,"message":"marshal error"}}`)
		fmt.Fprint(s.writer, "\n")
		return
	}
	fmt.Fprintf(s.writer, "%s\n", data)
}
