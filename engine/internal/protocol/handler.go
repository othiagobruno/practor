package protocol

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/practor/practor-engine/internal/connector"
	"github.com/practor/practor-engine/internal/migration"
	"github.com/practor/practor-engine/internal/query"
	"github.com/practor/practor-engine/internal/schema"
)

// ============================================================================
// Handler — Routes JSON-RPC methods to the appropriate engine
// ============================================================================

// EngineHandler registers all handlers on a JSON-RPC server.
type EngineHandler struct {
	server      *Server
	queryEngine *query.Engine
	migEngine   *migration.Engine
	conn        connector.Connector
	schema      *schema.Schema
}

// NewEngineHandler creates a new EngineHandler and registers all methods.
func NewEngineHandler(
	server *Server,
	conn connector.Connector,
	s *schema.Schema,
) *EngineHandler {
	h := &EngineHandler{
		server:      server,
		queryEngine: query.NewEngine(conn, s),
		migEngine:   migration.NewEngine(conn, s),
		conn:        conn,
		schema:      s,
	}

	// Register all handlers
	server.RegisterHandler("query", h.handleQuery)
	server.RegisterHandler("mutation", h.handleMutation)
	server.RegisterHandler("schema.parse", h.handleSchemaParse)
	server.RegisterHandler("schema.validate", h.handleSchemaValidate)
	server.RegisterHandler("schema.getJSON", h.handleSchemaGetJSON)
	server.RegisterHandler("db.push", h.handleDbPush)
	server.RegisterHandler("db.executeRaw", h.handleExecuteRaw)
	server.RegisterHandler("db.queryRaw", h.handleQueryRaw)
	server.RegisterHandler("migrate.status", h.handleMigrateStatus)
	server.RegisterHandler("migrate.deploy", h.handleMigrateDeploy)
	server.RegisterHandler("migrate.dev", h.handleMigrateDev)
	server.RegisterHandler("transaction.begin", h.handleTransactionBegin)
	server.RegisterHandler("transaction.commit", h.handleTransactionCommit)
	server.RegisterHandler("transaction.rollback", h.handleTransactionRollback)
	server.RegisterHandler("transaction.query", h.handleTransactionQuery)
	server.RegisterHandler("transaction.mutation", h.handleTransactionMutation)
	server.RegisterHandler("pool.getStats", h.handlePoolGetStats)
	server.RegisterHandler("ping", h.handlePing)
	server.RegisterHandler("shutdown", h.handleShutdown)

	return h
}

// ============================================================================
// Query/Mutation handlers
// ============================================================================

func (h *EngineHandler) handleQuery(ctx context.Context, params json.RawMessage) (interface{}, error) {
	var qp QueryParams
	if err := json.Unmarshal(params, &qp); err != nil {
		return nil, fmt.Errorf("invalid query params: %w", err)
	}

	result, err := h.queryEngine.Execute(ctx, qp.Model, qp.Action, qp.Args)
	if err != nil {
		return nil, err
	}

	return &QueryResponse{Data: result}, nil
}

func (h *EngineHandler) handleMutation(ctx context.Context, params json.RawMessage) (interface{}, error) {
	var qp QueryParams
	if err := json.Unmarshal(params, &qp); err != nil {
		return nil, fmt.Errorf("invalid mutation params: %w", err)
	}

	result, err := h.queryEngine.Execute(ctx, qp.Model, qp.Action, qp.Args)
	if err != nil {
		return nil, err
	}

	return &MutationResponse{Data: result}, nil
}

// ============================================================================
// Schema handlers
// ============================================================================

func (h *EngineHandler) handleSchemaParse(ctx context.Context, params json.RawMessage) (interface{}, error) {
	var sp SchemaParams
	if err := json.Unmarshal(params, &sp); err != nil {
		return nil, fmt.Errorf("invalid schema params: %w", err)
	}

	input := sp.Schema
	if input == "" && sp.SchemaPath != "" {
		data, err := os.ReadFile(sp.SchemaPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read schema file: %w", err)
		}
		input = string(data)
	}

	parsed, err := schema.Parse(input)
	if err != nil {
		return &SchemaResponse{Valid: false, Errors: []string{err.Error()}}, nil
	}

	schema.ResolveFieldTypes(parsed)

	return &SchemaResponse{Schema: parsed, Valid: true}, nil
}

func (h *EngineHandler) handleSchemaValidate(ctx context.Context, params json.RawMessage) (interface{}, error) {
	var sp SchemaParams
	if err := json.Unmarshal(params, &sp); err != nil {
		return nil, fmt.Errorf("invalid schema params: %w", err)
	}

	input := sp.Schema
	if input == "" && sp.SchemaPath != "" {
		data, err := os.ReadFile(sp.SchemaPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read schema: %w", err)
		}
		input = string(data)
	}

	parsed, err := schema.Parse(input)
	if err != nil {
		return &SchemaResponse{Valid: false, Errors: []string{err.Error()}}, nil
	}

	schema.ResolveFieldTypes(parsed)
	validationErrors := schema.Validate(parsed)

	if len(validationErrors) > 0 {
		var errStrs []string
		for _, e := range validationErrors {
			errStrs = append(errStrs, e.Error())
		}
		return &SchemaResponse{Valid: false, Errors: errStrs}, nil
	}

	return &SchemaResponse{Schema: parsed, Valid: true}, nil
}

func (h *EngineHandler) handleSchemaGetJSON(ctx context.Context, params json.RawMessage) (interface{}, error) {
	return h.schema, nil
}

// ============================================================================
// Database handlers
// ============================================================================

func (h *EngineHandler) handleDbPush(ctx context.Context, params json.RawMessage) (interface{}, error) {
	var dp DbPushParams
	if err := json.Unmarshal(params, &dp); err != nil {
		return nil, fmt.Errorf("invalid db push params: %w", err)
	}

	// Read and parse the schema
	data, err := os.ReadFile(dp.SchemaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema: %w", err)
	}

	parsed, err := schema.Parse(string(data))
	if err != nil {
		return nil, fmt.Errorf("schema parse error: %w", err)
	}

	schema.ResolveFieldTypes(parsed)

	// Create a temporary query engine with the new schema
	qe := query.NewEngine(h.conn, parsed)

	if err := qe.PushSchema(ctx); err != nil {
		return nil, fmt.Errorf("db push error: %w", err)
	}

	return map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Successfully pushed %d models and %d enums", len(parsed.Models), len(parsed.Enums)),
	}, nil
}

func (h *EngineHandler) handleExecuteRaw(ctx context.Context, params json.RawMessage) (interface{}, error) {
	var p struct {
		Query string        `json:"query"`
		Args  []interface{} `json:"args"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, err
	}
	return h.queryEngine.ExecuteRaw(ctx, p.Query, p.Args)
}

func (h *EngineHandler) handleQueryRaw(ctx context.Context, params json.RawMessage) (interface{}, error) {
	var p struct {
		Query string        `json:"query"`
		Args  []interface{} `json:"args"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, err
	}
	return h.queryEngine.QueryRaw(ctx, p.Query, p.Args)
}

// ============================================================================
// Migration handlers
// ============================================================================

func (h *EngineHandler) handleMigrateStatus(ctx context.Context, params json.RawMessage) (interface{}, error) {
	if err := h.migEngine.EnsureMigrationsTable(ctx); err != nil {
		return nil, err
	}
	return map[string]string{"status": "ok"}, nil
}

func (h *EngineHandler) handleMigrateDeploy(ctx context.Context, params json.RawMessage) (interface{}, error) {
	var p MigrateDeployParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("invalid migrate deploy params: %w", err)
	}

	result, err := h.migEngine.Deploy(ctx, p.MigrationsDir)
	if err != nil {
		return nil, err
	}

	return &MigrateDeployResponse{
		Applied: result.Applied,
		Count:   result.Count,
		Message: result.Message,
	}, nil
}

func (h *EngineHandler) handleMigrateDev(ctx context.Context, params json.RawMessage) (interface{}, error) {
	var p MigrateDevParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("invalid migrate dev params: %w", err)
	}

	result, err := h.migEngine.CreateDevMigration(ctx, p.MigrationsDir, p.Name, p.SchemaPath)
	if err != nil {
		return nil, err
	}

	return &MigrateDevResponse{
		MigrationID: result.MigrationID,
		SQL:         result.SQL,
		FilePath:    result.FilePath,
		Message:     result.Message,
	}, nil
}

// ============================================================================
// Transaction handlers
// ============================================================================

func (h *EngineHandler) handleTransactionBegin(ctx context.Context, params json.RawMessage) (interface{}, error) {
	var p TransactionBeginParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("invalid transaction begin params: %w", err)
	}

	txID, err := h.queryEngine.BeginTransaction(ctx, p.IsolationLevel, p.Timeout)
	if err != nil {
		return nil, err
	}

	return &TransactionBeginResponse{TxID: txID}, nil
}

func (h *EngineHandler) handleTransactionCommit(ctx context.Context, params json.RawMessage) (interface{}, error) {
	var p TransactionIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("invalid transaction commit params: %w", err)
	}

	if err := h.queryEngine.CommitTransaction(p.TxID); err != nil {
		return nil, err
	}

	return map[string]bool{"success": true}, nil
}

func (h *EngineHandler) handleTransactionRollback(ctx context.Context, params json.RawMessage) (interface{}, error) {
	var p TransactionIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("invalid transaction rollback params: %w", err)
	}

	if err := h.queryEngine.RollbackTransaction(p.TxID); err != nil {
		return nil, err
	}

	return map[string]bool{"success": true}, nil
}

func (h *EngineHandler) handleTransactionQuery(ctx context.Context, params json.RawMessage) (interface{}, error) {
	var p TransactionActionParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("invalid transaction query params: %w", err)
	}

	result, err := h.queryEngine.ExecuteInTransaction(ctx, p.TxID, p.Model, p.Action, p.Args)
	if err != nil {
		return nil, err
	}

	return &QueryResponse{Data: result}, nil
}

func (h *EngineHandler) handleTransactionMutation(ctx context.Context, params json.RawMessage) (interface{}, error) {
	var p TransactionActionParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("invalid transaction mutation params: %w", err)
	}

	result, err := h.queryEngine.ExecuteInTransaction(ctx, p.TxID, p.Model, p.Action, p.Args)
	if err != nil {
		return nil, err
	}

	return &MutationResponse{Data: result}, nil
}

// ============================================================================
// Pool handlers
// ============================================================================

// handlePoolGetStats returns runtime connection pool statistics.
func (h *EngineHandler) handlePoolGetStats(ctx context.Context, params json.RawMessage) (interface{}, error) {
	return h.conn.GetPoolStats(), nil
}

// ============================================================================
// Utility handlers
// ============================================================================

func (h *EngineHandler) handlePing(ctx context.Context, params json.RawMessage) (interface{}, error) {
	if err := h.conn.Ping(ctx); err != nil {
		return nil, err
	}
	return map[string]string{"status": "pong"}, nil
}

func (h *EngineHandler) handleShutdown(ctx context.Context, params json.RawMessage) (interface{}, error) {
	h.conn.Disconnect(ctx)
	os.Exit(0)
	return nil, nil
}
