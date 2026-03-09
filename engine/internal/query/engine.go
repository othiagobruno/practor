package query

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/practor/practor-engine/internal/connector"
	"github.com/practor/practor-engine/internal/schema"
)

// ============================================================================
// Query Engine — Central dispatcher for all query operations
// ============================================================================

// Engine is the central query engine that processes requests.
type Engine struct {
	connector connector.Connector
	schema    *schema.Schema
	builder   *Builder
	txStore   map[string]*transactionEntry
	txMu      sync.RWMutex
}

type transactionEntry struct {
	tx    *sql.Tx
	timer *time.Timer
}

func (e *Engine) newBuilder() *Builder {
	return NewBuilder(e.builder.dialect, e.schema)
}

// NewEngine creates a new query Engine.
func NewEngine(conn connector.Connector, s *schema.Schema) *Engine {
	dialect := string(conn.GetDialect())
	return &Engine{
		connector: conn,
		schema:    s,
		builder:   NewBuilder(dialect, s),
		txStore:   make(map[string]*transactionEntry),
	}
}

// Execute processes a query request and returns the result.
func (e *Engine) Execute(ctx context.Context, model string, action string, args map[string]interface{}) (interface{}, error) {
	switch action {
	case "findMany":
		return e.executeFindMany(ctx, model, args)
	case "findUnique":
		return e.executeFindUnique(ctx, model, args)
	case "findFirst":
		return e.executeFindFirst(ctx, model, args)
	case "findUniqueOrThrow":
		return e.executeFindUniqueOrThrow(ctx, model, args)
	case "findFirstOrThrow":
		return e.executeFindFirstOrThrow(ctx, model, args)
	case "create":
		return e.executeCreate(ctx, model, args)
	case "createMany":
		return e.executeCreateMany(ctx, model, args)
	case "update":
		return e.executeUpdate(ctx, model, args)
	case "updateMany":
		return e.executeUpdateMany(ctx, model, args)
	case "delete":
		return e.executeDelete(ctx, model, args)
	case "deleteMany":
		return e.executeDeleteMany(ctx, model, args)
	case "upsert":
		return e.executeUpsert(ctx, model, args)
	case "count":
		return e.executeCount(ctx, model, args)
	case "aggregate":
		return e.executeAggregate(ctx, model, args)
	case "groupBy":
		return e.executeGroupBy(ctx, model, args)
	case "findManyPaginated":
		return e.executeFindManyPaginated(ctx, model, args)
	case "findManyCursorPaginated":
		return e.executeFindManyCursorPaginated(ctx, model, args)
	default:
		return nil, fmt.Errorf("unknown action '%s'", action)
	}
}

// ============================================================================
// Query execution methods
// ============================================================================

func (e *Engine) executeFindMany(ctx context.Context, model string, args map[string]interface{}) (interface{}, error) {
	q, err := e.newBuilder().BuildFindMany(model, args)
	if err != nil {
		return nil, err
	}

	rows, err := e.connector.Query(ctx, q.SQL, q.Args...)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	result, err := connector.ScanRows(rows)
	if err != nil {
		return nil, fmt.Errorf("scan error: %w", err)
	}

	// Resolve relations (include / nested select)
	if err := e.resolveRelationsForRows(ctx, model, result.Rows, args); err != nil {
		return nil, fmt.Errorf("relation resolve error: %w", err)
	}

	return result.Rows, nil
}

func (e *Engine) executeFindUnique(ctx context.Context, model string, args map[string]interface{}) (interface{}, error) {
	q, err := e.newBuilder().BuildFindUnique(model, args)
	if err != nil {
		return nil, err
	}

	rows, err := e.connector.Query(ctx, q.SQL, q.Args...)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	result, err := connector.ScanRows(rows)
	if err != nil {
		return nil, fmt.Errorf("scan error: %w", err)
	}

	if len(result.Rows) == 0 {
		return nil, nil // Return null if not found
	}

	// Resolve relations for the single row
	if err := e.resolveRelationsForRow(ctx, model, result.Rows[0], args); err != nil {
		return nil, fmt.Errorf("relation resolve error: %w", err)
	}

	return result.Rows[0], nil
}

func (e *Engine) executeFindFirst(ctx context.Context, model string, args map[string]interface{}) (interface{}, error) {
	return e.executeFindUnique(ctx, model, args)
}

func (e *Engine) executeFindUniqueOrThrow(ctx context.Context, model string, args map[string]interface{}) (interface{}, error) {
	result, err := e.executeFindUnique(ctx, model, args)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, fmt.Errorf("record not found in model '%s'", model)
	}
	return result, nil
}

func (e *Engine) executeFindFirstOrThrow(ctx context.Context, model string, args map[string]interface{}) (interface{}, error) {
	return e.executeFindUniqueOrThrow(ctx, model, args)
}

func (e *Engine) executeCreate(ctx context.Context, model string, args map[string]interface{}) (interface{}, error) {
	q, err := e.newBuilder().BuildCreate(model, args)
	if err != nil {
		return nil, err
	}

	rows, err := e.connector.Query(ctx, q.SQL, q.Args...)
	if err != nil {
		return nil, fmt.Errorf("create error: %w", err)
	}
	defer rows.Close()

	result, err := connector.ScanRows(rows)
	if err != nil {
		return nil, fmt.Errorf("scan error: %w", err)
	}

	if len(result.Rows) == 0 {
		return nil, fmt.Errorf("create returned no data")
	}

	// Resolve relations for the newly created record
	if err := e.resolveRelationsForRow(ctx, model, result.Rows[0], args); err != nil {
		return nil, fmt.Errorf("relation resolve error: %w", err)
	}

	return result.Rows[0], nil
}

func (e *Engine) executeCreateMany(ctx context.Context, model string, args map[string]interface{}) (interface{}, error) {
	q, err := e.newBuilder().BuildCreateMany(model, args)
	if err != nil {
		return nil, err
	}

	if q.SQL == "" {
		return map[string]interface{}{"count": 0}, nil
	}

	result, err := e.connector.Execute(ctx, q.SQL, q.Args...)
	if err != nil {
		return nil, fmt.Errorf("createMany error: %w", err)
	}

	count, _ := result.RowsAffected()
	return map[string]interface{}{"count": count}, nil
}

func (e *Engine) executeUpdate(ctx context.Context, model string, args map[string]interface{}) (interface{}, error) {
	q, err := e.newBuilder().BuildUpdate(model, args)
	if err != nil {
		return nil, err
	}

	rows, err := e.connector.Query(ctx, q.SQL, q.Args...)
	if err != nil {
		return nil, fmt.Errorf("update error: %w", err)
	}
	defer rows.Close()

	result, err := connector.ScanRows(rows)
	if err != nil {
		return nil, fmt.Errorf("scan error: %w", err)
	}

	if len(result.Rows) == 0 {
		return nil, fmt.Errorf("record not found for update in model '%s'", model)
	}

	if err := e.resolveRelationsForRow(ctx, model, result.Rows[0], args); err != nil {
		return nil, fmt.Errorf("relation resolve error: %w", err)
	}

	return result.Rows[0], nil
}

func (e *Engine) executeUpdateMany(ctx context.Context, model string, args map[string]interface{}) (interface{}, error) {
	q, err := e.newBuilder().BuildUpdateMany(model, args)
	if err != nil {
		return nil, err
	}

	result, err := e.connector.Execute(ctx, q.SQL, q.Args...)
	if err != nil {
		return nil, fmt.Errorf("updateMany error: %w", err)
	}

	count, _ := result.RowsAffected()
	return map[string]interface{}{"count": count}, nil
}

func (e *Engine) executeDelete(ctx context.Context, model string, args map[string]interface{}) (interface{}, error) {
	q, err := e.newBuilder().BuildDelete(model, args)
	if err != nil {
		return nil, err
	}

	rows, err := e.connector.Query(ctx, q.SQL, q.Args...)
	if err != nil {
		return nil, fmt.Errorf("delete error: %w", err)
	}
	defer rows.Close()

	result, err := connector.ScanRows(rows)
	if err != nil {
		return nil, fmt.Errorf("scan error: %w", err)
	}

	if len(result.Rows) == 0 {
		return nil, fmt.Errorf("record not found for delete in model '%s'", model)
	}

	if err := e.resolveRelationsForRow(ctx, model, result.Rows[0], args); err != nil {
		return nil, fmt.Errorf("relation resolve error: %w", err)
	}

	return result.Rows[0], nil
}

func (e *Engine) executeDeleteMany(ctx context.Context, model string, args map[string]interface{}) (interface{}, error) {
	q, err := e.newBuilder().BuildDeleteMany(model, args)
	if err != nil {
		return nil, err
	}

	result, err := e.connector.Execute(ctx, q.SQL, q.Args...)
	if err != nil {
		return nil, fmt.Errorf("deleteMany error: %w", err)
	}

	count, _ := result.RowsAffected()
	return map[string]interface{}{"count": count}, nil
}

func (e *Engine) executeUpsert(ctx context.Context, model string, args map[string]interface{}) (interface{}, error) {
	q, err := e.newBuilder().BuildUpsert(model, args)
	if err != nil {
		return nil, err
	}

	rows, err := e.connector.Query(ctx, q.SQL, q.Args...)
	if err != nil {
		return nil, fmt.Errorf("upsert error: %w", err)
	}
	defer rows.Close()

	result, err := connector.ScanRows(rows)
	if err != nil {
		return nil, fmt.Errorf("scan error: %w", err)
	}

	if len(result.Rows) == 0 {
		return nil, fmt.Errorf("upsert returned no data")
	}

	if err := e.resolveRelationsForRow(ctx, model, result.Rows[0], args); err != nil {
		return nil, fmt.Errorf("relation resolve error: %w", err)
	}

	return result.Rows[0], nil
}

func (e *Engine) executeCount(ctx context.Context, model string, args map[string]interface{}) (interface{}, error) {
	q, err := e.newBuilder().BuildCount(model, args)
	if err != nil {
		return nil, err
	}

	row := e.connector.QueryRow(ctx, q.SQL, q.Args...)
	var count int64
	if err := row.Scan(&count); err != nil {
		return nil, fmt.Errorf("count error: %w", err)
	}

	return count, nil
}

func (e *Engine) executeAggregate(ctx context.Context, model string, args map[string]interface{}) (interface{}, error) {
	q, err := e.newBuilder().BuildAggregate(model, args)
	if err != nil {
		return nil, err
	}

	rows, err := e.connector.Query(ctx, q.SQL, q.Args...)
	if err != nil {
		return nil, fmt.Errorf("aggregate error: %w", err)
	}
	defer rows.Close()

	result, err := connector.ScanRows(rows)
	if err != nil {
		return nil, fmt.Errorf("scan error: %w", err)
	}

	if len(result.Rows) == 0 {
		return nil, nil
	}

	return result.Rows[0], nil
}

func (e *Engine) executeGroupBy(ctx context.Context, model string, args map[string]interface{}) (interface{}, error) {
	q, err := e.newBuilder().BuildGroupBy(model, args)
	if err != nil {
		return nil, err
	}

	rows, err := e.connector.Query(ctx, q.SQL, q.Args...)
	if err != nil {
		return nil, fmt.Errorf("groupBy error: %w", err)
	}
	defer rows.Close()

	result, err := connector.ScanRows(rows)
	if err != nil {
		return nil, fmt.Errorf("scan error: %w", err)
	}

	return result.Rows, nil
}

// ============================================================================
// Schema operations
// ============================================================================

// PushSchema creates all tables and enums from the schema.
func (e *Engine) PushSchema(ctx context.Context) error {
	// Create enums first
	for _, enum := range e.schema.Enums {
		sql := e.newBuilder().BuildCreateEnum(&enum)
		if _, err := e.connector.Execute(ctx, sql); err != nil {
			return fmt.Errorf("error creating enum '%s': %w", enum.Name, err)
		}
	}

	// Create tables
	for i := range e.schema.Models {
		sql := e.newBuilder().BuildCreateTable(&e.schema.Models[i])
		if _, err := e.connector.Execute(ctx, sql); err != nil {
			return fmt.Errorf("error creating table for model '%s': %w", e.schema.Models[i].Name, err)
		}
	}

	return nil
}

// ExecuteRaw executes a raw SQL query.
func (e *Engine) ExecuteRaw(ctx context.Context, sql string, args []interface{}) (interface{}, error) {
	result, err := e.connector.Execute(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	count, _ := result.RowsAffected()
	return map[string]interface{}{"count": count}, nil
}

// QueryRaw executes a raw SQL query and returns rows.
func (e *Engine) QueryRaw(ctx context.Context, sqlStr string, args []interface{}) (interface{}, error) {
	rows, err := e.connector.Query(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result, err := connector.ScanRows(rows)
	if err != nil {
		return nil, err
	}

	return result.Rows, nil
}

// GetSchemaJSON returns the parsed schema as JSON (for the client generator).
func (e *Engine) GetSchemaJSON() ([]byte, error) {
	return json.Marshal(e.schema)
}

// ============================================================================
// Transaction lifecycle
// ============================================================================

// mapIsolationLevel converts a string isolation level to sql.IsolationLevel.
func mapIsolationLevel(level string) sql.IsolationLevel {
	switch level {
	case "ReadUncommitted":
		return sql.LevelReadUncommitted
	case "ReadCommitted":
		return sql.LevelReadCommitted
	case "RepeatableRead":
		return sql.LevelRepeatableRead
	case "Serializable":
		return sql.LevelSerializable
	default:
		return sql.LevelDefault
	}
}

// BeginTransaction starts a SQL transaction and returns a unique txID.
func (e *Engine) BeginTransaction(ctx context.Context, isolationLevel string, timeoutMs int) (string, error) {
	opts := &sql.TxOptions{
		Isolation: mapIsolationLevel(isolationLevel),
	}

	tx, err := e.connector.BeginTx(ctx, opts)
	if err != nil {
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}

	txID := uuid.New().String()
	entry := &transactionEntry{tx: tx}

	e.txMu.Lock()
	e.txStore[txID] = entry
	e.txMu.Unlock()

	if timeoutMs > 0 {
		timer := time.AfterFunc(time.Duration(timeoutMs)*time.Millisecond, func() {
			e.expireTransaction(txID)
		})

		e.txMu.Lock()
		if current, ok := e.txStore[txID]; ok && current == entry {
			current.timer = timer
		} else {
			timer.Stop()
		}
		e.txMu.Unlock()
	}

	return txID, nil
}

// CommitTransaction commits the transaction identified by txID.
func (e *Engine) CommitTransaction(txID string) error {
	entry, ok := e.takeTransaction(txID)
	if !ok {
		return fmt.Errorf("transaction '%s' not found", txID)
	}

	return entry.tx.Commit()
}

// RollbackTransaction rolls back the transaction identified by txID.
func (e *Engine) RollbackTransaction(txID string) error {
	entry, ok := e.takeTransaction(txID)
	if !ok {
		return fmt.Errorf("transaction '%s' not found", txID)
	}

	return entry.tx.Rollback()
}

// ExecuteInTransaction runs a query/mutation inside the given transaction.
func (e *Engine) ExecuteInTransaction(ctx context.Context, txID string, model string, action string, args map[string]interface{}) (interface{}, error) {
	e.txMu.RLock()
	entry, ok := e.txStore[txID]
	e.txMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("transaction '%s' not found", txID)
	}

	// Use the transaction-scoped executor
	return e.executeWithTx(ctx, entry.tx, model, action, args)
}

func (e *Engine) takeTransaction(txID string) (*transactionEntry, bool) {
	e.txMu.Lock()
	entry, ok := e.txStore[txID]
	if ok {
		delete(e.txStore, txID)
	}
	e.txMu.Unlock()

	if ok && entry.timer != nil {
		entry.timer.Stop()
	}

	return entry, ok
}

func (e *Engine) expireTransaction(txID string) {
	entry, ok := e.takeTransaction(txID)
	if !ok {
		return
	}

	_ = entry.tx.Rollback()
}

// executeWithTx dispatches a query using the provided sql.Tx instead of the connector.
func (e *Engine) executeWithTx(ctx context.Context, tx *sql.Tx, model string, action string, args map[string]interface{}) (interface{}, error) {
	switch action {
	case "findMany":
		return e.txFindMany(ctx, tx, model, args)
	case "findUnique", "findFirst":
		return e.txFindUnique(ctx, tx, model, args)
	case "findUniqueOrThrow", "findFirstOrThrow":
		result, err := e.txFindUnique(ctx, tx, model, args)
		if err != nil {
			return nil, err
		}
		if result == nil {
			return nil, fmt.Errorf("record not found in model '%s'", model)
		}
		return result, nil
	case "create":
		return e.txCreate(ctx, tx, model, args)
	case "update":
		return e.txUpdate(ctx, tx, model, args)
	case "delete":
		return e.txDelete(ctx, tx, model, args)
	case "createMany":
		return e.txCreateMany(ctx, tx, model, args)
	case "updateMany":
		return e.txUpdateMany(ctx, tx, model, args)
	case "deleteMany":
		return e.txDeleteMany(ctx, tx, model, args)
	case "upsert":
		return e.txUpsert(ctx, tx, model, args)
	case "count":
		return e.txCount(ctx, tx, model, args)
	case "aggregate":
		return e.txAggregate(ctx, tx, model, args)
	case "groupBy":
		return e.txGroupBy(ctx, tx, model, args)
	case "findManyPaginated":
		return e.txFindManyPaginated(ctx, tx, model, args)
	case "findManyCursorPaginated":
		return e.txFindManyCursorPaginated(ctx, tx, model, args)
	default:
		return nil, fmt.Errorf("unsupported action '%s' inside transaction", action)
	}
}

// --- Transaction-scoped query helpers ---

func (e *Engine) txQuery(ctx context.Context, tx *sql.Tx, sql string, args ...interface{}) (*connector.QueryResult, error) {
	rows, err := tx.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return connector.ScanRows(rows)
}

func (e *Engine) txFindMany(ctx context.Context, tx *sql.Tx, model string, args map[string]interface{}) (interface{}, error) {
	q, err := e.newBuilder().BuildFindMany(model, args)
	if err != nil {
		return nil, err
	}
	result, err := e.txQuery(ctx, tx, q.SQL, q.Args...)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	return result.Rows, nil
}

func (e *Engine) txFindUnique(ctx context.Context, tx *sql.Tx, model string, args map[string]interface{}) (interface{}, error) {
	q, err := e.newBuilder().BuildFindUnique(model, args)
	if err != nil {
		return nil, err
	}
	result, err := e.txQuery(ctx, tx, q.SQL, q.Args...)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	if len(result.Rows) == 0 {
		return nil, nil
	}
	return result.Rows[0], nil
}

func (e *Engine) txCreate(ctx context.Context, tx *sql.Tx, model string, args map[string]interface{}) (interface{}, error) {
	q, err := e.newBuilder().BuildCreate(model, args)
	if err != nil {
		return nil, err
	}
	result, err := e.txQuery(ctx, tx, q.SQL, q.Args...)
	if err != nil {
		return nil, fmt.Errorf("create error: %w", err)
	}
	if len(result.Rows) == 0 {
		return nil, fmt.Errorf("create returned no data")
	}
	return result.Rows[0], nil
}

func (e *Engine) txUpdate(ctx context.Context, tx *sql.Tx, model string, args map[string]interface{}) (interface{}, error) {
	q, err := e.newBuilder().BuildUpdate(model, args)
	if err != nil {
		return nil, err
	}
	result, err := e.txQuery(ctx, tx, q.SQL, q.Args...)
	if err != nil {
		return nil, fmt.Errorf("update error: %w", err)
	}
	if len(result.Rows) == 0 {
		return nil, fmt.Errorf("record not found for update in model '%s'", model)
	}
	return result.Rows[0], nil
}

func (e *Engine) txDelete(ctx context.Context, tx *sql.Tx, model string, args map[string]interface{}) (interface{}, error) {
	q, err := e.newBuilder().BuildDelete(model, args)
	if err != nil {
		return nil, err
	}
	result, err := e.txQuery(ctx, tx, q.SQL, q.Args...)
	if err != nil {
		return nil, fmt.Errorf("delete error: %w", err)
	}
	if len(result.Rows) == 0 {
		return nil, fmt.Errorf("record not found for delete in model '%s'", model)
	}
	return result.Rows[0], nil
}

func (e *Engine) txCreateMany(ctx context.Context, tx *sql.Tx, model string, args map[string]interface{}) (interface{}, error) {
	q, err := e.newBuilder().BuildCreateMany(model, args)
	if err != nil {
		return nil, err
	}
	if q.SQL == "" {
		return map[string]interface{}{"count": 0}, nil
	}
	result, err := tx.ExecContext(ctx, q.SQL, q.Args...)
	if err != nil {
		return nil, fmt.Errorf("createMany error: %w", err)
	}
	count, _ := result.RowsAffected()
	return map[string]interface{}{"count": count}, nil
}

func (e *Engine) txUpdateMany(ctx context.Context, tx *sql.Tx, model string, args map[string]interface{}) (interface{}, error) {
	q, err := e.newBuilder().BuildUpdateMany(model, args)
	if err != nil {
		return nil, err
	}
	result, err := tx.ExecContext(ctx, q.SQL, q.Args...)
	if err != nil {
		return nil, fmt.Errorf("updateMany error: %w", err)
	}
	count, _ := result.RowsAffected()
	return map[string]interface{}{"count": count}, nil
}

func (e *Engine) txDeleteMany(ctx context.Context, tx *sql.Tx, model string, args map[string]interface{}) (interface{}, error) {
	q, err := e.newBuilder().BuildDeleteMany(model, args)
	if err != nil {
		return nil, err
	}
	result, err := tx.ExecContext(ctx, q.SQL, q.Args...)
	if err != nil {
		return nil, fmt.Errorf("deleteMany error: %w", err)
	}
	count, _ := result.RowsAffected()
	return map[string]interface{}{"count": count}, nil
}

func (e *Engine) txUpsert(ctx context.Context, tx *sql.Tx, model string, args map[string]interface{}) (interface{}, error) {
	q, err := e.newBuilder().BuildUpsert(model, args)
	if err != nil {
		return nil, err
	}
	result, err := e.txQuery(ctx, tx, q.SQL, q.Args...)
	if err != nil {
		return nil, fmt.Errorf("upsert error: %w", err)
	}
	if len(result.Rows) == 0 {
		return nil, fmt.Errorf("upsert returned no data")
	}
	return result.Rows[0], nil
}

func (e *Engine) txCount(ctx context.Context, tx *sql.Tx, model string, args map[string]interface{}) (interface{}, error) {
	q, err := e.newBuilder().BuildCount(model, args)
	if err != nil {
		return nil, err
	}
	row := tx.QueryRowContext(ctx, q.SQL, q.Args...)
	var count int64
	if err := row.Scan(&count); err != nil {
		return nil, fmt.Errorf("count error: %w", err)
	}
	return count, nil
}

func (e *Engine) txAggregate(ctx context.Context, tx *sql.Tx, model string, args map[string]interface{}) (interface{}, error) {
	q, err := e.newBuilder().BuildAggregate(model, args)
	if err != nil {
		return nil, err
	}

	result, err := e.txQuery(ctx, tx, q.SQL, q.Args...)
	if err != nil {
		return nil, fmt.Errorf("aggregate error: %w", err)
	}

	if len(result.Rows) == 0 {
		return nil, nil
	}

	return result.Rows[0], nil
}

func (e *Engine) txGroupBy(ctx context.Context, tx *sql.Tx, model string, args map[string]interface{}) (interface{}, error) {
	q, err := e.newBuilder().BuildGroupBy(model, args)
	if err != nil {
		return nil, err
	}

	result, err := e.txQuery(ctx, tx, q.SQL, q.Args...)
	if err != nil {
		return nil, fmt.Errorf("groupBy error: %w", err)
	}

	return result.Rows, nil
}

func (e *Engine) txFindManyPaginated(ctx context.Context, tx *sql.Tx, model string, args map[string]interface{}) (interface{}, error) {
	page := 1
	limit := 10

	if p, ok := args["page"]; ok {
		if pf, ok := p.(float64); ok {
			page = int(pf)
		}
	}
	if l, ok := args["limit"]; ok {
		if lf, ok := l.(float64); ok {
			limit = int(lf)
		}
	}
	if page < 1 {
		page = 1
	}
	if limit < 1 {
		limit = 10
	}

	countArgs := make(map[string]interface{})
	if where, ok := args["where"]; ok {
		countArgs["where"] = where
	}

	totalResult, err := e.txCount(ctx, tx, model, countArgs)
	if err != nil {
		return nil, fmt.Errorf("pagination count error: %w", err)
	}
	total, _ := totalResult.(int64)

	findArgs := make(map[string]interface{})
	for k, v := range args {
		if k != "page" && k != "limit" {
			findArgs[k] = v
		}
	}
	findArgs["take"] = float64(limit)
	findArgs["skip"] = float64((page - 1) * limit)

	data, err := e.txFindMany(ctx, tx, model, findArgs)
	if err != nil {
		return nil, fmt.Errorf("pagination query error: %w", err)
	}

	totalPages := int(math.Ceil(float64(total) / float64(limit)))
	hasNext := page < totalPages

	return map[string]interface{}{
		"data":     data,
		"page":     page,
		"limit":    limit,
		"has_next": hasNext,
		"total":    total,
	}, nil
}

func (e *Engine) txFindManyCursorPaginated(ctx context.Context, tx *sql.Tx, model string, args map[string]interface{}) (interface{}, error) {
	take := 10
	if t, ok := args["take"]; ok {
		if tf, ok := t.(float64); ok {
			take = int(tf)
		}
	}
	if take < 1 {
		take = 10
	}

	cursorField := ""
	if cursor, ok := args["cursor"].(map[string]interface{}); ok {
		for k := range cursor {
			cursorField = k
			break
		}
	}

	if cursorField == "" {
		if orderBy, ok := args["orderBy"]; ok {
			switch ob := orderBy.(type) {
			case map[string]interface{}:
				for k := range ob {
					cursorField = k
					break
				}
			case []interface{}:
				if len(ob) > 0 {
					if first, ok := ob[0].(map[string]interface{}); ok {
						for k := range first {
							cursorField = k
							break
						}
					}
				}
			}
		}
	}

	if cursorField == "" {
		cursorField = "id"
	}

	buildArgs := make(map[string]interface{})
	for k, v := range args {
		buildArgs[k] = v
	}
	buildArgs["take"] = float64(take + 1)

	q, err := e.newBuilder().BuildFindManyCursorPaginated(model, buildArgs)
	if err != nil {
		return nil, fmt.Errorf("cursor pagination build error: %w", err)
	}

	result, err := e.txQuery(ctx, tx, q.SQL, q.Args...)
	if err != nil {
		return nil, fmt.Errorf("cursor pagination query error: %w", err)
	}

	allRows := result.Rows
	hasNextPage := len(allRows) > take
	if hasNextPage {
		allRows = allRows[:take]
	}

	var nextCursor interface{}
	if hasNextPage && len(allRows) > 0 {
		lastRow := allRows[len(allRows)-1]
		nextCursor = lastRow[cursorField]
		if nextCursor == nil {
			nextCursor = lastRow[toSnakeCase(cursorField)]
		}
	}

	data := make([]interface{}, len(allRows))
	for i, row := range allRows {
		data[i] = row
	}

	return map[string]interface{}{
		"data":        data,
		"nextCursor":  nextCursor,
		"hasNextPage": hasNextPage,
	}, nil
}

// ============================================================================
// Pagination
// ============================================================================

// executeFindManyPaginated runs a paginated findMany: COUNT(*) + SELECT with LIMIT/OFFSET.
func (e *Engine) executeFindManyPaginated(ctx context.Context, model string, args map[string]interface{}) (interface{}, error) {
	// Extract pagination params with defaults
	page := 1
	limit := 10

	if p, ok := args["page"]; ok {
		if pf, ok := p.(float64); ok {
			page = int(pf)
		}
	}
	if l, ok := args["limit"]; ok {
		if lf, ok := l.(float64); ok {
			limit = int(lf)
		}
	}
	if page < 1 {
		page = 1
	}
	if limit < 1 {
		limit = 10
	}

	// Run COUNT(*) with the same WHERE
	countArgs := make(map[string]interface{})
	if where, ok := args["where"]; ok {
		countArgs["where"] = where
	}

	totalResult, err := e.executeCount(ctx, model, countArgs)
	if err != nil {
		return nil, fmt.Errorf("pagination count error: %w", err)
	}
	total, _ := totalResult.(int64)

	// Build findMany args with skip/take
	findArgs := make(map[string]interface{})
	for k, v := range args {
		if k != "page" && k != "limit" {
			findArgs[k] = v
		}
	}
	findArgs["take"] = float64(limit)
	findArgs["skip"] = float64((page - 1) * limit)

	data, err := e.executeFindMany(ctx, model, findArgs)
	if err != nil {
		return nil, fmt.Errorf("pagination query error: %w", err)
	}

	totalPages := int(math.Ceil(float64(total) / float64(limit)))
	hasNext := page < totalPages

	return map[string]interface{}{
		"data":     data,
		"page":     page,
		"limit":    limit,
		"has_next": hasNext,
		"total":    total,
	}, nil
}

// executeFindManyCursorPaginated runs a cursor-based paginated findMany.
// It fetches take+1 rows to determine hasNextPage, then trims to take.
func (e *Engine) executeFindManyCursorPaginated(ctx context.Context, model string, args map[string]interface{}) (interface{}, error) {
	// Extract take (default 10)
	take := 10
	if t, ok := args["take"]; ok {
		if tf, ok := t.(float64); ok {
			take = int(tf)
		}
	}
	if take < 1 {
		take = 10
	}

	// Determine cursor field name for nextCursor extraction
	cursorField := ""
	if cursor, ok := args["cursor"].(map[string]interface{}); ok {
		for k := range cursor {
			cursorField = k
			break
		}
	}

	// If no cursor field specified, infer from orderBy
	if cursorField == "" {
		if orderBy, ok := args["orderBy"]; ok {
			switch ob := orderBy.(type) {
			case map[string]interface{}:
				for k := range ob {
					cursorField = k
					break
				}
			case []interface{}:
				if len(ob) > 0 {
					if first, ok := ob[0].(map[string]interface{}); ok {
						for k := range first {
							cursorField = k
							break
						}
					}
				}
			}
		}
	}

	// Default cursor field to "id" if nothing specified
	if cursorField == "" {
		cursorField = "id"
	}

	// Build args for the builder with take+1 for boundary detection
	buildArgs := make(map[string]interface{})
	for k, v := range args {
		buildArgs[k] = v
	}
	buildArgs["take"] = float64(take + 1)

	q, err := e.newBuilder().BuildFindManyCursorPaginated(model, buildArgs)
	if err != nil {
		return nil, fmt.Errorf("cursor pagination build error: %w", err)
	}

	rows, err := e.connector.Query(ctx, q.SQL, q.Args...)
	if err != nil {
		return nil, fmt.Errorf("cursor pagination query error: %w", err)
	}
	defer rows.Close()

	result, err := connector.ScanRows(rows)
	if err != nil {
		return nil, fmt.Errorf("cursor pagination scan error: %w", err)
	}

	allRows := result.Rows
	hasNextPage := len(allRows) > take

	// Trim the extra boundary row
	if hasNextPage {
		allRows = allRows[:take]
	}

	// Extract nextCursor from the last row
	var nextCursor interface{}
	if hasNextPage && len(allRows) > 0 {
		lastRow := allRows[len(allRows)-1]
		nextCursor = lastRow[cursorField]
		// Also try snake_case variant
		if nextCursor == nil {
			nextCursor = lastRow[toSnakeCase(cursorField)]
		}
	}

	// Convert to []interface{} for JSON serialization consistency
	data := make([]interface{}, len(allRows))
	for i, row := range allRows {
		data[i] = row
	}

	return map[string]interface{}{
		"data":        data,
		"nextCursor":  nextCursor,
		"hasNextPage": hasNextPage,
	}, nil
}

// ============================================================================
// Relation loading (include & nested select)
// ============================================================================

// resolveRelationsForRows resolves include/select relation loading for a list of rows.
// This is the batch entry point — it collects all parent IDs and runs one query per relation.
func (e *Engine) resolveRelationsForRows(ctx context.Context, modelName string, rows []map[string]interface{}, args map[string]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	relationsToLoad := e.extractRelationsToLoad(modelName, args)
	if len(relationsToLoad) == 0 {
		return nil
	}

	model := e.builder.getModel(modelName)
	if model == nil {
		return fmt.Errorf("model '%s' not found", modelName)
	}

	for _, rel := range relationsToLoad {
		relInfo := model.GetRelationInfo(rel.fieldName, e.schema)
		if relInfo == nil {
			continue // Skip unknown relations silently
		}

		if err := e.loadRelation(ctx, modelName, rows, relInfo, rel.nestedArgs); err != nil {
			return fmt.Errorf("error loading relation '%s': %w", rel.fieldName, err)
		}
	}

	return nil
}

// resolveRelationsForRow resolves include/select relation loading for a single row.
func (e *Engine) resolveRelationsForRow(ctx context.Context, modelName string, row map[string]interface{}, args map[string]interface{}) error {
	rows := []map[string]interface{}{row}
	return e.resolveRelationsForRows(ctx, modelName, rows, args)
}

// relationToLoad represents a requested relation with optional nested query args.
type relationToLoad struct {
	fieldName  string
	nestedArgs map[string]interface{}
}

// extractRelationsToLoad collects relations from both "include" and "select" args.
//
// Why both? In Prisma's API:
//   - `include: { posts: true }` loads all scalar fields + adds the relation
//   - `select: { name: true, posts: true }` picks specific scalar fields + adds the relation
//   - `select` and `include` are mutually exclusive at the same level
func (e *Engine) extractRelationsToLoad(modelName string, args map[string]interface{}) []relationToLoad {
	var result []relationToLoad
	model := e.builder.getModel(modelName)
	if model == nil {
		return nil
	}

	// Check "include" first
	if includeMap, ok := args["include"].(map[string]interface{}); ok {
		for fieldName, val := range includeMap {
			field := model.GetFieldByName(fieldName)
			if field == nil || !field.Type.IsModel {
				continue
			}

			rel := relationToLoad{fieldName: fieldName}
			switch v := val.(type) {
			case bool:
				if !v {
					continue
				}
				rel.nestedArgs = make(map[string]interface{})
			case map[string]interface{}:
				rel.nestedArgs = v
			default:
				continue
			}
			result = append(result, rel)
		}
	}

	// Check "select" for relation fields
	if selectMap, ok := args["select"].(map[string]interface{}); ok {
		for fieldName, val := range selectMap {
			field := model.GetFieldByName(fieldName)
			if field == nil || !field.Type.IsModel {
				continue
			}

			rel := relationToLoad{fieldName: fieldName}
			switch v := val.(type) {
			case bool:
				if !v {
					continue
				}
				rel.nestedArgs = make(map[string]interface{})
			case map[string]interface{}:
				rel.nestedArgs = v
			default:
				continue
			}
			result = append(result, rel)
		}
	}

	return result
}

// loadRelation batch-loads a single relation for a set of parent rows.
//
// Strategy:
//  1. Collect all unique parent IDs from the result rows
//  2. Issue ONE query: SELECT ... FROM target WHERE fk IN (...)
//  3. Group loaded rows by their FK value
//  4. Attach grouped results to each parent row
//  5. Recursively resolve nested includes
func (e *Engine) loadRelation(
	ctx context.Context,
	sourceModelName string,
	parentRows []map[string]interface{},
	relInfo *schema.RelationInfo,
	nestedArgs map[string]interface{},
) error {
	if len(relInfo.FKFields) == 0 || len(relInfo.RefFields) == 0 {
		return nil
	}

	switch relInfo.Direction {
	case schema.RelationHasMany, schema.RelationHasOne:
		return e.loadHasManyOrHasOne(ctx, sourceModelName, parentRows, relInfo, nestedArgs)
	case schema.RelationBelongsTo:
		return e.loadBelongsTo(ctx, sourceModelName, parentRows, relInfo, nestedArgs)
	default:
		return nil
	}
}

// loadHasManyOrHasOne loads relations where the FK is on the TARGET model.
// e.g., User.posts → Post table has author_id pointing to User.id
func (e *Engine) loadHasManyOrHasOne(
	ctx context.Context,
	sourceModelName string,
	parentRows []map[string]interface{},
	relInfo *schema.RelationInfo,
	nestedArgs map[string]interface{},
) error {
	// refFields are on the SOURCE model (e.g., User.id)
	// fkFields are on the TARGET model (e.g., Post.authorId → column "author_id")
	refFieldName := relInfo.RefFields[0]
	fkFieldName := relInfo.FKFields[0]

	// Collect unique parent ref values (e.g., all user IDs)
	parentIDs, refColName := e.collectParentValues(sourceModelName, parentRows, refFieldName)
	if len(parentIDs) == 0 {
		// No parent IDs → set empty results
		for _, row := range parentRows {
			if relInfo.IsList {
				row[relInfo.FieldName] = []interface{}{}
			} else {
				row[relInfo.FieldName] = nil
			}
		}
		return nil
	}

	// Build and execute the relation query
	q, err := e.newBuilder().BuildRelationQuery(relInfo.TargetModel, fkFieldName, parentIDs, nestedArgs)
	if err != nil {
		return err
	}

	rows, err := e.connector.Query(ctx, q.SQL, q.Args...)
	if err != nil {
		return fmt.Errorf("relation query error: %w", err)
	}
	defer rows.Close()

	result, err := connector.ScanRows(rows)
	if err != nil {
		return fmt.Errorf("relation scan error: %w", err)
	}

	// Recursively resolve nested relations on the loaded rows
	if err := e.resolveRelationsForRows(ctx, relInfo.TargetModel, result.Rows, nestedArgs); err != nil {
		return err
	}

	// Get the FK column name as it appears in query results
	targetModel := e.builder.getModel(relInfo.TargetModel)
	fkField := targetModel.GetFieldByName(fkFieldName)
	fkColName := strings.Trim(e.builder.columnName(fkField), `"`)

	// Group loaded rows by FK value
	grouped := make(map[interface{}][]interface{})
	for _, relRow := range result.Rows {
		fkVal := relRow[fkColName]
		grouped[fkVal] = append(grouped[fkVal], relRow)
	}

	// Attach to each parent row
	for _, parentRow := range parentRows {
		parentVal := parentRow[refColName]
		if relInfo.IsList {
			if children, ok := grouped[parentVal]; ok {
				parentRow[relInfo.FieldName] = children
			} else {
				parentRow[relInfo.FieldName] = []interface{}{}
			}
		} else {
			if children, ok := grouped[parentVal]; ok && len(children) > 0 {
				parentRow[relInfo.FieldName] = children[0]
			} else {
				parentRow[relInfo.FieldName] = nil
			}
		}
	}

	return nil
}

// loadBelongsTo loads relations where the FK is on the SOURCE model.
// e.g., Post.author → Post has authorId, loading User by User.id
func (e *Engine) loadBelongsTo(
	ctx context.Context,
	sourceModelName string,
	parentRows []map[string]interface{},
	relInfo *schema.RelationInfo,
	nestedArgs map[string]interface{},
) error {
	// fkFields are on the SOURCE model (e.g., Post.authorId → column "author_id")
	// refFields are on the TARGET model (e.g., User.id → column "id")
	fkFieldName := relInfo.FKFields[0]
	refFieldName := relInfo.RefFields[0]

	// Collect unique FK values from parent rows (e.g., all authorId values)
	fkValues, fkColName := e.collectParentValues(sourceModelName, parentRows, fkFieldName)
	if len(fkValues) == 0 {
		for _, row := range parentRows {
			row[relInfo.FieldName] = nil
		}
		return nil
	}

	// Build and execute query on the target model
	q, err := e.newBuilder().BuildBelongsToQuery(relInfo.TargetModel, refFieldName, fkValues, nestedArgs)
	if err != nil {
		return err
	}

	rows, err := e.connector.Query(ctx, q.SQL, q.Args...)
	if err != nil {
		return fmt.Errorf("relation query error: %w", err)
	}
	defer rows.Close()

	result, err := connector.ScanRows(rows)
	if err != nil {
		return fmt.Errorf("relation scan error: %w", err)
	}

	// Recursively resolve nested relations
	if err := e.resolveRelationsForRows(ctx, relInfo.TargetModel, result.Rows, nestedArgs); err != nil {
		return err
	}

	// Get the ref column name as it appears in query results
	targetModel := e.builder.getModel(relInfo.TargetModel)
	refField := targetModel.GetFieldByName(refFieldName)
	refColName := strings.Trim(e.builder.columnName(refField), `"`)

	// Index loaded rows by their PK value
	indexed := make(map[interface{}]map[string]interface{})
	for _, relRow := range result.Rows {
		pkVal := relRow[refColName]
		indexed[pkVal] = relRow
	}

	// Attach to each parent row
	for _, parentRow := range parentRows {
		fkVal := parentRow[fkColName]
		if fkVal == nil {
			parentRow[relInfo.FieldName] = nil
			continue
		}
		if target, ok := indexed[fkVal]; ok {
			parentRow[relInfo.FieldName] = target
		} else {
			parentRow[relInfo.FieldName] = nil
		}
	}

	return nil
}

// collectParentValues extracts unique values for a given field from parent rows.
// It resolves the field name to the actual column name used in the query result.
// Returns the unique values and the column name key used in the row maps.
func (e *Engine) collectParentValues(modelName string, parentRows []map[string]interface{}, fieldName string) ([]interface{}, string) {
	colName := toSnakeCase(fieldName)
	if model := e.builder.getModel(modelName); model != nil {
		if field := model.GetFieldByName(fieldName); field != nil {
			colName = e.builder.columnDBName(field)
		}
	}

	seen := make(map[interface{}]bool)
	var values []interface{}

	for _, row := range parentRows {
		val, ok := row[colName]
		if !ok || val == nil {
			continue
		}
		if !seen[val] {
			seen[val] = true
			values = append(values, val)
		}
	}

	return values, colName
}
