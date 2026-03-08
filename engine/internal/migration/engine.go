package migration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/practor/practor-engine/internal/connector"
	"github.com/practor/practor-engine/internal/query"
	"github.com/practor/practor-engine/internal/schema"
)

// ============================================================================
// Migration Engine — Manages database schema migrations
// ============================================================================

// Engine manages migrations.
type Engine struct {
	connector    connector.Connector
	queryBuilder *query.Builder
	schema       *schema.Schema
}

// NewEngine creates a new migration Engine.
func NewEngine(conn connector.Connector, s *schema.Schema) *Engine {
	return &Engine{
		connector:    conn,
		queryBuilder: query.NewBuilder(string(conn.GetDialect()), s),
		schema:       s,
	}
}

// Migration represents a single migration.
type Migration struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	SQL       string    `json:"sql"`
	AppliedAt time.Time `json:"appliedAt"`
}

// MigrationStatus represents the current migration status.
type MigrationStatus struct {
	Applied []Migration `json:"applied"`
	Pending []Migration `json:"pending"`
}

// DeployResult represents the result of a deploy operation.
type DeployResult struct {
	Applied []string `json:"applied"`
	Count   int      `json:"count"`
	Message string   `json:"message"`
}

// DevMigrationResult represents the result of creating a dev migration.
type DevMigrationResult struct {
	MigrationID string `json:"migrationId"`
	SQL         string `json:"sql"`
	FilePath    string `json:"filePath"`
	Message     string `json:"message"`
}

// ============================================================================
// Migration tracking table
// ============================================================================

const migrationsTableSQL = `
CREATE TABLE IF NOT EXISTS "_practor_migrations" (
  "id" TEXT PRIMARY KEY,
  "name" TEXT NOT NULL,
  "applied_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "sql_content" TEXT NOT NULL
)
`

const (
	migrationSQLFileName            = "migration.sql"
	migrationSchemaSnapshotFileName = "schema.practor"
)

// EnsureMigrationsTable creates the migrations tracking table if it doesn't exist.
func (e *Engine) EnsureMigrationsTable(ctx context.Context) error {
	_, err := e.connector.Execute(ctx, migrationsTableSQL)
	return err
}

// ============================================================================
// Applied migration tracking
// ============================================================================

// GetAppliedMigrations returns a list of migration IDs already applied to the database.
func (e *Engine) GetAppliedMigrations(ctx context.Context) ([]string, error) {
	rows, err := e.connector.Query(ctx, `SELECT "id" FROM "_practor_migrations" ORDER BY "applied_at" ASC`)
	if err != nil {
		return nil, fmt.Errorf("failed to query applied migrations: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan migration row: %w", err)
		}
		ids = append(ids, id)
	}

	return ids, rows.Err()
}

// RecordMigration inserts a migration record into the tracking table.
func (e *Engine) RecordMigration(ctx context.Context, id, name, sqlContent string) error {
	_, err := e.connector.Execute(ctx,
		`INSERT INTO "_practor_migrations" ("id", "name", "sql_content") VALUES ($1, $2, $3)`,
		id, name, sqlContent,
	)
	if err != nil {
		return fmt.Errorf("failed to record migration '%s': %w", id, err)
	}
	return nil
}

func recordMigrationTx(ctx context.Context, tx *sql.Tx, id, name, sqlContent string) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO "_practor_migrations" ("id", "name", "sql_content") VALUES ($1, $2, $3)`,
		id, name, sqlContent,
	)
	if err != nil {
		return fmt.Errorf("failed to record migration '%s': %w", id, err)
	}
	return nil
}

// ============================================================================
// Deploy — Apply pending migrations from disk (production)
// ============================================================================

// Deploy reads migration files from migrationsDir, determines which are pending,
// and applies them sequentially. Each migration runs inside a transaction.
// This is the production-safe command — it never creates new migration files.
func (e *Engine) Deploy(ctx context.Context, migrationsDir string) (*DeployResult, error) {
	// Ensure tracking table exists
	if err := e.EnsureMigrationsTable(ctx); err != nil {
		return nil, fmt.Errorf("failed to ensure migrations table: %w", err)
	}

	// Get already-applied migrations
	applied, err := e.GetAppliedMigrations(ctx)
	if err != nil {
		return nil, err
	}
	appliedSet := make(map[string]bool, len(applied))
	for _, id := range applied {
		appliedSet[id] = true
	}

	// Discover migration directories on disk
	migrationDirs, err := discoverMigrations(migrationsDir)
	if err != nil {
		return nil, err
	}

	if len(migrationDirs) == 0 {
		return &DeployResult{
			Applied: []string{},
			Count:   0,
			Message: "No migration files found",
		}, nil
	}

	// Apply each pending migration
	var appliedMigrations []string
	for _, migDir := range migrationDirs {
		migID := filepath.Base(migDir)

		if appliedSet[migID] {
			continue // Already applied
		}

		sqlPath := filepath.Join(migDir, migrationSQLFileName)
		sqlBytes, err := os.ReadFile(sqlPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read migration '%s': %w", migID, err)
		}

		sqlContent := string(sqlBytes)
		if strings.TrimSpace(sqlContent) == "" {
			continue // Skip empty migrations
		}

		// Extract human-readable name from dir name (strip timestamp prefix)
		name := extractMigrationName(migID)

		// Apply inside a transaction
		tx, err := e.connector.BeginTx(ctx, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to begin transaction for migration '%s': %w", migID, err)
		}

		if _, err := tx.ExecContext(ctx, sqlContent); err != nil {
			tx.Rollback()
			return nil, fmt.Errorf("failed to apply migration '%s': %w", migID, err)
		}

		if err := recordMigrationTx(ctx, tx, migID, name, sqlContent); err != nil {
			tx.Rollback()
			return nil, err
		}

		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("failed to commit migration '%s': %w", migID, err)
		}

		appliedMigrations = append(appliedMigrations, migID)
	}

	if len(appliedMigrations) == 0 {
		return &DeployResult{
			Applied: []string{},
			Count:   0,
			Message: "Database is already up to date",
		}, nil
	}

	return &DeployResult{
		Applied: appliedMigrations,
		Count:   len(appliedMigrations),
		Message: fmt.Sprintf("Applied %d migration(s) successfully", len(appliedMigrations)),
	}, nil
}

// ============================================================================
// Dev Migration — Generate and apply a new migration (development)
// ============================================================================

// CreateDevMigration generates a new migration SQL file from the current schema,
// writes it to disk, and applies it. This is the development workflow command.
func (e *Engine) CreateDevMigration(ctx context.Context, migrationsDir, name, schemaPath string) (*DevMigrationResult, error) {
	// Read and parse the schema
	data, err := os.ReadFile(schemaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema: %w", err)
	}

	parsed, err := schema.Parse(string(data))
	if err != nil {
		return nil, fmt.Errorf("schema parse error: %w", err)
	}
	schema.ResolveFieldTypes(parsed)

	// Ensure tracking table exists
	if err := e.EnsureMigrationsTable(ctx); err != nil {
		return nil, fmt.Errorf("failed to ensure migrations table: %w", err)
	}

	dialect := string(e.connector.GetDialect())
	migrationDirs, err := discoverMigrations(migrationsDir)
	if err != nil {
		return nil, err
	}

	previousSchema, err := loadPreviousSchemaSnapshot(migrationDirs)
	if err != nil {
		return nil, err
	}
	if previousSchema == nil && len(migrationDirs) > 0 {
		return nil, fmt.Errorf(
			"existing migrations were found in '%s' but no schema snapshots are available; add '%s' to older migrations or create a new baseline",
			migrationsDir,
			migrationSchemaSnapshotFileName,
		)
	}

	sqlContent, err := generateDevMigrationSQL(previousSchema, parsed, dialect)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(sqlContent) == "" {
		return &DevMigrationResult{
			Message: "No schema changes detected",
		}, nil
	}

	// Generate migration ID (timestamp + name)
	timestamp := time.Now().Format("20060102150405")
	safeName := sanitizeMigrationName(name)
	if safeName == "" {
		safeName = "migration"
	}
	migrationID := fmt.Sprintf("%s_%s", timestamp, safeName)

	// Create migration directory and file
	migDir := filepath.Join(migrationsDir, migrationID)
	if err := os.MkdirAll(migDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create migration directory: %w", err)
	}

	sqlPath := filepath.Join(migDir, migrationSQLFileName)
	if err := os.WriteFile(sqlPath, []byte(sqlContent), 0644); err != nil {
		return nil, fmt.Errorf("failed to write migration file: %w", err)
	}

	snapshotPath := filepath.Join(migDir, migrationSchemaSnapshotFileName)
	if err := os.WriteFile(snapshotPath, data, 0644); err != nil {
		_ = os.RemoveAll(migDir)
		return nil, fmt.Errorf("failed to write migration schema snapshot: %w", err)
	}

	// Apply the migration
	tx, err := e.connector.BeginTx(ctx, nil)
	if err != nil {
		_ = os.RemoveAll(migDir)
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	if _, err := tx.ExecContext(ctx, sqlContent); err != nil {
		tx.Rollback()
		_ = os.RemoveAll(migDir)
		return nil, fmt.Errorf("failed to apply migration: %w", err)
	}

	if err := recordMigrationTx(ctx, tx, migrationID, safeName, sqlContent); err != nil {
		tx.Rollback()
		_ = os.RemoveAll(migDir)
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		_ = os.RemoveAll(migDir)
		return nil, fmt.Errorf("failed to commit migration: %w", err)
	}

	return &DevMigrationResult{
		MigrationID: migrationID,
		SQL:         sqlContent,
		FilePath:    sqlPath,
		Message:     fmt.Sprintf("Migration '%s' created and applied successfully", migrationID),
	}, nil
}

// ============================================================================
// Schema diffing
// ============================================================================

// Diff represents a schema difference.
type Diff struct {
	Type    DiffType `json:"type"`
	Model   string   `json:"model,omitempty"`
	Field   string   `json:"field,omitempty"`
	Details string   `json:"details"`
	SQL     string   `json:"sql"`
}

// DiffType represents the type of schema change.
type DiffType string

const (
	DiffCreateModel DiffType = "CREATE_MODEL"
	DiffDropModel   DiffType = "DROP_MODEL"
	DiffAddField    DiffType = "ADD_FIELD"
	DiffDropField   DiffType = "DROP_FIELD"
	DiffAlterField  DiffType = "ALTER_FIELD"
	DiffCreateEnum  DiffType = "CREATE_ENUM"
	DiffDropEnum    DiffType = "DROP_ENUM"
	DiffAddIndex    DiffType = "ADD_INDEX"
	DiffDropIndex   DiffType = "DROP_INDEX"
)

// DiffSchemas compares two schemas and returns the differences.
func DiffSchemas(from, to *schema.Schema) []Diff {
	var diffs []Diff

	fromModels := make(map[string]*schema.Model)
	toModels := make(map[string]*schema.Model)

	if from != nil {
		for i := range from.Models {
			fromModels[from.Models[i].Name] = &from.Models[i]
		}
	}
	for i := range to.Models {
		toModels[to.Models[i].Name] = &to.Models[i]
	}

	// Find new models
	for name, model := range toModels {
		if _, exists := fromModels[name]; !exists {
			diffs = append(diffs, Diff{
				Type:    DiffCreateModel,
				Model:   name,
				Details: fmt.Sprintf("Create model '%s' with %d fields", name, len(model.Fields)),
			})
		}
	}

	// Find dropped models
	if from != nil {
		for name := range fromModels {
			if _, exists := toModels[name]; !exists {
				diffs = append(diffs, Diff{
					Type:    DiffDropModel,
					Model:   name,
					Details: fmt.Sprintf("Drop model '%s'", name),
				})
			}
		}
	}

	// Find field changes in existing models
	for name, toModel := range toModels {
		fromModel, exists := fromModels[name]
		if !exists {
			continue
		}

		fromFields := make(map[string]*schema.Field)
		toFields := make(map[string]*schema.Field)

		for i := range fromModel.Fields {
			if isSchemaColumnField(&fromModel.Fields[i]) {
				fromFields[fromModel.Fields[i].Name] = &fromModel.Fields[i]
			}
		}
		for i := range toModel.Fields {
			if isSchemaColumnField(&toModel.Fields[i]) {
				toFields[toModel.Fields[i].Name] = &toModel.Fields[i]
			}
		}

		// New fields
		for fieldName, field := range toFields {
			if _, exists := fromFields[fieldName]; !exists {
				diffs = append(diffs, Diff{
					Type:    DiffAddField,
					Model:   name,
					Field:   fieldName,
					Details: fmt.Sprintf("Add field '%s' (%s) to model '%s'", fieldName, field.Type.Name, name),
				})
			}
		}

		// Dropped fields
		for fieldName := range fromFields {
			if _, exists := toFields[fieldName]; !exists {
				diffs = append(diffs, Diff{
					Type:    DiffDropField,
					Model:   name,
					Field:   fieldName,
					Details: fmt.Sprintf("Drop field '%s' from model '%s'", fieldName, name),
				})
			}
		}

		// Modified fields
		for fieldName, toField := range toFields {
			fromField, exists := fromFields[fieldName]
			if !exists {
				continue
			}

			if fromField.Type.Name != toField.Type.Name ||
				fromField.Type.IsEnum != toField.Type.IsEnum ||
				fromField.Type.IsScalar != toField.Type.IsScalar ||
				fromField.IsOptional != toField.IsOptional ||
				fromField.IsList != toField.IsList {
				diffs = append(diffs, Diff{
					Type:    DiffAlterField,
					Model:   name,
					Field:   fieldName,
					Details: fmt.Sprintf("Alter field '%s' in model '%s'", fieldName, name),
				})
			}
		}
	}

	// Enum changes
	fromEnums := make(map[string]*schema.Enum)
	toEnums := make(map[string]*schema.Enum)

	if from != nil {
		for i := range from.Enums {
			fromEnums[from.Enums[i].Name] = &from.Enums[i]
		}
	}
	for i := range to.Enums {
		toEnums[to.Enums[i].Name] = &to.Enums[i]
	}

	for name := range toEnums {
		if _, exists := fromEnums[name]; !exists {
			diffs = append(diffs, Diff{
				Type:    DiffCreateEnum,
				Model:   name,
				Details: fmt.Sprintf("Create enum '%s'", name),
			})
		}
	}

	if from != nil {
		for name := range fromEnums {
			if _, exists := toEnums[name]; !exists {
				diffs = append(diffs, Diff{
					Type:    DiffDropEnum,
					Model:   name,
					Details: fmt.Sprintf("Drop enum '%s'", name),
				})
			}
		}
	}

	return diffs
}

// GenerateMigrationSQL generates SQL for a list of diffs.
func GenerateMigrationSQL(diffs []Diff, from, to *schema.Schema, dialect string) (string, error) {
	builder := query.NewBuilder(dialect, to)
	var statements []string
	var unsupported []string

	for _, diff := range diffs {
		switch diff.Type {
		case DiffCreateModel:
			for i := range to.Models {
				if to.Models[i].Name == diff.Model {
					statements = append(statements, builder.BuildCreateTable(&to.Models[i]))
				}
			}
		case DiffDropModel:
			tableName := toSnakeCase(diff.Model)
			if fromModel := from.GetModelByName(diff.Model); fromModel != nil {
				tableName = modelDBName(fromModel)
			}
			statements = append(statements, fmt.Sprintf(`DROP TABLE IF EXISTS %s CASCADE`, quoteIdentifier(tableName)))
		case DiffCreateEnum:
			for i := range to.Enums {
				if to.Enums[i].Name == diff.Model {
					statements = append(statements, builder.BuildCreateEnum(&to.Enums[i]))
				}
			}
		case DiffDropEnum:
			enumName := toSnakeCase(diff.Model)
			if fromEnum := getEnumByName(from, diff.Model); fromEnum != nil {
				enumName = enumDBName(fromEnum)
			}
			statements = append(statements, fmt.Sprintf(`DROP TYPE IF EXISTS %s`, quoteIdentifier(enumName)))
		case DiffAddField:
			model := to.GetModelByName(diff.Model)
			if model == nil {
				return "", fmt.Errorf("model '%s' not found while adding field '%s'", diff.Model, diff.Field)
			}
			field := model.GetFieldByName(diff.Field)
			if field == nil {
				return "", fmt.Errorf("field '%s' not found in model '%s'", diff.Field, diff.Model)
			}
			if !isSchemaColumnField(field) {
				continue
			}
			if field.IsList {
				unsupported = append(unsupported, fmt.Sprintf("%s.%s list fields", diff.Model, diff.Field))
				continue
			}
			statements = append(statements, fmt.Sprintf(
				`ALTER TABLE %s ADD COLUMN %s`,
				quoteIdentifier(modelDBName(model)),
				buildColumnDefinition(field),
			))
		case DiffDropField:
			model := from.GetModelByName(diff.Model)
			if model == nil {
				return "", fmt.Errorf("model '%s' not found while dropping field '%s'", diff.Model, diff.Field)
			}
			field := model.GetFieldByName(diff.Field)
			if field != nil && !isSchemaColumnField(field) {
				continue
			}
			colName := toSnakeCase(diff.Field)
			if field != nil {
				colName = fieldDBName(field)
			}
			statements = append(statements, fmt.Sprintf(
				`ALTER TABLE %s DROP COLUMN IF EXISTS %s`,
				quoteIdentifier(modelDBName(model)),
				quoteIdentifier(colName),
			))
		case DiffAlterField:
			fromModel := from.GetModelByName(diff.Model)
			toModel := to.GetModelByName(diff.Model)
			if fromModel == nil || toModel == nil {
				return "", fmt.Errorf("model '%s' not found while altering field '%s'", diff.Model, diff.Field)
			}
			fromField := fromModel.GetFieldByName(diff.Field)
			toField := toModel.GetFieldByName(diff.Field)
			if fromField == nil || toField == nil {
				return "", fmt.Errorf("field '%s' not found in model '%s' while generating alter SQL", diff.Field, diff.Model)
			}
			if !isSchemaColumnField(fromField) || !isSchemaColumnField(toField) {
				continue
			}
			if fromField.IsList || toField.IsList {
				unsupported = append(unsupported, fmt.Sprintf("%s.%s list field alterations", diff.Model, diff.Field))
				continue
			}

			tableName := quoteIdentifier(modelDBName(toModel))
			columnName := quoteIdentifier(fieldDBName(toField))

			if fromField.Type.Name != toField.Type.Name ||
				fromField.Type.IsEnum != toField.Type.IsEnum ||
				fromField.Type.IsScalar != toField.Type.IsScalar {
				statements = append(statements, fmt.Sprintf(
					`ALTER TABLE %s ALTER COLUMN %s TYPE %s`,
					tableName,
					columnName,
					fieldSQLType(toField),
				))
			}

			if fromField.IsOptional != toField.IsOptional {
				if toField.IsOptional {
					statements = append(statements, fmt.Sprintf(
						`ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL`,
						tableName,
						columnName,
					))
				} else {
					statements = append(statements, fmt.Sprintf(
						`ALTER TABLE %s ALTER COLUMN %s SET NOT NULL`,
						tableName,
						columnName,
					))
				}
			}
		case DiffAddIndex, DiffDropIndex:
			unsupported = append(unsupported, diff.Details)
		}
	}

	if len(unsupported) > 0 {
		return "", fmt.Errorf("unsupported schema changes: %s", strings.Join(unsupported, ", "))
	}
	if len(statements) == 0 {
		return "", fmt.Errorf("schema changes were detected but no SQL statements were generated")
	}

	return strings.Join(statements, ";\n\n") + ";\n", nil
}

// ============================================================================
// Helpers
// ============================================================================

// discoverMigrations scans the migrations directory and returns sorted migration
// directory paths. Each migration dir is expected to contain a migration.sql file.
func discoverMigrations(migrationsDir string) ([]string, error) {
	if _, err := os.Stat(migrationsDir); os.IsNotExist(err) {
		return nil, nil // No migrations directory yet
	}

	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read migrations directory: %w", err)
	}

	var dirs []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		sqlPath := filepath.Join(migrationsDir, entry.Name(), migrationSQLFileName)
		if _, err := os.Stat(sqlPath); err == nil {
			dirs = append(dirs, filepath.Join(migrationsDir, entry.Name()))
		}
	}

	// Sort by directory name (timestamp prefix ensures chronological order)
	sort.Strings(dirs)
	return dirs, nil
}

// extractMigrationName extracts the human-readable name from a migration ID.
// E.g., "20260308120000_add_users" -> "add_users"
func extractMigrationName(migID string) string {
	parts := strings.SplitN(migID, "_", 2)
	if len(parts) == 2 {
		return parts[1]
	}
	return migID
}

// sanitizeMigrationName converts a name into a safe directory-name fragment.
func sanitizeMigrationName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ToLower(name)
	// Replace spaces and special chars with underscores
	var result strings.Builder
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			result.WriteRune(r)
		} else if r == ' ' || r == '-' {
			result.WriteRune('_')
		}
	}
	return result.String()
}

func toSnakeCase(s string) string {
	var result strings.Builder
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			result.WriteRune('_')
		}
		result.WriteRune(r)
	}
	return strings.ToLower(result.String())
}

func loadPreviousSchemaSnapshot(migrationDirs []string) (*schema.Schema, error) {
	for i := len(migrationDirs) - 1; i >= 0; i-- {
		snapshotPath := filepath.Join(migrationDirs[i], migrationSchemaSnapshotFileName)
		if _, err := os.Stat(snapshotPath); err != nil {
			continue
		}

		data, err := os.ReadFile(snapshotPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read schema snapshot '%s': %w", snapshotPath, err)
		}

		parsed, err := schema.Parse(string(data))
		if err != nil {
			return nil, fmt.Errorf("failed to parse schema snapshot '%s': %w", snapshotPath, err)
		}
		schema.ResolveFieldTypes(parsed)
		return parsed, nil
	}

	return nil, nil
}

func generateDevMigrationSQL(from, to *schema.Schema, dialect string) (string, error) {
	if from == nil {
		return buildFullSchemaSQL(to, dialect), nil
	}

	diffs := DiffSchemas(from, to)
	if len(diffs) == 0 {
		return "", nil
	}

	return GenerateMigrationSQL(diffs, from, to, dialect)
}

func buildFullSchemaSQL(s *schema.Schema, dialect string) string {
	builder := query.NewBuilder(dialect, s)
	var statements []string

	for i := range s.Enums {
		statements = append(statements, builder.BuildCreateEnum(&s.Enums[i]))
	}
	for i := range s.Models {
		statements = append(statements, builder.BuildCreateTable(&s.Models[i]))
	}

	return strings.Join(statements, "\n\n") + "\n"
}

func getEnumByName(s *schema.Schema, name string) *schema.Enum {
	if s == nil {
		return nil
	}

	for i := range s.Enums {
		if s.Enums[i].Name == name {
			return &s.Enums[i]
		}
	}

	return nil
}

func isSchemaColumnField(field *schema.Field) bool {
	return field != nil && (field.Type.IsScalar || field.Type.IsEnum)
}

func modelDBName(model *schema.Model) string {
	if model != nil && model.DBName != "" {
		return model.DBName
	}
	if model == nil {
		return ""
	}
	return toSnakeCase(model.Name)
}

func enumDBName(enum *schema.Enum) string {
	if enum != nil && enum.DBName != "" {
		return enum.DBName
	}
	if enum == nil {
		return ""
	}
	return toSnakeCase(enum.Name)
}

func fieldDBName(field *schema.Field) string {
	if field == nil {
		return ""
	}

	for _, attr := range field.Attributes {
		if attr.Name == "map" {
			if name, ok := attr.Args["_0"].(string); ok && name != "" {
				return name
			}
		}
	}

	return toSnakeCase(field.Name)
}

func quoteIdentifier(name string) string {
	return fmt.Sprintf(`"%s"`, strings.ReplaceAll(name, `"`, `""`))
}

func escapeStringLiteral(value string) string {
	return strings.ReplaceAll(value, `'`, `''`)
}

func fieldSQLType(field *schema.Field) string {
	switch {
	case field.DefaultValue != nil &&
		field.DefaultValue.Type == schema.DefaultValueFunction &&
		field.DefaultValue.FuncName == "autoincrement" &&
		field.Type.Name == "BigInt":
		return "BIGSERIAL"
	case field.DefaultValue != nil &&
		field.DefaultValue.Type == schema.DefaultValueFunction &&
		field.DefaultValue.FuncName == "autoincrement":
		return "SERIAL"
	case field.Type.IsEnum:
		return "TEXT"
	}

	switch field.Type.Name {
	case "String":
		return "TEXT"
	case "Int":
		return "INTEGER"
	case "Float":
		return "DOUBLE PRECISION"
	case "Boolean":
		return "BOOLEAN"
	case "DateTime":
		return "TIMESTAMP(3)"
	case "Json":
		return "JSONB"
	case "BigInt":
		return "BIGINT"
	case "Bytes":
		return "BYTEA"
	case "Decimal":
		return "DECIMAL(65,30)"
	default:
		return "TEXT"
	}
}

func buildColumnDefinition(field *schema.Field) string {
	colDef := fmt.Sprintf("%s %s", quoteIdentifier(fieldDBName(field)), fieldSQLType(field))

	if !field.IsOptional && field.DefaultValue == nil {
		colDef += " NOT NULL"
	}

	if field.IsID() {
		colDef += " PRIMARY KEY"
	}

	if field.IsUnique() {
		colDef += " UNIQUE"
	}

	if field.DefaultValue != nil {
		switch field.DefaultValue.Type {
		case schema.DefaultValueLiteral:
			switch v := field.DefaultValue.Value.(type) {
			case string:
				colDef += fmt.Sprintf(" DEFAULT '%s'", escapeStringLiteral(v))
			case bool:
				colDef += fmt.Sprintf(" DEFAULT %t", v)
			default:
				colDef += fmt.Sprintf(" DEFAULT %v", v)
			}
		case schema.DefaultValueFunction:
			switch field.DefaultValue.FuncName {
			case "now":
				colDef += " DEFAULT CURRENT_TIMESTAMP"
			case "uuid":
				colDef += " DEFAULT gen_random_uuid()"
			}
		case schema.DefaultValueEnum:
			colDef += fmt.Sprintf(" DEFAULT '%v'", field.DefaultValue.Value)
		}
	}

	return colDef
}
