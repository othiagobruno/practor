package query

import (
	"fmt"
	"strings"

	"github.com/practor/practor-engine/internal/schema"
)

// ============================================================================
// SQL Query Builder — Translates Practor query operations into SQL
// ============================================================================

// Builder generates parameterized SQL from query operations.
type Builder struct {
	dialect    string
	schema     *schema.Schema
	paramIndex int
}

// NewBuilder creates a new query Builder.
func NewBuilder(dialect string, s *schema.Schema) *Builder {
	return &Builder{
		dialect: dialect,
		schema:  s,
	}
}

// BuiltQuery represents a parameterized SQL query.
type BuiltQuery struct {
	SQL    string        `json:"sql"`
	Args   []interface{} `json:"args"`
	Action string        `json:"action"`
}

// placeholder returns the next placeholder for the dialect.
func (b *Builder) placeholder() string {
	b.paramIndex++
	switch b.dialect {
	case "postgresql", "postgres":
		return fmt.Sprintf("$%d", b.paramIndex)
	default:
		return "?"
	}
}

// resetParams resets the parameter counter.
func (b *Builder) resetParams() {
	b.paramIndex = 0
}

// quote quotes an identifier.
func (b *Builder) quote(name string) string {
	return fmt.Sprintf(`"%s"`, strings.ReplaceAll(name, `"`, `""`))
}

// toSnakeCase converts PascalCase/camelCase to snake_case for table/column names.
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

// tableName returns the table name for a model.
func (b *Builder) tableName(model *schema.Model) string {
	return b.quote(b.tableDBName(model))
}

func (b *Builder) tableDBName(model *schema.Model) string {
	if model.DBName != "" {
		return model.DBName
	}

	return toSnakeCase(model.Name)
}

// columnDBName returns the database column name for a field.
func (b *Builder) columnDBName(field *schema.Field) string {
	for _, attr := range field.Attributes {
		if attr.Name == "map" {
			if name, ok := attr.Args["_0"].(string); ok {
				return name
			}
		}
	}

	return toSnakeCase(field.Name)
}

// columnName returns the quoted column name for a field.
func (b *Builder) columnName(field *schema.Field) string {
	return b.quote(b.columnDBName(field))
}

func (b *Builder) enumDBName(enum *schema.Enum) string {
	if enum.DBName != "" {
		return enum.DBName
	}

	return toSnakeCase(enum.Name)
}

func (b *Builder) enumValueDBName(value *schema.EnumValue) string {
	if value.DBName != "" {
		return value.DBName
	}

	return value.Name
}

func escapeStringLiteral(value string) string {
	return strings.ReplaceAll(value, `'`, `''`)
}

func isUpdatedAtField(field *schema.Field) bool {
	return field.HasAttribute("updatedAt")
}

func (b *Builder) castedPlaceholder(sqlType string) string {
	placeholder := b.placeholder()
	if placeholder == "?" || sqlType == "" {
		return placeholder
	}

	return fmt.Sprintf("%s::%s", placeholder, sqlType)
}

func (b *Builder) integerPlaceholder() string {
	return b.castedPlaceholder("BIGINT")
}

func (b *Builder) fieldSQLType(field *schema.Field) string {
	switch {
	case field.Type.IsEnum:
		return "TEXT"
	case field.Type.IsScalar:
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
		}
	}

	return "TEXT"
}

func (b *Builder) fieldPlaceholder(field *schema.Field) string {
	return b.castedPlaceholder(b.fieldSQLType(field))
}

// getModel finds a model by name in the schema.
func (b *Builder) getModel(name string) *schema.Model {
	for i := range b.schema.Models {
		if b.schema.Models[i].Name == name {
			return &b.schema.Models[i]
		}
	}
	return nil
}

func (b *Builder) getEnum(name string) *schema.Enum {
	for i := range b.schema.Enums {
		if b.schema.Enums[i].Name == name {
			return &b.schema.Enums[i]
		}
	}

	return nil
}

func (b *Builder) resolveEnumValueDBName(enumName string, value interface{}) string {
	enum := b.getEnum(enumName)
	name := fmt.Sprintf("%v", value)
	if enum == nil {
		return name
	}

	for i := range enum.Values {
		if enum.Values[i].Name == name {
			return b.enumValueDBName(&enum.Values[i])
		}
	}

	return name
}

// ============================================================================
// SELECT (findMany, findUnique, findFirst)
// ============================================================================

// BuildFindMany generates a SELECT query for findMany.
func (b *Builder) BuildFindMany(modelName string, args map[string]interface{}) (*BuiltQuery, error) {
	b.resetParams()
	model := b.getModel(modelName)
	if model == nil {
		return nil, fmt.Errorf("model '%s' not found", modelName)
	}

	var sqlArgs []interface{}

	// SELECT columns
	selectCols := b.buildSelectColumns(model, args)

	// FROM
	sql := fmt.Sprintf("SELECT %s FROM %s", selectCols, b.tableName(model))

	// WHERE
	if where, ok := args["where"]; ok {
		whereClause, whereArgs, err := b.buildWhere(model, where)
		if err != nil {
			return nil, err
		}
		if whereClause != "" {
			sql += " WHERE " + whereClause
			sqlArgs = append(sqlArgs, whereArgs...)
		}
	}

	// ORDER BY
	if orderBy, ok := args["orderBy"]; ok {
		orderClause := b.buildOrderBy(model, orderBy)
		if orderClause != "" {
			sql += " ORDER BY " + orderClause
		}
	}

	// LIMIT (take)
	if take, ok := args["take"]; ok {
		sql += fmt.Sprintf(" LIMIT %s", b.integerPlaceholder())
		sqlArgs = append(sqlArgs, take)
	}

	// OFFSET (skip)
	if skip, ok := args["skip"]; ok {
		sql += fmt.Sprintf(" OFFSET %s", b.integerPlaceholder())
		sqlArgs = append(sqlArgs, skip)
	}

	return &BuiltQuery{SQL: sql, Args: sqlArgs, Action: "findMany"}, nil
}

// BuildFindUnique generates a SELECT query for findUnique (by ID or unique field).
func (b *Builder) BuildFindUnique(modelName string, args map[string]interface{}) (*BuiltQuery, error) {
	q, err := b.BuildFindMany(modelName, args)
	if err != nil {
		return nil, err
	}
	q.SQL += " LIMIT 1"
	q.Action = "findUnique"
	return q, nil
}

// BuildFindFirst generates a SELECT query for findFirst.
func (b *Builder) BuildFindFirst(modelName string, args map[string]interface{}) (*BuiltQuery, error) {
	q, err := b.BuildFindMany(modelName, args)
	if err != nil {
		return nil, err
	}
	q.SQL += " LIMIT 1"
	q.Action = "findFirst"
	return q, nil
}

// ============================================================================
// INSERT (create, createMany)
// ============================================================================

// BuildCreate generates an INSERT query for a single record.
func (b *Builder) BuildCreate(modelName string, args map[string]interface{}) (*BuiltQuery, error) {
	b.resetParams()
	model := b.getModel(modelName)
	if model == nil {
		return nil, fmt.Errorf("model '%s' not found", modelName)
	}

	data, ok := args["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("'create' requires a 'data' argument")
	}

	var columns []string
	var placeholders []string
	var sqlArgs []interface{}

	for _, field := range model.GetScalarFields() {
		if val, ok := data[field.Name]; ok {
			columns = append(columns, b.columnName(&field))
			placeholders = append(placeholders, b.fieldPlaceholder(&field))
			sqlArgs = append(sqlArgs, val)
		}
	}

	selectCols := b.buildSelectColumns(model, args)

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) RETURNING %s",
		b.tableName(model),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
		selectCols,
	)

	return &BuiltQuery{SQL: sql, Args: sqlArgs, Action: "create"}, nil
}

// BuildCreateMany generates an INSERT query for multiple records.
func (b *Builder) BuildCreateMany(modelName string, args map[string]interface{}) (*BuiltQuery, error) {
	b.resetParams()
	model := b.getModel(modelName)
	if model == nil {
		return nil, fmt.Errorf("model '%s' not found", modelName)
	}

	dataList, ok := args["data"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("'createMany' requires a 'data' array argument")
	}

	if len(dataList) == 0 {
		return &BuiltQuery{SQL: "", Args: nil, Action: "createMany"}, nil
	}

	// Collect all column names from the first record
	firstData, ok := dataList[0].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid data format in createMany")
	}

	var columns []string
	var columnFields []schema.Field
	for _, field := range model.GetScalarFields() {
		if _, ok := firstData[field.Name]; ok {
			columns = append(columns, b.columnName(&field))
			columnFields = append(columnFields, field)
		}
	}

	var valueGroups []string
	var sqlArgs []interface{}

	for _, item := range dataList {
		row, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		var placeholders []string
		for _, field := range columnFields {
			placeholders = append(placeholders, b.fieldPlaceholder(&field))
			sqlArgs = append(sqlArgs, row[field.Name])
		}
		valueGroups = append(valueGroups, fmt.Sprintf("(%s)", strings.Join(placeholders, ", ")))
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		b.tableName(model),
		strings.Join(columns, ", "),
		strings.Join(valueGroups, ", "),
	)

	// skipDuplicates
	if skip, ok := args["skipDuplicates"].(bool); ok && skip {
		sql += " ON CONFLICT DO NOTHING"
	}

	return &BuiltQuery{SQL: sql, Args: sqlArgs, Action: "createMany"}, nil
}

// ============================================================================
// UPDATE (update, updateMany)
// ============================================================================

// BuildUpdate generates an UPDATE query for a single record.
func (b *Builder) BuildUpdate(modelName string, args map[string]interface{}) (*BuiltQuery, error) {
	b.resetParams()
	model := b.getModel(modelName)
	if model == nil {
		return nil, fmt.Errorf("model '%s' not found", modelName)
	}

	data, ok := args["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("'update' requires a 'data' argument")
	}

	var setClauses []string
	var sqlArgs []interface{}

	for _, field := range model.GetScalarFields() {
		if val, ok := data[field.Name]; ok {
			setClauses = append(setClauses, fmt.Sprintf("%s = %s", b.columnName(&field), b.fieldPlaceholder(&field)))
			sqlArgs = append(sqlArgs, val)
		}
	}

	for _, field := range model.GetScalarFields() {
		if isUpdatedAtField(&field) {
			if _, ok := data[field.Name]; !ok {
				setClauses = append(setClauses, fmt.Sprintf("%s = CURRENT_TIMESTAMP", b.columnName(&field)))
			}
		}
	}

	selectCols := b.buildSelectColumns(model, args)

	sql := fmt.Sprintf("UPDATE %s SET %s",
		b.tableName(model),
		strings.Join(setClauses, ", "),
	)

	// WHERE
	if where, ok := args["where"]; ok {
		whereClause, whereArgs, err := b.buildWhere(model, where)
		if err != nil {
			return nil, err
		}
		if whereClause != "" {
			sql += " WHERE " + whereClause
			sqlArgs = append(sqlArgs, whereArgs...)
		}
	}

	sql += " RETURNING " + selectCols

	return &BuiltQuery{SQL: sql, Args: sqlArgs, Action: "update"}, nil
}

// BuildUpdateMany generates an UPDATE query for multiple records.
func (b *Builder) BuildUpdateMany(modelName string, args map[string]interface{}) (*BuiltQuery, error) {
	b.resetParams()
	model := b.getModel(modelName)
	if model == nil {
		return nil, fmt.Errorf("model '%s' not found", modelName)
	}

	data, ok := args["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("'updateMany' requires a 'data' argument")
	}

	var setClauses []string
	var sqlArgs []interface{}

	for _, field := range model.GetScalarFields() {
		if val, ok := data[field.Name]; ok {
			setClauses = append(setClauses, fmt.Sprintf("%s = %s", b.columnName(&field), b.fieldPlaceholder(&field)))
			sqlArgs = append(sqlArgs, val)
		}
	}

	for _, field := range model.GetScalarFields() {
		if isUpdatedAtField(&field) {
			if _, ok := data[field.Name]; !ok {
				setClauses = append(setClauses, fmt.Sprintf("%s = CURRENT_TIMESTAMP", b.columnName(&field)))
			}
		}
	}

	sql := fmt.Sprintf("UPDATE %s SET %s",
		b.tableName(model),
		strings.Join(setClauses, ", "),
	)

	if where, ok := args["where"]; ok {
		whereClause, whereArgs, err := b.buildWhere(model, where)
		if err != nil {
			return nil, err
		}
		if whereClause != "" {
			sql += " WHERE " + whereClause
			sqlArgs = append(sqlArgs, whereArgs...)
		}
	}

	return &BuiltQuery{SQL: sql, Args: sqlArgs, Action: "updateMany"}, nil
}

// ============================================================================
// DELETE (delete, deleteMany)
// ============================================================================

// BuildDelete generates a DELETE query for a single record.
func (b *Builder) BuildDelete(modelName string, args map[string]interface{}) (*BuiltQuery, error) {
	b.resetParams()
	model := b.getModel(modelName)
	if model == nil {
		return nil, fmt.Errorf("model '%s' not found", modelName)
	}

	selectCols := b.buildSelectColumns(model, args)

	sql := fmt.Sprintf("DELETE FROM %s", b.tableName(model))

	var sqlArgs []interface{}
	if where, ok := args["where"]; ok {
		whereClause, whereArgs, err := b.buildWhere(model, where)
		if err != nil {
			return nil, err
		}
		if whereClause != "" {
			sql += " WHERE " + whereClause
			sqlArgs = append(sqlArgs, whereArgs...)
		}
	}

	sql += " RETURNING " + selectCols

	return &BuiltQuery{SQL: sql, Args: sqlArgs, Action: "delete"}, nil
}

// BuildDeleteMany generates a DELETE query for multiple records.
func (b *Builder) BuildDeleteMany(modelName string, args map[string]interface{}) (*BuiltQuery, error) {
	b.resetParams()
	model := b.getModel(modelName)
	if model == nil {
		return nil, fmt.Errorf("model '%s' not found", modelName)
	}

	sql := fmt.Sprintf("DELETE FROM %s", b.tableName(model))

	var sqlArgs []interface{}
	if where, ok := args["where"]; ok {
		whereClause, whereArgs, err := b.buildWhere(model, where)
		if err != nil {
			return nil, err
		}
		if whereClause != "" {
			sql += " WHERE " + whereClause
			sqlArgs = append(sqlArgs, whereArgs...)
		}
	}

	return &BuiltQuery{SQL: sql, Args: sqlArgs, Action: "deleteMany"}, nil
}

// ============================================================================
// UPSERT
// ============================================================================

// BuildUpsert generates an INSERT ... ON CONFLICT ... UPDATE query.
func (b *Builder) BuildUpsert(modelName string, args map[string]interface{}) (*BuiltQuery, error) {
	b.resetParams()
	model := b.getModel(modelName)
	if model == nil {
		return nil, fmt.Errorf("model '%s' not found", modelName)
	}

	createData, ok := args["create"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("'upsert' requires a 'create' argument")
	}

	updateData, ok := args["update"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("'upsert' requires an 'update' argument")
	}

	where, ok := args["where"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("'upsert' requires a 'where' argument")
	}

	// Build INSERT part
	var columns []string
	var placeholders []string
	var sqlArgs []interface{}

	for _, field := range model.GetScalarFields() {
		if val, ok := createData[field.Name]; ok {
			columns = append(columns, b.columnName(&field))
			placeholders = append(placeholders, b.fieldPlaceholder(&field))
			sqlArgs = append(sqlArgs, val)
		}
	}

	// Determine conflict column from where clause
	var conflictCols []string
	for key := range where {
		field := model.GetFieldByName(key)
		if field != nil {
			conflictCols = append(conflictCols, b.columnName(field))
		}
	}

	// Build UPDATE part
	var setClauses []string
	for _, field := range model.GetScalarFields() {
		if val, ok := updateData[field.Name]; ok {
			setClauses = append(setClauses, fmt.Sprintf("%s = %s", b.columnName(&field), b.fieldPlaceholder(&field)))
			sqlArgs = append(sqlArgs, val)
		}
	}

	for _, field := range model.GetScalarFields() {
		if isUpdatedAtField(&field) {
			if _, ok := updateData[field.Name]; !ok {
				setClauses = append(setClauses, fmt.Sprintf("%s = CURRENT_TIMESTAMP", b.columnName(&field)))
			}
		}
	}

	selectCols := b.buildSelectColumns(model, args)

	sql := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s RETURNING %s",
		b.tableName(model),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(conflictCols, ", "),
		strings.Join(setClauses, ", "),
		selectCols,
	)

	return &BuiltQuery{SQL: sql, Args: sqlArgs, Action: "upsert"}, nil
}

// ============================================================================
// AGGREGATE (count, aggregate, groupBy)
// ============================================================================

// BuildCount generates a COUNT query.
func (b *Builder) BuildCount(modelName string, args map[string]interface{}) (*BuiltQuery, error) {
	b.resetParams()
	model := b.getModel(modelName)
	if model == nil {
		return nil, fmt.Errorf("model '%s' not found", modelName)
	}

	sql := fmt.Sprintf("SELECT COUNT(*) as count FROM %s", b.tableName(model))

	var sqlArgs []interface{}
	if where, ok := args["where"]; ok {
		whereClause, whereArgs, err := b.buildWhere(model, where)
		if err != nil {
			return nil, err
		}
		if whereClause != "" {
			sql += " WHERE " + whereClause
			sqlArgs = append(sqlArgs, whereArgs...)
		}
	}

	return &BuiltQuery{SQL: sql, Args: sqlArgs, Action: "count"}, nil
}

// BuildAggregate generates an aggregate query (_count, _avg, _sum, _min, _max).
func (b *Builder) BuildAggregate(modelName string, args map[string]interface{}) (*BuiltQuery, error) {
	b.resetParams()
	model := b.getModel(modelName)
	if model == nil {
		return nil, fmt.Errorf("model '%s' not found", modelName)
	}

	var selectParts []string

	// _count
	if countArgs, ok := args["_count"]; ok {
		if countMap, ok := countArgs.(map[string]interface{}); ok {
			for fieldName, include := range countMap {
				if include == true {
					field := model.GetFieldByName(fieldName)
					if field != nil {
						selectParts = append(selectParts,
							fmt.Sprintf("COUNT(%s) as \"_count_%s\"", b.columnName(field), fieldName))
					}
				}
			}
		} else if countArgs == true {
			selectParts = append(selectParts, "COUNT(*) as \"_count__all\"")
		}
	}

	// _avg, _sum, _min, _max
	for _, aggOp := range []string{"_avg", "_sum", "_min", "_max"} {
		if aggArgs, ok := args[aggOp].(map[string]interface{}); ok {
			for fieldName, include := range aggArgs {
				if include == true {
					field := model.GetFieldByName(fieldName)
					if field != nil {
						op := strings.TrimPrefix(aggOp, "_")
						selectParts = append(selectParts,
							fmt.Sprintf("%s(%s) as \"%s_%s\"",
								strings.ToUpper(op), b.columnName(field), aggOp, fieldName))
					}
				}
			}
		}
	}

	if len(selectParts) == 0 {
		selectParts = append(selectParts, "COUNT(*) as \"_count__all\"")
	}

	sql := fmt.Sprintf("SELECT %s FROM %s", strings.Join(selectParts, ", "), b.tableName(model))

	var sqlArgs []interface{}
	if where, ok := args["where"]; ok {
		whereClause, whereArgs, err := b.buildWhere(model, where)
		if err != nil {
			return nil, err
		}
		if whereClause != "" {
			sql += " WHERE " + whereClause
			sqlArgs = append(sqlArgs, whereArgs...)
		}
	}

	return &BuiltQuery{SQL: sql, Args: sqlArgs, Action: "aggregate"}, nil
}

// BuildGroupBy generates a GROUP BY query.
func (b *Builder) BuildGroupBy(modelName string, args map[string]interface{}) (*BuiltQuery, error) {
	b.resetParams()
	model := b.getModel(modelName)
	if model == nil {
		return nil, fmt.Errorf("model '%s' not found", modelName)
	}

	byFields, ok := args["by"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("'groupBy' requires a 'by' argument")
	}

	var groupCols []string
	var selectParts []string

	for _, fieldName := range byFields {
		name := fmt.Sprintf("%v", fieldName)
		field := model.GetFieldByName(name)
		if field != nil {
			col := b.columnName(field)
			groupCols = append(groupCols, col)
			selectParts = append(selectParts, col)
		}
	}

	// Add aggregate operations
	for _, aggOp := range []string{"_count", "_avg", "_sum", "_min", "_max"} {
		if aggArgs, ok := args[aggOp]; ok {
			if aggArgs == true && aggOp == "_count" {
				selectParts = append(selectParts, "COUNT(*) as \"_count\"")
			} else if aggMap, ok := aggArgs.(map[string]interface{}); ok {
				for fieldName, include := range aggMap {
					if include == true {
						field := model.GetFieldByName(fieldName)
						if field != nil {
							op := strings.TrimPrefix(aggOp, "_")
							selectParts = append(selectParts,
								fmt.Sprintf("%s(%s) as \"%s_%s\"",
									strings.ToUpper(op), b.columnName(field), aggOp, fieldName))
						}
					}
				}
			}
		}
	}

	sql := fmt.Sprintf("SELECT %s FROM %s", strings.Join(selectParts, ", "), b.tableName(model))

	var sqlArgs []interface{}
	if where, ok := args["where"]; ok {
		whereClause, whereArgs, err := b.buildWhere(model, where)
		if err != nil {
			return nil, err
		}
		if whereClause != "" {
			sql += " WHERE " + whereClause
			sqlArgs = append(sqlArgs, whereArgs...)
		}
	}

	sql += " GROUP BY " + strings.Join(groupCols, ", ")

	// HAVING
	if having, ok := args["having"]; ok {
		havingClause, havingArgs, err := b.buildWhere(model, having)
		if err != nil {
			return nil, err
		}
		if havingClause != "" {
			sql += " HAVING " + havingClause
			sqlArgs = append(sqlArgs, havingArgs...)
		}
	}

	// ORDER BY
	if orderBy, ok := args["orderBy"]; ok {
		orderClause := b.buildOrderBy(model, orderBy)
		if orderClause != "" {
			sql += " ORDER BY " + orderClause
		}
	}

	return &BuiltQuery{SQL: sql, Args: sqlArgs, Action: "groupBy"}, nil
}

// ============================================================================
// Cursor-based pagination builder
// ============================================================================

// BuildFindManyCursorPaginated generates a SELECT with cursor-based WHERE filtering.
// It expects args to contain:
//   - "cursor": map[string]interface{} (optional — omit for first page)
//   - "take":   float64              (limit, already incremented by +1 by the caller)
//   - "where":  map[string]interface{} (optional — additional filters)
//   - "orderBy": map[string]interface{} or []interface{} (required — determines direction)
//   - "select": map[string]interface{} (optional)
func (b *Builder) BuildFindManyCursorPaginated(modelName string, args map[string]interface{}) (*BuiltQuery, error) {
	b.resetParams()
	model := b.getModel(modelName)
	if model == nil {
		return nil, fmt.Errorf("model '%s' not found", modelName)
	}

	var sqlArgs []interface{}

	// SELECT columns
	selectCols := b.buildSelectColumns(model, args)

	// FROM
	sqlStr := fmt.Sprintf("SELECT %s FROM %s", selectCols, b.tableName(model))

	// Collect WHERE conditions: user-provided + cursor condition
	var allConditions []string

	// User-provided WHERE
	if where, ok := args["where"]; ok {
		whereClause, whereArgs, err := b.buildWhere(model, where)
		if err != nil {
			return nil, err
		}
		if whereClause != "" {
			allConditions = append(allConditions, whereClause)
			sqlArgs = append(sqlArgs, whereArgs...)
		}
	}

	// Cursor WHERE: field > value (ASC) or field < value (DESC)
	if cursor, ok := args["cursor"].(map[string]interface{}); ok && len(cursor) > 0 {
		direction := b.extractCursorDirection(args)
		op := ">"
		if direction == "DESC" {
			op = "<"
		}

		for fieldName, cursorValue := range cursor {
			field := model.GetFieldByName(fieldName)
			if field == nil {
				return nil, fmt.Errorf("cursor field '%s' not found in model '%s'", fieldName, modelName)
			}
			allConditions = append(allConditions, fmt.Sprintf("%s %s %s", b.columnName(field), op, b.fieldPlaceholder(field)))
			sqlArgs = append(sqlArgs, cursorValue)
		}
	}

	if len(allConditions) > 0 {
		sqlStr += " WHERE " + strings.Join(allConditions, " AND ")
	}

	// ORDER BY
	if orderBy, ok := args["orderBy"]; ok {
		orderClause := b.buildOrderBy(model, orderBy)
		if orderClause != "" {
			sqlStr += " ORDER BY " + orderClause
		}
	}

	// LIMIT (take, already +1 from caller)
	if take, ok := args["take"]; ok {
		sqlStr += fmt.Sprintf(" LIMIT %s", b.integerPlaceholder())
		sqlArgs = append(sqlArgs, take)
	}

	return &BuiltQuery{SQL: sqlStr, Args: sqlArgs, Action: "findManyCursorPaginated"}, nil
}

// extractCursorDirection inspects orderBy to determine cursor scan direction.
func (b *Builder) extractCursorDirection(args map[string]interface{}) string {
	if orderBy, ok := args["orderBy"]; ok {
		switch ob := orderBy.(type) {
		case map[string]interface{}:
			for _, dir := range ob {
				if strings.ToUpper(fmt.Sprintf("%v", dir)) == "DESC" {
					return "DESC"
				}
			}
		case []interface{}:
			if len(ob) > 0 {
				if first, ok := ob[0].(map[string]interface{}); ok {
					for _, dir := range first {
						if strings.ToUpper(fmt.Sprintf("%v", dir)) == "DESC" {
							return "DESC"
						}
					}
				}
			}
		}
	}
	return "ASC"
}

// ============================================================================
// WHERE clause builder
// ============================================================================

func (b *Builder) buildWhere(model *schema.Model, where interface{}) (string, []interface{}, error) {
	whereMap, ok := where.(map[string]interface{})
	if !ok {
		return "", nil, fmt.Errorf("invalid where clause format")
	}

	var conditions []string
	var args []interface{}

	for key, value := range whereMap {
		switch key {
		case "AND":
			andClause, andArgs, err := b.buildLogicalOp(model, value, "AND")
			if err != nil {
				return "", nil, err
			}
			conditions = append(conditions, andClause)
			args = append(args, andArgs...)

		case "OR":
			orClause, orArgs, err := b.buildLogicalOp(model, value, "OR")
			if err != nil {
				return "", nil, err
			}
			conditions = append(conditions, orClause)
			args = append(args, orArgs...)

		case "NOT":
			notClause, notArgs, err := b.buildWhere(model, value)
			if err != nil {
				return "", nil, err
			}
			conditions = append(conditions, fmt.Sprintf("NOT (%s)", notClause))
			args = append(args, notArgs...)

		default:
			// Field-level condition
			field := model.GetFieldByName(key)
			if field == nil {
				return "", nil, fmt.Errorf("field '%s' not found in model '%s'", key, model.Name)
			}

			condStr, condArgs, err := b.buildFieldCondition(field, value)
			if err != nil {
				return "", nil, err
			}
			conditions = append(conditions, condStr)
			args = append(args, condArgs...)
		}
	}

	return strings.Join(conditions, " AND "), args, nil
}

func (b *Builder) buildLogicalOp(model *schema.Model, value interface{}, op string) (string, []interface{}, error) {
	list, ok := value.([]interface{})
	if !ok {
		return "", nil, fmt.Errorf("%s requires an array", op)
	}

	var conditions []string
	var args []interface{}

	for _, item := range list {
		clause, itemArgs, err := b.buildWhere(model, item)
		if err != nil {
			return "", nil, err
		}
		conditions = append(conditions, "("+clause+")")
		args = append(args, itemArgs...)
	}

	return "(" + strings.Join(conditions, " "+op+" ") + ")", args, nil
}

func (b *Builder) buildFieldCondition(field *schema.Field, value interface{}) (string, []interface{}, error) {
	col := b.columnName(field)

	// Direct value comparison (equals shorthand)
	if !isMap(value) {
		if value == nil {
			return fmt.Sprintf("%s IS NULL", col), nil, nil
		}
		return fmt.Sprintf("%s = %s", col, b.fieldPlaceholder(field)), []interface{}{value}, nil
	}

	// Operator-based comparison
	opMap := value.(map[string]interface{})
	var conditions []string
	var args []interface{}

	for op, val := range opMap {
		switch op {
		case "equals":
			if val == nil {
				conditions = append(conditions, fmt.Sprintf("%s IS NULL", col))
			} else {
				conditions = append(conditions, fmt.Sprintf("%s = %s", col, b.fieldPlaceholder(field)))
				args = append(args, val)
			}
		case "not":
			if val == nil {
				conditions = append(conditions, fmt.Sprintf("%s IS NOT NULL", col))
			} else if isMap(val) {
				// Nested not: { not: { equals: ... } }
				subCond, subArgs, err := b.buildFieldCondition(field, val)
				if err != nil {
					return "", nil, err
				}
				conditions = append(conditions, fmt.Sprintf("NOT (%s)", subCond))
				args = append(args, subArgs...)
			} else {
				conditions = append(conditions, fmt.Sprintf("%s != %s", col, b.fieldPlaceholder(field)))
				args = append(args, val)
			}
		case "in":
			inVals, ok := val.([]interface{})
			if !ok {
				return "", nil, fmt.Errorf("'in' requires an array")
			}
			var placeholders []string
			for _, v := range inVals {
				placeholders = append(placeholders, b.fieldPlaceholder(field))
				args = append(args, v)
			}
			conditions = append(conditions, fmt.Sprintf("%s IN (%s)", col, strings.Join(placeholders, ", ")))
		case "notIn":
			inVals, ok := val.([]interface{})
			if !ok {
				return "", nil, fmt.Errorf("'notIn' requires an array")
			}
			var placeholders []string
			for _, v := range inVals {
				placeholders = append(placeholders, b.fieldPlaceholder(field))
				args = append(args, v)
			}
			conditions = append(conditions, fmt.Sprintf("%s NOT IN (%s)", col, strings.Join(placeholders, ", ")))
		case "lt":
			conditions = append(conditions, fmt.Sprintf("%s < %s", col, b.fieldPlaceholder(field)))
			args = append(args, val)
		case "lte":
			conditions = append(conditions, fmt.Sprintf("%s <= %s", col, b.fieldPlaceholder(field)))
			args = append(args, val)
		case "gt":
			conditions = append(conditions, fmt.Sprintf("%s > %s", col, b.fieldPlaceholder(field)))
			args = append(args, val)
		case "gte":
			conditions = append(conditions, fmt.Sprintf("%s >= %s", col, b.fieldPlaceholder(field)))
			args = append(args, val)
		case "contains":
			conditions = append(conditions, fmt.Sprintf("%s LIKE %s", col, b.fieldPlaceholder(field)))
			args = append(args, fmt.Sprintf("%%%v%%", val))
		case "startsWith":
			conditions = append(conditions, fmt.Sprintf("%s LIKE %s", col, b.fieldPlaceholder(field)))
			args = append(args, fmt.Sprintf("%v%%", val))
		case "endsWith":
			conditions = append(conditions, fmt.Sprintf("%s LIKE %s", col, b.fieldPlaceholder(field)))
			args = append(args, fmt.Sprintf("%%%v", val))
		case "mode":
			// insensitive mode — handled at the caller level
		default:
			return "", nil, fmt.Errorf("unknown operator '%s'", op)
		}
	}

	return strings.Join(conditions, " AND "), args, nil
}

// ============================================================================
// SELECT columns builder
// ============================================================================

func (b *Builder) buildSelectColumns(model *schema.Model, args map[string]interface{}) string {
	// If 'select' is specified, only return those fields
	if selectMap, ok := args["select"].(map[string]interface{}); ok {
		var cols []string
		for fieldName, include := range selectMap {
			if include == true {
				field := model.GetFieldByName(fieldName)
				if field != nil && (field.Type.IsScalar || field.Type.IsEnum) {
					cols = append(cols, b.columnName(field))
				}
			}
		}
		if len(cols) > 0 {
			return strings.Join(cols, ", ")
		}
	}

	// Default: return all scalar fields
	var cols []string
	for _, field := range model.GetScalarFields() {
		cols = append(cols, b.columnName(&field))
	}
	return strings.Join(cols, ", ")
}

// ============================================================================
// Relation query builders
// ============================================================================

// BuildRelationQuery generates a SELECT for loading related rows using WHERE fk IN (...).
//
// Used for HasMany / HasOne directions where the FK lives on the TARGET table.
// Example: Loading posts for users → SELECT * FROM "post" WHERE "author_id" IN ($1, $2, ...)
func (b *Builder) BuildRelationQuery(
	targetModelName string,
	fkFieldName string,
	parentIDs []interface{},
	nestedArgs map[string]interface{},
) (*BuiltQuery, error) {
	b.resetParams()
	targetModel := b.getModel(targetModelName)
	if targetModel == nil {
		return nil, fmt.Errorf("relation target model '%s' not found", targetModelName)
	}

	fkField := targetModel.GetFieldByName(fkFieldName)
	if fkField == nil {
		return nil, fmt.Errorf("FK field '%s' not found in model '%s'", fkFieldName, targetModelName)
	}

	// SELECT columns (respecting nested select if provided)
	selectCols := b.buildSelectColumns(targetModel, nestedArgs)

	// Always include the FK column so we can group results by parent
	fkCol := b.columnName(fkField)
	if !strings.Contains(selectCols, fkCol) {
		selectCols = fkCol + ", " + selectCols
	}

	// WHERE fk IN (...)
	var placeholders []string
	var sqlArgs []interface{}
	for _, id := range parentIDs {
		placeholders = append(placeholders, b.fieldPlaceholder(fkField))
		sqlArgs = append(sqlArgs, id)
	}

	sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s IN (%s)",
		selectCols,
		b.tableName(targetModel),
		fkCol,
		strings.Join(placeholders, ", "),
	)

	// Additional WHERE conditions from nested args
	if where, ok := nestedArgs["where"]; ok {
		whereClause, whereArgs, err := b.buildWhere(targetModel, where)
		if err != nil {
			return nil, err
		}
		if whereClause != "" {
			sql += " AND " + whereClause
			sqlArgs = append(sqlArgs, whereArgs...)
		}
	}

	// ORDER BY
	if orderBy, ok := nestedArgs["orderBy"]; ok {
		orderClause := b.buildOrderBy(targetModel, orderBy)
		if orderClause != "" {
			sql += " ORDER BY " + orderClause
		}
	}

	// LIMIT (take)
	if take, ok := nestedArgs["take"]; ok {
		sql += fmt.Sprintf(" LIMIT %s", b.integerPlaceholder())
		sqlArgs = append(sqlArgs, take)
	}

	// OFFSET (skip)
	if skip, ok := nestedArgs["skip"]; ok {
		sql += fmt.Sprintf(" OFFSET %s", b.integerPlaceholder())
		sqlArgs = append(sqlArgs, skip)
	}

	return &BuiltQuery{SQL: sql, Args: sqlArgs, Action: "relationQuery"}, nil
}

// BuildBelongsToQuery generates a SELECT for loading parent rows using WHERE pk IN (...).
//
// Used for BelongsTo direction where the FK lives on the SOURCE table.
// Example: Loading author for posts → SELECT * FROM "user" WHERE "id" IN ($1, $2, ...)
func (b *Builder) BuildBelongsToQuery(
	targetModelName string,
	refFieldName string,
	parentFKValues []interface{},
	nestedArgs map[string]interface{},
) (*BuiltQuery, error) {
	b.resetParams()
	targetModel := b.getModel(targetModelName)
	if targetModel == nil {
		return nil, fmt.Errorf("relation target model '%s' not found", targetModelName)
	}

	refField := targetModel.GetFieldByName(refFieldName)
	if refField == nil {
		return nil, fmt.Errorf("reference field '%s' not found in model '%s'", refFieldName, targetModelName)
	}

	selectCols := b.buildSelectColumns(targetModel, nestedArgs)

	// Always include the reference column for mapping
	refCol := b.columnName(refField)
	if !strings.Contains(selectCols, refCol) {
		selectCols = refCol + ", " + selectCols
	}

	var placeholders []string
	var sqlArgs []interface{}
	for _, val := range parentFKValues {
		placeholders = append(placeholders, b.fieldPlaceholder(refField))
		sqlArgs = append(sqlArgs, val)
	}

	sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s IN (%s)",
		selectCols,
		b.tableName(targetModel),
		refCol,
		strings.Join(placeholders, ", "),
	)

	// Additional WHERE
	if where, ok := nestedArgs["where"]; ok {
		whereClause, whereArgs, err := b.buildWhere(targetModel, where)
		if err != nil {
			return nil, err
		}
		if whereClause != "" {
			sql += " AND " + whereClause
			sqlArgs = append(sqlArgs, whereArgs...)
		}
	}

	// ORDER BY
	if orderBy, ok := nestedArgs["orderBy"]; ok {
		orderClause := b.buildOrderBy(targetModel, orderBy)
		if orderClause != "" {
			sql += " ORDER BY " + orderClause
		}
	}

	return &BuiltQuery{SQL: sql, Args: sqlArgs, Action: "relationQuery"}, nil
}

// ============================================================================
// ORDER BY builder
// ============================================================================

func (b *Builder) buildOrderBy(model *schema.Model, orderBy interface{}) string {
	var parts []string

	switch ob := orderBy.(type) {
	case map[string]interface{}:
		for fieldName, direction := range ob {
			field := model.GetFieldByName(fieldName)
			if field != nil {
				dir := strings.ToUpper(fmt.Sprintf("%v", direction))
				if dir != "ASC" && dir != "DESC" {
					dir = "ASC"
				}
				parts = append(parts, fmt.Sprintf("%s %s", b.columnName(field), dir))
			}
		}
	case []interface{}:
		for _, item := range ob {
			if itemMap, ok := item.(map[string]interface{}); ok {
				for fieldName, direction := range itemMap {
					field := model.GetFieldByName(fieldName)
					if field != nil {
						dir := strings.ToUpper(fmt.Sprintf("%v", direction))
						if dir != "ASC" && dir != "DESC" {
							dir = "ASC"
						}
						parts = append(parts, fmt.Sprintf("%s %s", b.columnName(field), dir))
					}
				}
			}
		}
	}

	return strings.Join(parts, ", ")
}

// ============================================================================
// DDL: CREATE TABLE
// ============================================================================

// BuildCreateTable generates a CREATE TABLE SQL statement.
func (b *Builder) BuildCreateTable(model *schema.Model) string {
	typeMappings := map[string]string{
		"String":   "TEXT",
		"Int":      "INTEGER",
		"Float":    "DOUBLE PRECISION",
		"Boolean":  "BOOLEAN",
		"DateTime": "TIMESTAMP(3)",
		"Json":     "JSONB",
		"BigInt":   "BIGINT",
		"Bytes":    "BYTEA",
		"Decimal":  "DECIMAL(65,30)",
	}

	var columns []string
	var constraints []string

	for _, field := range model.Fields {
		// Skip relation fields (they don't have columns)
		if field.Type.IsModel {
			continue
		}

		col := b.columnName(&field)
		var colType string

		// Check for autoincrement
		if field.DefaultValue != nil && field.DefaultValue.Type == schema.DefaultValueFunction &&
			field.DefaultValue.FuncName == "autoincrement" {
			if field.Type.Name == "BigInt" {
				colType = "BIGSERIAL"
			} else {
				colType = "SERIAL"
			}
		} else if field.Type.IsEnum {
			colType = "TEXT" // Enums stored as TEXT with CHECK constraint
		} else {
			ct, ok := typeMappings[field.Type.Name]
			if !ok {
				colType = "TEXT"
			} else {
				colType = ct
			}
		}

		colDef := fmt.Sprintf("%s %s", col, colType)

		// NOT NULL (unless optional or has default)
		if !field.IsOptional && field.DefaultValue == nil &&
			!(field.DefaultValue != nil && field.DefaultValue.FuncName == "autoincrement") {
			colDef += " NOT NULL"
		}

		// PRIMARY KEY
		if field.IsID() {
			colDef += " PRIMARY KEY"
		}

		// UNIQUE
		if field.IsUnique() {
			colDef += " UNIQUE"
		}

		// DEFAULT
		if isUpdatedAtField(&field) && field.DefaultValue == nil {
			colDef += " DEFAULT CURRENT_TIMESTAMP"
		} else if field.DefaultValue != nil {
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
				case "cuid":
					// CUID generated in application layer
				}
			case schema.DefaultValueEnum:
				colDef += fmt.Sprintf(
					" DEFAULT '%s'",
					escapeStringLiteral(b.resolveEnumValueDBName(field.Type.Name, field.DefaultValue.Value)),
				)
			}
		}

		columns = append(columns, colDef)
	}

	// Add foreign key constraints for relation fields
	for _, field := range model.Fields {
		if !field.Type.IsModel || field.IsList {
			continue
		}
		relAttr := field.GetRelationAttribute()
		if relAttr == nil {
			continue
		}

		if fieldsArg, ok := relAttr.Args["fields"]; ok {
			if refsArg, ok := relAttr.Args["references"]; ok {
				targetModel := b.getModel(field.Type.Name)
				if targetModel == nil {
					continue
				}
				fkFields := toStringSlice(fieldsArg)
				refFields := toStringSlice(refsArg)
				if len(fkFields) > 0 && len(refFields) > 0 {
					var fkCols, refCols []string
					for _, f := range fkFields {
						fkField := model.GetFieldByName(f)
						if fkField != nil {
							fkCols = append(fkCols, b.columnName(fkField))
						}
					}
					for _, r := range refFields {
						refField := targetModel.GetFieldByName(r)
						if refField != nil {
							refCols = append(refCols, b.columnName(refField))
						}
					}
					if len(fkCols) == 0 || len(refCols) == 0 {
						continue
					}
					constraints = append(constraints,
						fmt.Sprintf("FOREIGN KEY (%s) REFERENCES %s (%s)",
							strings.Join(fkCols, ", "),
							b.tableName(targetModel),
							strings.Join(refCols, ", ")))
				}
			}
		}
	}

	allParts := append(columns, constraints...)

	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n  %s\n)",
		b.tableName(model),
		strings.Join(allParts, ",\n  "))
}

// BuildCreateEnum generates CREATE TYPE ... AS ENUM SQL for PostgreSQL.
func (b *Builder) BuildCreateEnum(enum *schema.Enum) string {
	var values []string
	for _, v := range enum.Values {
		values = append(values, fmt.Sprintf("'%s'", escapeStringLiteral(b.enumValueDBName(&v))))
	}
	return fmt.Sprintf("DO $$ BEGIN\n  CREATE TYPE %s AS ENUM (%s);\nEXCEPTION\n  WHEN duplicate_object THEN null;\nEND $$",
		b.quote(b.enumDBName(enum)), strings.Join(values, ", "))
}

// ============================================================================
// Utility functions
// ============================================================================

func isMap(v interface{}) bool {
	_, ok := v.(map[string]interface{})
	return ok
}

func toStringSlice(v interface{}) []string {
	if list, ok := v.([]interface{}); ok {
		var result []string
		for _, item := range list {
			result = append(result, fmt.Sprintf("%v", item))
		}
		return result
	}
	if s, ok := v.(string); ok {
		return []string{s}
	}
	return nil
}
