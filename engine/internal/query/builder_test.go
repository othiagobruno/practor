package query

import (
	"strings"
	"testing"

	"github.com/practor/practor-engine/internal/schema"
)

func TestBuildCreateTableEscapesDefaultsAndMappedForeignKeys(t *testing.T) {
	s := &schema.Schema{
		Enums: []schema.Enum{
			{
				Name:   "Role",
				DBName: `role"type`,
				Values: []schema.EnumValue{
					{Name: "USER"},
					{Name: "ADMIN", DBName: "admin'value"},
				},
			},
		},
		Models: []schema.Model{
			{
				Name:   "User",
				DBName: `app"users`,
				Fields: []schema.Field{
					{
						Name: "id",
						Type: schema.FieldType{IsScalar: true, Name: "Int"},
						Attributes: []schema.FieldAttribute{
							{Name: "id"},
							{Name: "map", Args: map[string]interface{}{"_0": `user"id`}},
						},
					},
				},
			},
			{
				Name: "Post",
				Fields: []schema.Field{
					{
						Name: "title",
						Type: schema.FieldType{IsScalar: true, Name: "String"},
						DefaultValue: &schema.DefaultValue{
							Type:  schema.DefaultValueLiteral,
							Value: "O'Reilly",
						},
					},
					{
						Name: "role",
						Type: schema.FieldType{Name: "Role", IsEnum: true},
						DefaultValue: &schema.DefaultValue{
							Type:  schema.DefaultValueEnum,
							Value: "ADMIN",
						},
					},
					{
						Name: "authorId",
						Type: schema.FieldType{IsScalar: true, Name: "Int"},
						Attributes: []schema.FieldAttribute{
							{Name: "map", Args: map[string]interface{}{"_0": "author_id"}},
						},
					},
					{
						Name: "author",
						Type: schema.FieldType{Name: "User", IsModel: true},
						Attributes: []schema.FieldAttribute{
							{
								Name: "relation",
								Args: map[string]interface{}{
									"fields":     []interface{}{"authorId"},
									"references": []interface{}{"id"},
								},
							},
						},
					},
				},
			},
		},
	}

	builder := NewBuilder("postgresql", s)
	postSQL := builder.BuildCreateTable(&s.Models[1])
	enumSQL := builder.BuildCreateEnum(&s.Enums[0])

	if !strings.Contains(postSQL, `DEFAULT 'O''Reilly'`) {
		t.Fatalf("expected escaped string default, got:\n%s", postSQL)
	}

	if !strings.Contains(postSQL, `DEFAULT 'admin''value'`) {
		t.Fatalf("expected escaped mapped enum default, got:\n%s", postSQL)
	}

	if !strings.Contains(postSQL, `FOREIGN KEY ("author_id") REFERENCES "app""users" ("user""id")`) {
		t.Fatalf("expected mapped FK constraint, got:\n%s", postSQL)
	}

	if !strings.Contains(enumSQL, `CREATE TYPE "role""type" AS ENUM ('USER', 'admin''value')`) {
		t.Fatalf("expected escaped mapped enum DDL, got:\n%s", enumSQL)
	}
}

func TestCollectParentValuesUsesMappedColumnNames(t *testing.T) {
	s := &schema.Schema{
		Models: []schema.Model{
			{
				Name: "User",
				Fields: []schema.Field{
					{
						Name: "id",
						Type: schema.FieldType{IsScalar: true, Name: "Int"},
						Attributes: []schema.FieldAttribute{
							{Name: "map", Args: map[string]interface{}{"_0": "user_id"}},
						},
					},
				},
			},
		},
	}

	engine := &Engine{
		schema:  s,
		builder: NewBuilder("postgresql", s),
	}

	values, columnName := engine.collectParentValues("User", []map[string]interface{}{
		{"user_id": 7},
		{"user_id": 7},
		{"user_id": 9},
	}, "id")

	if columnName != "user_id" {
		t.Fatalf("expected mapped column name user_id, got %s", columnName)
	}

	if len(values) != 2 || values[0] != 7 || values[1] != 9 {
		t.Fatalf("unexpected collected values: %#v", values)
	}
}

func TestUpdatedAtFieldsReceiveDatabaseDefaultsAndAutoUpdate(t *testing.T) {
	s := &schema.Schema{
		Models: []schema.Model{
			{
				Name: "User",
				Fields: []schema.Field{
					{
						Name: "id",
						Type: schema.FieldType{IsScalar: true, Name: "Int"},
						Attributes: []schema.FieldAttribute{
							{Name: "id"},
						},
					},
					{
						Name: "name",
						Type: schema.FieldType{IsScalar: true, Name: "String"},
					},
					{
						Name: "updatedAt",
						Type: schema.FieldType{IsScalar: true, Name: "DateTime"},
						Attributes: []schema.FieldAttribute{
							{Name: "updatedAt"},
						},
					},
				},
			},
		},
	}

	builder := NewBuilder("postgresql", s)

	createTableSQL := builder.BuildCreateTable(&s.Models[0])
	if !strings.Contains(createTableSQL, `"updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP`) {
		t.Fatalf("expected updatedAt default in DDL, got:\n%s", createTableSQL)
	}

	updateSQL, err := builder.BuildUpdate("User", map[string]interface{}{
		"data": map[string]interface{}{
			"name": "Alice",
		},
		"where": map[string]interface{}{
			"id": 1,
		},
	})
	if err != nil {
		t.Fatalf("BuildUpdate returned error: %v", err)
	}

	if !strings.Contains(updateSQL.SQL, `"updated_at" = CURRENT_TIMESTAMP`) {
		t.Fatalf("expected updatedAt auto-update clause, got:\n%s", updateSQL.SQL)
	}

	upsertSQL, err := builder.BuildUpsert("User", map[string]interface{}{
		"where": map[string]interface{}{
			"id": 1,
		},
		"create": map[string]interface{}{
			"id":   1,
			"name": "Alice",
		},
		"update": map[string]interface{}{
			"name": "Alice Updated",
		},
	})
	if err != nil {
		t.Fatalf("BuildUpsert returned error: %v", err)
	}

	if !strings.Contains(upsertSQL.SQL, `"updated_at" = CURRENT_TIMESTAMP`) {
		t.Fatalf("expected updatedAt auto-update clause in upsert, got:\n%s", upsertSQL.SQL)
	}
}

func TestWhereAndPaginationUseTypedPlaceholders(t *testing.T) {
	s := &schema.Schema{
		Models: []schema.Model{
			{
				Name: "User",
				Fields: []schema.Field{
					{
						Name: "id",
						Type: schema.FieldType{IsScalar: true, Name: "Int"},
						Attributes: []schema.FieldAttribute{
							{Name: "id"},
						},
					},
					{
						Name: "email",
						Type: schema.FieldType{IsScalar: true, Name: "String"},
						Attributes: []schema.FieldAttribute{
							{Name: "unique"},
						},
					},
				},
			},
		},
	}

	builder := NewBuilder("postgresql", s)
	query, err := builder.BuildFindMany("User", map[string]interface{}{
		"where": map[string]interface{}{
			"id": 1,
		},
		"take": 10,
		"skip": 5,
	})
	if err != nil {
		t.Fatalf("BuildFindMany returned error: %v", err)
	}

	if !strings.Contains(query.SQL, `"id" = $1::INTEGER`) {
		t.Fatalf("expected typed integer where placeholder, got:\n%s", query.SQL)
	}

	if !strings.Contains(query.SQL, `LIMIT $2::BIGINT`) {
		t.Fatalf("expected typed limit placeholder, got:\n%s", query.SQL)
	}

	if !strings.Contains(query.SQL, `OFFSET $3::BIGINT`) {
		t.Fatalf("expected typed offset placeholder, got:\n%s", query.SQL)
	}
}
