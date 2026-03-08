package migration

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/practor/practor-engine/internal/schema"
)

func parseTestSchema(t *testing.T, input string) *schema.Schema {
	t.Helper()

	parsed, err := schema.Parse(strings.TrimSpace(input))
	if err != nil {
		t.Fatalf("parse schema: %v", err)
	}

	schema.ResolveFieldTypes(parsed)
	return parsed
}

func TestGenerateMigrationSQLUsesAlterTableForScalarDiffs(t *testing.T) {
	from := parseTestSchema(t, `
datasource db {
  provider = "postgresql"
  url      = "postgresql://localhost/test"
}

model User {
  id    Int    @id @default(autoincrement())
  email String
}
`)

	to := parseTestSchema(t, `
datasource db {
  provider = "postgresql"
  url      = "postgresql://localhost/test"
}

model User {
  id    Int     @id @default(autoincrement())
  email String?
  name  String?
}
`)

	sql, err := GenerateMigrationSQL(DiffSchemas(from, to), from, to, "postgresql")
	if err != nil {
		t.Fatalf("generate migration SQL: %v", err)
	}

	if !strings.Contains(sql, `ALTER TABLE "user" ALTER COLUMN "email" DROP NOT NULL`) {
		t.Fatalf("expected NOT NULL alteration in SQL, got:\n%s", sql)
	}
	if !strings.Contains(sql, `ALTER TABLE "user" ADD COLUMN "name" TEXT`) {
		t.Fatalf("expected ADD COLUMN in SQL, got:\n%s", sql)
	}
}

func TestDiffSchemasIgnoresVirtualRelationFields(t *testing.T) {
	from := parseTestSchema(t, `
datasource db {
  provider = "postgresql"
  url      = "postgresql://localhost/test"
}

model User {
  id Int @id @default(autoincrement())
}

model Post {
  id       Int  @id @default(autoincrement())
  authorId Int
  author   User @relation(fields: [authorId], references: [id])
}
`)

	to := parseTestSchema(t, `
datasource db {
  provider = "postgresql"
  url      = "postgresql://localhost/test"
}

model User {
  id    Int    @id @default(autoincrement())
  posts Post[]
}

model Post {
  id       Int  @id @default(autoincrement())
  authorId Int
  author   User @relation(fields: [authorId], references: [id])
}
`)

	diffs := DiffSchemas(from, to)
	if len(diffs) != 0 {
		t.Fatalf("expected no diffs for virtual relation field, got %#v", diffs)
	}
}

func TestLoadPreviousSchemaSnapshotReadsNewestSnapshot(t *testing.T) {
	tempDir := t.TempDir()
	oldDir := filepath.Join(tempDir, "20260308090000_init")
	newDir := filepath.Join(tempDir, "20260308100000_add_user_name")

	for _, dir := range []string{oldDir, newDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
		if err := os.WriteFile(filepath.Join(dir, migrationSQLFileName), []byte("-- migration"), 0o644); err != nil {
			t.Fatalf("write migration.sql: %v", err)
		}
	}

	oldSnapshot := `
datasource db {
  provider = "postgresql"
  url      = "postgresql://localhost/test"
}

model User {
  id Int @id @default(autoincrement())
}
`

	newSnapshot := `
datasource db {
  provider = "postgresql"
  url      = "postgresql://localhost/test"
}

model User {
  id   Int    @id @default(autoincrement())
  name String?
}
`

	if err := os.WriteFile(filepath.Join(oldDir, migrationSchemaSnapshotFileName), []byte(oldSnapshot), 0o644); err != nil {
		t.Fatalf("write old snapshot: %v", err)
	}
	if err := os.WriteFile(filepath.Join(newDir, migrationSchemaSnapshotFileName), []byte(newSnapshot), 0o644); err != nil {
		t.Fatalf("write new snapshot: %v", err)
	}

	migrationDirs, err := discoverMigrations(tempDir)
	if err != nil {
		t.Fatalf("discover migrations: %v", err)
	}

	parsed, err := loadPreviousSchemaSnapshot(migrationDirs)
	if err != nil {
		t.Fatalf("load previous schema snapshot: %v", err)
	}

	user := parsed.GetModelByName("User")
	if user == nil || user.GetFieldByName("name") == nil {
		t.Fatalf("expected newest snapshot to be loaded, got %#v", parsed)
	}
}
