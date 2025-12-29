package schema

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lockplane/lockplane/internal/database"
)

func TestLoadSchemaSingleFile(t *testing.T) {
	tempDir := t.TempDir()
	sqlFile := filepath.Join(tempDir, "users.lp.sql")

	sqlContent := `CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL);`
	if err := os.WriteFile(sqlFile, []byte(sqlContent), 0600); err != nil {
		t.Fatalf("Failed to write SQL file: %v", err)
	}

	schema, err := LoadSchema(sqlFile)
	if err != nil {
		t.Fatalf("LoadSchema failed: %v", err)
	}

	if len(schema.Tables) != 1 {
		t.Fatalf("Expected 1 table, got %d", len(schema.Tables))
	}

	if schema.Tables[0].Name != "users" {
		t.Errorf("Expected table name 'users', got %q", schema.Tables[0].Name)
	}
}

func TestLoadSchemaFromDirectory(t *testing.T) {
	tempDir := t.TempDir()

	// Create multiple SQL files
	files := map[string]string{
		"users.lp.sql": `CREATE TABLE users (id INTEGER PRIMARY KEY);`,
		"posts.lp.sql": `CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER);`,
	}

	for filename, content := range files {
		path := filepath.Join(tempDir, filename)
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatalf("Failed to write %s: %v", filename, err)
		}
	}

	schema, err := LoadSchema(tempDir)
	if err != nil {
		t.Fatalf("LoadSchema failed: %v", err)
	}

	if len(schema.Tables) != 2 {
		t.Fatalf("Expected 2 tables, got %d", len(schema.Tables))
	}

	// Tables should be loaded in alphabetical order by filename
	// posts.lp.sql comes before users.lp.sql
	if schema.Tables[0].Name != "posts" {
		t.Errorf("Expected first table 'posts', got %q", schema.Tables[0].Name)
	}
	if schema.Tables[1].Name != "users" {
		t.Errorf("Expected second table 'users', got %q", schema.Tables[1].Name)
	}
}

func TestLoadSchemaAlphabeticalOrder(t *testing.T) {
	tempDir := t.TempDir()

	// Create files in non-alphabetical order to test sorting
	files := map[string]string{
		"c_table.lp.sql": `CREATE TABLE c_table (id INTEGER);`,
		"a_table.lp.sql": `CREATE TABLE a_table (id INTEGER);`,
		"b_table.lp.sql": `CREATE TABLE b_table (id INTEGER);`,
	}

	for filename, content := range files {
		path := filepath.Join(tempDir, filename)
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatalf("Failed to write %s: %v", filename, err)
		}
	}

	schema, err := LoadSchema(tempDir)
	if err != nil {
		t.Fatalf("LoadSchema failed: %v", err)
	}

	if len(schema.Tables) != 3 {
		t.Fatalf("Expected 3 tables, got %d", len(schema.Tables))
	}

	// Should be in alphabetical order
	expectedOrder := []string{"a_table", "b_table", "c_table"}
	for i, expected := range expectedOrder {
		if schema.Tables[i].Name != expected {
			t.Errorf("Table %d: expected %q, got %q", i, expected, schema.Tables[i].Name)
		}
	}
}

func TestLoadSchemaIgnoresNonSQLFiles(t *testing.T) {
	tempDir := t.TempDir()

	// Create SQL and non-SQL files
	files := map[string]string{
		"users.lp.sql": `CREATE TABLE users (id INTEGER);`,
		"readme.txt":   "This is a readme",
		"schema.sql":   `CREATE TABLE ignored (id INTEGER);`, // Wrong extension
		"test.md":      "# Documentation",
	}

	for filename, content := range files {
		path := filepath.Join(tempDir, filename)
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatalf("Failed to write %s: %v", filename, err)
		}
	}

	schema, err := LoadSchema(tempDir)
	if err != nil {
		t.Fatalf("LoadSchema failed: %v", err)
	}

	// Should only load the .lp.sql file
	if len(schema.Tables) != 1 {
		t.Fatalf("Expected 1 table, got %d", len(schema.Tables))
	}

	if schema.Tables[0].Name != "users" {
		t.Errorf("Expected table 'users', got %q", schema.Tables[0].Name)
	}
}

func TestLoadSchemaIgnoresSubdirectories(t *testing.T) {
	tempDir := t.TempDir()

	// Create a SQL file in root
	rootFile := filepath.Join(tempDir, "users.lp.sql")
	if err := os.WriteFile(rootFile, []byte(`CREATE TABLE users (id INTEGER);`), 0600); err != nil {
		t.Fatalf("Failed to write root SQL file: %v", err)
	}

	// Create a subdirectory with SQL file (should be ignored)
	subdir := filepath.Join(tempDir, "migrations")
	if err := os.MkdirAll(subdir, 0755); err != nil {
		t.Fatalf("Failed to create subdirectory: %v", err)
	}
	subFile := filepath.Join(subdir, "posts.lp.sql")
	if err := os.WriteFile(subFile, []byte(`CREATE TABLE posts (id INTEGER);`), 0600); err != nil {
		t.Fatalf("Failed to write subdirectory SQL file: %v", err)
	}

	schema, err := LoadSchema(tempDir)
	if err != nil {
		t.Fatalf("LoadSchema failed: %v", err)
	}

	// Should only load the root file, not the subdirectory
	if len(schema.Tables) != 1 {
		t.Fatalf("Expected 1 table, got %d", len(schema.Tables))
	}

	if schema.Tables[0].Name != "users" {
		t.Errorf("Expected table 'users', got %q", schema.Tables[0].Name)
	}
}

func TestLoadSchemaEmptyDirectory(t *testing.T) {
	tempDir := t.TempDir()

	_, err := LoadSchema(tempDir)
	if err == nil {
		t.Fatal("Expected error for empty directory, got nil")
	}

	if err.Error() != "no .lp.sql files found in directory "+tempDir {
		t.Errorf("Expected 'no .lp.sql files found' error, got %q", err.Error())
	}
}

func TestLoadSchemaNonExistentPath(t *testing.T) {
	_, err := LoadSchema("/nonexistent/path/file.lp.sql")
	if err == nil {
		t.Fatal("Expected error for non-existent path, got nil")
	}
}

func TestLoadSchemaInvalidSQL(t *testing.T) {
	tempDir := t.TempDir()
	sqlFile := filepath.Join(tempDir, "invalid.lp.sql")

	invalidSQL := `CREATE TABLE users id INTEGER);` // Missing opening paren
	if err := os.WriteFile(sqlFile, []byte(invalidSQL), 0600); err != nil {
		t.Fatalf("Failed to write SQL file: %v", err)
	}

	_, err := LoadSchema(sqlFile)
	if err == nil {
		t.Fatal("Expected error for invalid SQL, got nil")
	}
}

func TestLoadSchemaWithoutExtension(t *testing.T) {
	tempDir := t.TempDir()
	sqlFile := filepath.Join(tempDir, "schema.sql") // Wrong extension

	sqlContent := `CREATE TABLE users (id INTEGER);`
	if err := os.WriteFile(sqlFile, []byte(sqlContent), 0600); err != nil {
		t.Fatalf("Failed to write SQL file: %v", err)
	}

	_, err := LoadSchema(sqlFile)
	if err == nil {
		t.Fatal("Expected error for file without .lp.sql extension, got nil")
	}
}

func TestLoadSchemaCaseInsensitiveExtension(t *testing.T) {
	tempDir := t.TempDir()

	// Test different case variations of the extension
	files := []string{
		"users.lp.sql",
		"posts.LP.SQL",
		"comments.Lp.Sql",
	}

	for i, filename := range files {
		path := filepath.Join(tempDir, filename)
		var content string
		switch i {
		case 0:
			content = `CREATE TABLE users (id INTEGER);`
		case 1:
			content = `CREATE TABLE posts (id INTEGER);`
		case 2:
			content = `CREATE TABLE comments (id INTEGER);`
		}
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatalf("Failed to write %s: %v", filename, err)
		}
	}

	schema, err := LoadSchema(tempDir)
	if err != nil {
		t.Fatalf("LoadSchema failed: %v", err)
	}

	// All files should be loaded regardless of case
	if len(schema.Tables) != 3 {
		t.Fatalf("Expected 3 tables, got %d", len(schema.Tables))
	}
}

func TestLoadSchemaPreservesDialect(t *testing.T) {
	tempDir := t.TempDir()
	sqlFile := filepath.Join(tempDir, "users.lp.sql")

	sqlContent := `CREATE TABLE users (id INTEGER);`
	if err := os.WriteFile(sqlFile, []byte(sqlContent), 0600); err != nil {
		t.Fatalf("Failed to write SQL file: %v", err)
	}

	schema, err := LoadSchema(sqlFile)
	if err != nil {
		t.Fatalf("LoadSchema failed: %v", err)
	}

	if schema.Dialect != database.DialectPostgres {
		t.Errorf("Expected dialect %q, got %q", database.DialectPostgres, schema.Dialect)
	}
}

func TestLoadSchemaMultipleStatements(t *testing.T) {
	tempDir := t.TempDir()
	sqlFile := filepath.Join(tempDir, "schema.lp.sql")

	sqlContent := `
		CREATE TABLE users (id INTEGER PRIMARY KEY);
		CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER);
	`
	if err := os.WriteFile(sqlFile, []byte(sqlContent), 0600); err != nil {
		t.Fatalf("Failed to write SQL file: %v", err)
	}

	schema, err := LoadSchema(sqlFile)
	if err != nil {
		t.Fatalf("LoadSchema failed: %v", err)
	}

	if len(schema.Tables) != 2 {
		t.Fatalf("Expected 2 tables, got %d", len(schema.Tables))
	}
}

func TestLoadSchemaWithTrailingNewline(t *testing.T) {
	tempDir := t.TempDir()
	sqlFile := filepath.Join(tempDir, "users.lp.sql")

	// File with trailing newline
	sqlContent := "CREATE TABLE users (id INTEGER);\n"
	if err := os.WriteFile(sqlFile, []byte(sqlContent), 0600); err != nil {
		t.Fatalf("Failed to write SQL file: %v", err)
	}

	schema, err := LoadSchema(sqlFile)
	if err != nil {
		t.Fatalf("LoadSchema failed: %v", err)
	}

	if len(schema.Tables) != 1 {
		t.Fatalf("Expected 1 table, got %d", len(schema.Tables))
	}
}

func TestLoadSchemaWithoutTrailingNewline(t *testing.T) {
	tempDir := t.TempDir()
	sqlFile := filepath.Join(tempDir, "users.lp.sql")

	// File without trailing newline
	sqlContent := "CREATE TABLE users (id INTEGER);"
	if err := os.WriteFile(sqlFile, []byte(sqlContent), 0600); err != nil {
		t.Fatalf("Failed to write SQL file: %v", err)
	}

	schema, err := LoadSchema(sqlFile)
	if err != nil {
		t.Fatalf("LoadSchema failed: %v", err)
	}

	if len(schema.Tables) != 1 {
		t.Fatalf("Expected 1 table, got %d", len(schema.Tables))
	}
}

func TestLoadSchemaEmptyFile(t *testing.T) {
	tempDir := t.TempDir()
	sqlFile := filepath.Join(tempDir, "empty.lp.sql")

	// Create empty file
	if err := os.WriteFile(sqlFile, []byte(""), 0600); err != nil {
		t.Fatalf("Failed to write SQL file: %v", err)
	}

	schema, err := LoadSchema(sqlFile)
	if err != nil {
		t.Fatalf("LoadSchema failed: %v", err)
	}

	// Empty file should result in zero tables
	if len(schema.Tables) != 0 {
		t.Errorf("Expected 0 tables for empty file, got %d", len(schema.Tables))
	}
}

func TestLoadSchemaMultipleFilesInOrder(t *testing.T) {
	tempDir := t.TempDir()

	// First file creates tables
	file1 := filepath.Join(tempDir, "01_tables.lp.sql")
	if err := os.WriteFile(file1, []byte(`
		CREATE TABLE users (id INTEGER);
		CREATE TABLE posts (id INTEGER);
	`), 0600); err != nil {
		t.Fatalf("Failed to write file1: %v", err)
	}

	// Second file creates more tables
	file2 := filepath.Join(tempDir, "02_more_tables.lp.sql")
	if err := os.WriteFile(file2, []byte(`
		CREATE TABLE comments (id INTEGER);
	`), 0600); err != nil {
		t.Fatalf("Failed to write file2: %v", err)
	}

	schema, err := LoadSchema(tempDir)
	if err != nil {
		t.Fatalf("LoadSchema failed: %v", err)
	}

	if len(schema.Tables) != 3 {
		t.Fatalf("Expected 3 tables, got %d", len(schema.Tables))
	}

	// Check table names are loaded correctly
	users := schema.Tables[0]
	if users.Name != "users" {
		t.Errorf("Expected first table to be 'users', got %q", users.Name)
	}

	posts := schema.Tables[1]
	if posts.Name != "posts" {
		t.Errorf("Expected second table to be 'posts', got %q", posts.Name)
	}

	comments := schema.Tables[2]
	if comments.Name != "comments" {
		t.Errorf("Expected third table to be 'comments', got %q", comments.Name)
	}
}

func TestLoadSchemaDuplicateTableInSameFile(t *testing.T) {
	tempDir := t.TempDir()
	sqlFile := filepath.Join(tempDir, "duplicate.lp.sql")

	// Define the same table twice in one file
	sqlContent := `
		CREATE TABLE users (id INTEGER);
		CREATE TABLE users (id BIGINT, email TEXT);
	`
	if err := os.WriteFile(sqlFile, []byte(sqlContent), 0600); err != nil {
		t.Fatalf("Failed to write SQL file: %v", err)
	}

	_, err := LoadSchema(sqlFile)
	if err == nil {
		t.Fatal("Expected error for duplicate table definition, got nil")
	}

	expectedErr := `table "public.users" is defined multiple times`
	if !strings.Contains(err.Error(), expectedErr) {
		t.Errorf("Expected error to contain %q, got %q", expectedErr, err.Error())
	}
}

func TestLoadSchemaDuplicateTableAcrossFiles(t *testing.T) {
	tempDir := t.TempDir()

	// First file
	file1 := filepath.Join(tempDir, "users1.lp.sql")
	if err := os.WriteFile(file1, []byte(`CREATE TABLE users (id INTEGER);`), 0600); err != nil {
		t.Fatalf("Failed to write file1: %v", err)
	}

	// Second file with duplicate table
	file2 := filepath.Join(tempDir, "users2.lp.sql")
	if err := os.WriteFile(file2, []byte(`CREATE TABLE users (id BIGINT);`), 0600); err != nil {
		t.Fatalf("Failed to write file2: %v", err)
	}

	_, err := LoadSchema(tempDir)
	if err == nil {
		t.Fatal("Expected error for duplicate table across files, got nil")
	}

	expectedErr := `table "public.users" is defined multiple times`
	if !strings.Contains(err.Error(), expectedErr) {
		t.Errorf("Expected error to contain %q, got %q", expectedErr, err.Error())
	}
}

func TestLoadSchemaMultipleDuplicateTables(t *testing.T) {
	tempDir := t.TempDir()

	// First file
	file1 := filepath.Join(tempDir, "01_tables.lp.sql")
	if err := os.WriteFile(file1, []byte(`
		CREATE TABLE users (id INTEGER);
		CREATE TABLE posts (id INTEGER);
	`), 0600); err != nil {
		t.Fatalf("Failed to write file1: %v", err)
	}

	// Second file with multiple duplicates
	file2 := filepath.Join(tempDir, "02_duplicates.lp.sql")
	if err := os.WriteFile(file2, []byte(`
		CREATE TABLE users (id BIGINT);
		CREATE TABLE posts (id BIGINT);
	`), 0600); err != nil {
		t.Fatalf("Failed to write file2: %v", err)
	}

	_, err := LoadSchema(tempDir)
	if err == nil {
		t.Fatal("Expected error for multiple duplicate tables, got nil")
	}

	// Should mention multiple tables with schema prefix
	if !strings.Contains(err.Error(), "public.users") {
		t.Errorf("Expected error to mention 'public.users', got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "public.posts") {
		t.Errorf("Expected error to mention 'public.posts', got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "defined multiple times") {
		t.Errorf("Expected error to say 'defined multiple times', got %q", err.Error())
	}
}

func TestLoadSchemaDuplicateWithOtherTables(t *testing.T) {
	tempDir := t.TempDir()

	file1 := filepath.Join(tempDir, "01_schema.lp.sql")
	if err := os.WriteFile(file1, []byte(`
		CREATE TABLE users (id INTEGER);
		CREATE TABLE posts (id INTEGER);
		CREATE TABLE comments (id INTEGER);
	`), 0600); err != nil {
		t.Fatalf("Failed to write file1: %v", err)
	}

	file2 := filepath.Join(tempDir, "02_duplicate.lp.sql")
	if err := os.WriteFile(file2, []byte(`
		CREATE TABLE tags (id INTEGER);
		CREATE TABLE posts (id BIGINT);
	`), 0600); err != nil {
		t.Fatalf("Failed to write file2: %v", err)
	}

	_, err := LoadSchema(tempDir)
	if err == nil {
		t.Fatal("Expected error for duplicate 'posts' table, got nil")
	}

	// Should specifically mention the duplicate table with schema
	expectedErr := `table "public.posts" is defined multiple times`
	if !strings.Contains(err.Error(), expectedErr) {
		t.Errorf("Expected error to contain %q, got %q", expectedErr, err.Error())
	}
}

func TestLoadSchemaSameTableNameDifferentSchemas(t *testing.T) {
	tempDir := t.TempDir()

	// Same table name "users" in different schemas should be allowed
	sqlFile := filepath.Join(tempDir, "multi_schema.lp.sql")
	sqlContent := `
		CREATE TABLE public.users (id INTEGER);
		CREATE TABLE auth.users (id BIGINT, email TEXT);
	`
	if err := os.WriteFile(sqlFile, []byte(sqlContent), 0600); err != nil {
		t.Fatalf("Failed to write SQL file: %v", err)
	}

	schema, err := LoadSchema(sqlFile)
	if err != nil {
		t.Fatalf("Expected no error for same table name in different schemas, got: %v", err)
	}

	if len(schema.Tables) != 2 {
		t.Fatalf("Expected 2 tables, got %d", len(schema.Tables))
	}

	// Verify both tables are present with correct schemas
	foundPublic := false
	foundAuth := false
	for _, table := range schema.Tables {
		if table.Name == "users" && table.Schema == "public" {
			foundPublic = true
		}
		if table.Name == "users" && table.Schema == "auth" {
			foundAuth = true
		}
	}

	if !foundPublic {
		t.Error("Expected to find public.users")
	}
	if !foundAuth {
		t.Error("Expected to find auth.users")
	}
}

func TestLoadSchemaDuplicateImplicitAndExplicitPublic(t *testing.T) {
	tempDir := t.TempDir()

	// Table without schema (defaults to public) and explicit public.table should be duplicate
	sqlFile := filepath.Join(tempDir, "implicit_explicit.lp.sql")
	sqlContent := `
		CREATE TABLE users (id INTEGER);
		CREATE TABLE public.users (id BIGINT);
	`
	if err := os.WriteFile(sqlFile, []byte(sqlContent), 0600); err != nil {
		t.Fatalf("Failed to write SQL file: %v", err)
	}

	_, err := LoadSchema(sqlFile)
	if err == nil {
		t.Fatal("Expected error for duplicate implicit/explicit public.users, got nil")
	}

	expectedErr := `table "public.users" is defined multiple times`
	if !strings.Contains(err.Error(), expectedErr) {
		t.Errorf("Expected error to contain %q, got %q", expectedErr, err.Error())
	}
}

func TestLoadSchemaDifferentSchemasAcrossFiles(t *testing.T) {
	tempDir := t.TempDir()

	// First file with public.users
	file1 := filepath.Join(tempDir, "public_users.lp.sql")
	if err := os.WriteFile(file1, []byte(`CREATE TABLE public.users (id INTEGER);`), 0600); err != nil {
		t.Fatalf("Failed to write file1: %v", err)
	}

	// Second file with auth.users (different schema, same name - should be OK)
	file2 := filepath.Join(tempDir, "auth_users.lp.sql")
	if err := os.WriteFile(file2, []byte(`CREATE TABLE auth.users (id BIGINT);`), 0600); err != nil {
		t.Fatalf("Failed to write file2: %v", err)
	}

	schema, err := LoadSchema(tempDir)
	if err != nil {
		t.Fatalf("Expected no error for same table name in different schemas, got: %v", err)
	}

	if len(schema.Tables) != 2 {
		t.Fatalf("Expected 2 tables, got %d", len(schema.Tables))
	}
}

func TestLoadSchemaCaseSensitivity(t *testing.T) {
	tempDir := t.TempDir()

	tests := []struct {
		name        string
		sql         string
		expectError bool
		description string
	}{
		{
			name:        "unquoted_mixed_case_not_duplicate",
			sql:         `CREATE TABLE Users (id INTEGER); CREATE TABLE users (id BIGINT);`,
			expectError: true,
			description: "Unquoted identifiers are normalized to lowercase, so Users and users are duplicates",
		},
		{
			name:        "quoted_mixed_case_not_duplicate",
			sql:         `CREATE TABLE "Users" (id INTEGER); CREATE TABLE users (id BIGINT);`,
			expectError: false,
			description: "Quoted 'Users' preserves case, unquoted 'users' is lowercase - different tables",
		},
		{
			name:        "quoted_exact_duplicate",
			sql:         `CREATE TABLE "Users" (id INTEGER); CREATE TABLE "Users" (id BIGINT);`,
			expectError: true,
			description: "Quoted identifiers with same case are duplicates",
		},
		{
			name:        "schema_case_sensitivity",
			sql:         `CREATE TABLE Public.users (id INTEGER); CREATE TABLE public.users (id BIGINT);`,
			expectError: true,
			description: "Unquoted 'Public' normalized to 'public' - both are same schema",
		},
		{
			name:        "schema_quoted_case",
			sql:         `CREATE TABLE "Public".users (id INTEGER); CREATE TABLE public.users (id BIGINT);`,
			expectError: false,
			description: "Quoted 'Public' and unquoted 'public' are different schemas",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sqlFile := filepath.Join(tempDir, tt.name+".lp.sql")
			if err := os.WriteFile(sqlFile, []byte(tt.sql), 0600); err != nil {
				t.Fatalf("Failed to write SQL file: %v", err)
			}

			_, err := LoadSchema(sqlFile)

			if tt.expectError && err == nil {
				t.Errorf("%s: Expected error but got none", tt.description)
			}
			if !tt.expectError && err != nil {
				t.Errorf("%s: Expected no error but got: %v", tt.description, err)
			}
		})
	}
}
