package schema

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/lockplane/lockplane/internal/database"
)

func TestCheckSchema_ValidSchema(t *testing.T) {
	// Create a temporary file with valid schema
	tmpDir := t.TempDir()
	schemaPath := filepath.Join(tmpDir, "schema.lp.sql")

	sql := `CREATE TABLE users (
		id BIGINT PRIMARY KEY,
		email TEXT NOT NULL
	);`

	if err := os.WriteFile(schemaPath, []byte(sql), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	// Run CheckSchema
	jsonOutput, err := CheckSchema(schemaPath)
	if err != nil {
		t.Fatalf("CheckSchema failed: %v", err)
	}

	// Parse JSON output
	var output CheckOutput
	if err := json.Unmarshal([]byte(jsonOutput), &output); err != nil {
		t.Fatalf("Failed to parse JSON output: %v", err)
	}

	// Verify output
	if !output.Summary.Valid {
		t.Errorf("Expected valid schema, got valid=%v", output.Summary.Valid)
	}

	if output.Summary.Errors != 0 {
		t.Errorf("Expected 0 errors, got %d", output.Summary.Errors)
	}

	if len(output.Diagnostics) != 0 {
		t.Errorf("Expected 0 diagnostics, got %d", len(output.Diagnostics))
	}
}

func TestCheckSchema_DuplicateTable(t *testing.T) {
	// Create a temporary file with duplicate tables
	tmpDir := t.TempDir()
	schemaPath := filepath.Join(tmpDir, "schema.lp.sql")

	sql := `CREATE TABLE users (
		id BIGINT PRIMARY KEY,
		email TEXT NOT NULL
	);

	CREATE TABLE users (
		id BIGINT PRIMARY KEY,
		name TEXT
	);`

	if err := os.WriteFile(schemaPath, []byte(sql), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	// Run CheckSchema
	jsonOutput, err := CheckSchema(schemaPath)
	if err != nil {
		t.Fatalf("CheckSchema failed: %v", err)
	}

	// Parse JSON output
	var output CheckOutput
	if err := json.Unmarshal([]byte(jsonOutput), &output); err != nil {
		t.Fatalf("Failed to parse JSON output: %v", err)
	}

	// Verify output
	if output.Summary.Valid {
		t.Errorf("Expected invalid schema, got valid=%v", output.Summary.Valid)
	}

	if output.Summary.Errors != 2 {
		t.Errorf("Expected 2 errors, got %d", output.Summary.Errors)
	}

	if len(output.Diagnostics) != 2 {
		t.Errorf("Expected 2 diagnostics, got %d", len(output.Diagnostics))
	}

	// Verify diagnostics have location info
	for i, diag := range output.Diagnostics {
		if diag.File == "" {
			t.Errorf("Diagnostic %d missing file", i)
		}
		if diag.Line == 0 {
			t.Errorf("Diagnostic %d missing line number", i)
		}
		if diag.Severity != "error" {
			t.Errorf("Diagnostic %d has wrong severity: %s", i, diag.Severity)
		}
	}
}

func TestCheckSchema_DuplicateAcrossFiles(t *testing.T) {
	// Create a temporary directory with multiple files
	tmpDir := t.TempDir()

	file1 := filepath.Join(tmpDir, "users.lp.sql")
	file2 := filepath.Join(tmpDir, "duplicate.lp.sql")

	sql1 := `CREATE TABLE users (
		id BIGINT PRIMARY KEY,
		email TEXT NOT NULL
	);`

	sql2 := `CREATE TABLE users (
		id BIGINT PRIMARY KEY,
		name TEXT
	);`

	if err := os.WriteFile(file1, []byte(sql1), 0644); err != nil {
		t.Fatalf("Failed to write file1: %v", err)
	}

	if err := os.WriteFile(file2, []byte(sql2), 0644); err != nil {
		t.Fatalf("Failed to write file2: %v", err)
	}

	// Run CheckSchema on directory
	jsonOutput, err := CheckSchema(tmpDir)
	if err != nil {
		t.Fatalf("CheckSchema failed: %v", err)
	}

	// Parse JSON output
	var output CheckOutput
	if err := json.Unmarshal([]byte(jsonOutput), &output); err != nil {
		t.Fatalf("Failed to parse JSON output: %v", err)
	}

	// Verify output
	if output.Summary.Valid {
		t.Errorf("Expected invalid schema, got valid=%v", output.Summary.Valid)
	}

	if output.Summary.Errors != 2 {
		t.Errorf("Expected 2 errors, got %d", output.Summary.Errors)
	}

	if len(output.Diagnostics) != 2 {
		t.Errorf("Expected 2 diagnostics, got %d", len(output.Diagnostics))
	}

	// Verify diagnostics reference different files
	files := make(map[string]bool)
	for _, diag := range output.Diagnostics {
		files[diag.File] = true
	}

	if len(files) != 2 {
		t.Errorf("Expected diagnostics from 2 different files, got %d", len(files))
	}
}

func TestValidateDuplicateTablesAsDiagnostics_NoDuplicates(t *testing.T) {
	schema := &database.Schema{
		Tables: []database.Table{
			{
				Name:   "users",
				Schema: "public",
				SourceLocation: &database.SourceLocation{
					File:   "users.lp.sql",
					Line:   1,
					Column: 1,
				},
			},
			{
				Name:   "posts",
				Schema: "public",
				SourceLocation: &database.SourceLocation{
					File:   "posts.lp.sql",
					Line:   1,
					Column: 1,
				},
			},
		},
	}

	diagnostics := ValidateDuplicateTablesAsDiagnostics(schema)

	if len(diagnostics) != 0 {
		t.Errorf("Expected no diagnostics for unique tables, got %d", len(diagnostics))
	}
}

func TestValidateDuplicateTablesAsDiagnostics_WithDuplicates(t *testing.T) {
	schema := &database.Schema{
		Tables: []database.Table{
			{
				Name:   "users",
				Schema: "public",
				SourceLocation: &database.SourceLocation{
					File:   "file1.lp.sql",
					Line:   5,
					Column: 1,
				},
			},
			{
				Name:   "users",
				Schema: "public",
				SourceLocation: &database.SourceLocation{
					File:   "file2.lp.sql",
					Line:   10,
					Column: 3,
				},
			},
		},
	}

	diagnostics := ValidateDuplicateTablesAsDiagnostics(schema)

	if len(diagnostics) != 2 {
		t.Fatalf("Expected 2 diagnostics, got %d", len(diagnostics))
	}

	// Check that both diagnostics are errors
	for i, diag := range diagnostics {
		if diag.Severity != "error" {
			t.Errorf("Diagnostic %d: expected severity 'error', got '%s'", i, diag.Severity)
		}

		if diag.Line == 0 {
			t.Errorf("Diagnostic %d: missing line number", i)
		}

		if diag.File == "" {
			t.Errorf("Diagnostic %d: missing file", i)
		}
	}

	// Verify we have one "first occurrence" and one "duplicate"
	hasFirst := false
	hasDuplicate := false

	for _, diag := range diagnostics {
		if contains(diag.Message, "first occurrence") {
			hasFirst = true
		}
		if contains(diag.Message, "duplicate definition") {
			hasDuplicate = true
		}
	}

	if !hasFirst {
		t.Error("Missing diagnostic for first occurrence")
	}

	if !hasDuplicate {
		t.Error("Missing diagnostic for duplicate definition")
	}
}

func TestValidateDuplicateTablesAsDiagnostics_DifferentSchemas(t *testing.T) {
	schema := &database.Schema{
		Tables: []database.Table{
			{
				Name:   "users",
				Schema: "public",
				SourceLocation: &database.SourceLocation{
					File:   "public_users.lp.sql",
					Line:   1,
					Column: 1,
				},
			},
			{
				Name:   "users",
				Schema: "auth",
				SourceLocation: &database.SourceLocation{
					File:   "auth_users.lp.sql",
					Line:   1,
					Column: 1,
				},
			},
		},
	}

	diagnostics := ValidateDuplicateTablesAsDiagnostics(schema)

	// Should be no diagnostics - same table name in different schemas is allowed
	if len(diagnostics) != 0 {
		t.Errorf("Expected no diagnostics for tables in different schemas, got %d", len(diagnostics))
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
