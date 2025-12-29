package schema

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/lockplane/lockplane/internal/database"
)

// load a schema from SQL DDL (.lp.sql) files. Accepts a file (must be .lp.sql)
// or a directory to perform a shallow search for .lp.sql files.
func LoadSchema(path string) (*database.Schema, error) {
	if info, err := os.Stat(path); err == nil && info.IsDir() {
		return loadSchemaFromDir(path)
	}

	// Check for .lp.sql extension
	if _, err := os.Stat(path); err == nil && strings.HasSuffix(strings.ToLower(path), ".lp.sql") {
		return loadSQLSchema(path)
	}

	return nil, fmt.Errorf("did not find .lp.sql file(s)")
}

func loadSchemaFromDir(dir string) (*database.Schema, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema directory %s: %w", dir, err)
	}

	var sqlFiles []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if entry.Type()&os.ModeSymlink != 0 {
			continue
		}

		name := entry.Name()
		lowerName := strings.ToLower(name)

		// Only include .lp.sql files
		if strings.HasSuffix(lowerName, ".lp.sql") {
			sqlFiles = append(sqlFiles, filepath.Join(dir, name))
		}
	}

	if len(sqlFiles) == 0 {
		return nil, fmt.Errorf("no .lp.sql files found in directory %s", dir)
	}

	sort.Strings(sqlFiles)

	var builder strings.Builder
	for _, file := range sqlFiles {
		data, readErr := os.ReadFile(file)
		if readErr != nil {
			return nil, fmt.Errorf("failed to read SQL file %s: %w", file, readErr)
		}

		builder.WriteString(fmt.Sprintf("-- File: %s\n", file))
		builder.Write(data)
		if len(data) == 0 || data[len(data)-1] != '\n' {
			builder.WriteByte('\n')
		}
		builder.WriteByte('\n')
	}

	return loadSQLSchemaFromBytes([]byte(builder.String()))
}

// LoadSQLSchemaWithOptions loads a SQL schema with optional parsing options.
func loadSQLSchema(path string) (*database.Schema, error) {
	// Read the SQL file
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read SQL file: %w", err)
	}

	return loadSQLSchemaFromBytes(data)
}

// LoadSQLSchemaFromBytes loads a SQL schema from a byte slice
func loadSQLSchemaFromBytes(data []byte) (*database.Schema, error) {
	schema, err := ParseSQLSchemaWithDialect(string(data), database.DialectPostgres)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL DDL: %w", err)
	}

	// Validate that there are no duplicate table definitions
	if err := validateNoDuplicateTables(schema); err != nil {
		return nil, err
	}

	return schema, nil
}

// validateNoDuplicateTables checks that each table is defined only once within its schema.
// Tables with the same name can exist in different schemas (e.g., public.users and auth.users),
// but the same table cannot be defined multiple times in the same schema.
// If no schema is specified, it defaults to "public".
func validateNoDuplicateTables(schema *database.Schema) error {
	// Use (schema, name) as the key to identify unique tables
	seen := make(map[string]bool)
	var duplicates []string

	for _, table := range schema.Tables {
		// Default to "public" schema if not specified
		tableSchema := table.Schema
		if tableSchema == "" {
			tableSchema = "public"
		}

		// Create a composite key: schema.table_name
		key := fmt.Sprintf("%s.%s", tableSchema, table.Name)

		if seen[key] {
			duplicates = append(duplicates, key)
		}
		seen[key] = true
	}

	if len(duplicates) > 0 {
		if len(duplicates) == 1 {
			return fmt.Errorf("table %q is defined multiple times", duplicates[0])
		}
		return fmt.Errorf("tables are defined multiple times: %v", duplicates)
	}

	return nil
}
