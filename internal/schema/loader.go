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

	// Parse each file individually to track source locations properly
	var allTables []database.Table
	for _, file := range sqlFiles {
		data, readErr := os.ReadFile(file)
		if readErr != nil {
			return nil, fmt.Errorf("failed to read SQL file %s: %w", file, readErr)
		}

		schema, parseErr := loadSQLSchemaFromBytesWithFilename(data, file)
		if parseErr != nil {
			return nil, fmt.Errorf("failed to parse SQL file %s: %w", file, parseErr)
		}

		allTables = append(allTables, schema.Tables...)
	}

	combinedSchema := &database.Schema{
		Tables:  allTables,
		Dialect: database.DialectPostgres,
	}

	// Validate that there are no duplicate table definitions
	if err := validateNoDuplicateTables(combinedSchema); err != nil {
		return nil, err
	}

	return combinedSchema, nil
}

// LoadSQLSchemaWithOptions loads a SQL schema with optional parsing options.
func loadSQLSchema(path string) (*database.Schema, error) {
	// Read the SQL file
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read SQL file: %w", err)
	}

	return loadSQLSchemaFromBytesWithFilename(data, path)
}

// loadSQLSchemaFromBytesWithFilename loads a SQL schema from a byte slice with a filename
func loadSQLSchemaFromBytesWithFilename(data []byte, filename string) (*database.Schema, error) {
	schema, err := ParseSQLSchemaWithDialectAndFilename(string(data), database.DialectPostgres, filename)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL DDL: %w", err)
	}

	// Validate that there are no duplicate table definitions
	if err := validateNoDuplicateTables(schema); err != nil {
		return nil, err
	}

	return schema, nil
}

// ValidateDuplicateTablesAsDiagnostics checks for duplicate tables and returns structured diagnostics.
// This is used by CheckSchema to provide detailed error locations to the VS Code extension.
func ValidateDuplicateTablesAsDiagnostics(schema *database.Schema) []Diagnostic {
	var diagnostics []Diagnostic

	// Track first occurrence of each table for detailed error messages
	type tableInfo struct {
		firstIndex int
		indices    []int
	}
	seen := make(map[string]*tableInfo)

	for i, table := range schema.Tables {
		// Default to "public" schema if not specified
		tableSchema := table.Schema
		if tableSchema == "" {
			tableSchema = "public"
		}

		// Create a composite key: schema.table_name
		key := fmt.Sprintf("%s.%s", tableSchema, table.Name)

		if info, exists := seen[key]; exists {
			// Add this duplicate to the list
			info.indices = append(info.indices, i)
		} else {
			// First occurrence
			seen[key] = &tableInfo{
				firstIndex: i,
				indices:    []int{i},
			}
		}
	}

	// Check for duplicates and create diagnostics
	for key, info := range seen {
		if len(info.indices) > 1 {
			// Create a diagnostic for each duplicate occurrence (except the first)
			for i, idx := range info.indices {
				table := schema.Tables[idx]

				var message string
				if i == 0 {
					// First occurrence
					message = fmt.Sprintf("Table %q is defined multiple times (first occurrence)", key)
				} else {
					// Subsequent occurrences
					message = fmt.Sprintf("Table %q is already defined (duplicate definition)", key)
				}

				diagnostic := Diagnostic{
					Message:  message,
					Severity: "error",
					File:     "",
					Line:     1,
					Column:   1,
				}

				// Add source location if available
				if table.SourceLocation != nil {
					if table.SourceLocation.File != "" {
						diagnostic.File = table.SourceLocation.File
					}
					diagnostic.Line = table.SourceLocation.Line
					diagnostic.Column = table.SourceLocation.Column
				}

				diagnostics = append(diagnostics, diagnostic)
			}
		}
	}

	return diagnostics
}

// validateNoDuplicateTables checks that each table is defined only once within its schema.
// Tables with the same name can exist in different schemas (e.g., public.users and auth.users),
// but the same table cannot be defined multiple times in the same schema.
// If no schema is specified, it defaults to "public".
// Returns an error for duplicate tables, or nil if no duplicates found.
func validateNoDuplicateTables(schema *database.Schema) error {
	// Track first occurrence of each table for detailed error messages
	type tableInfo struct {
		firstIndex int
		indices    []int
	}
	seen := make(map[string]*tableInfo)

	for i, table := range schema.Tables {
		// Default to "public" schema if not specified
		tableSchema := table.Schema
		if tableSchema == "" {
			tableSchema = "public"
		}

		// Create a composite key: schema.table_name
		key := fmt.Sprintf("%s.%s", tableSchema, table.Name)

		if info, exists := seen[key]; exists {
			// Add this duplicate to the list
			info.indices = append(info.indices, i)
		} else {
			// First occurrence
			seen[key] = &tableInfo{
				firstIndex: i,
				indices:    []int{i},
			}
		}
	}

	// Check for duplicates and create error messages with location info
	var errorMessages []string
	for key, info := range seen {
		if len(info.indices) > 1 {
			// Build error message with all locations
			var locations []string
			for _, idx := range info.indices {
				table := schema.Tables[idx]
				if table.SourceLocation != nil && table.SourceLocation.File != "" {
					locations = append(locations, fmt.Sprintf("%s:%d:%d",
						table.SourceLocation.File,
						table.SourceLocation.Line,
						table.SourceLocation.Column))
				} else if table.SourceLocation != nil {
					locations = append(locations, fmt.Sprintf("line %d",
						table.SourceLocation.Line))
				}
			}

			if len(locations) > 0 {
				errorMessages = append(errorMessages, fmt.Sprintf(
					"table %q is defined multiple times at: %s",
					key, strings.Join(locations, ", ")))
			} else {
				errorMessages = append(errorMessages, fmt.Sprintf(
					"table %q is defined multiple times", key))
			}
		}
	}

	if len(errorMessages) > 0 {
		return fmt.Errorf("%s", strings.Join(errorMessages, "; "))
	}

	return nil
}
