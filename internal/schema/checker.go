package schema

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/lockplane/lockplane/internal/database"
)

// CheckSchema validates schema files and returns structured diagnostics as JSON.
// It attempts to parse the schema and returns any errors as structured diagnostics.
func CheckSchema(path string) (reportJson string, err error) {
	output := NewCheckOutput()

	// Step 1: Parse the SQL schema (without duplicate validation)
	// We need to parse first to get source locations
	schema, err := loadSchemaWithoutValidation(path)
	if err != nil {
		// Parse the error to extract file/line/column information if available
		diagnostic := parseErrorToDiagnostic(err, path)
		output.Diagnostics = append(output.Diagnostics, diagnostic)
		output.Summary.Errors++
		output.Summary.Valid = false

		// Marshal to JSON and return early since we can't continue
		jsonBytes, marshalErr := json.MarshalIndent(output, "", "  ")
		if marshalErr != nil {
			return "", fmt.Errorf("failed to marshal check output: %w", marshalErr)
		}
		return string(jsonBytes), nil
	}

	// Step 2: Validate for duplicate tables with structured diagnostics
	duplicateDiagnostics := ValidateDuplicateTablesAsDiagnostics(schema)
	for _, diag := range duplicateDiagnostics {
		output.Diagnostics = append(output.Diagnostics, diag)
		output.Summary.Errors++
		output.Summary.Valid = false
	}

	// Step 3: Enrich the parser output (future: add linting rules, etc.)

	// Step 4: With DB, run a diff and validate the results
	// if DB is not available, include a warning
	// TODO: surface the warning in vscode

	// Marshal to JSON
	jsonBytes, marshalErr := json.MarshalIndent(output, "", "  ")
	if marshalErr != nil {
		return "", fmt.Errorf("failed to marshal check output: %w", marshalErr)
	}

	return string(jsonBytes), nil
}

// loadSchemaWithoutValidation loads a schema without validating for duplicates.
// This is needed by CheckSchema to get source locations before validation.
func loadSchemaWithoutValidation(path string) (*database.Schema, error) {
	if info, err := os.Stat(path); err == nil && info.IsDir() {
		return loadSchemaFromDirWithoutValidation(path)
	}

	// Check for .lp.sql extension
	if _, err := os.Stat(path); err == nil && strings.HasSuffix(strings.ToLower(path), ".lp.sql") {
		return loadSQLSchemaWithoutValidation(path)
	}

	return nil, fmt.Errorf("did not find .lp.sql file(s)")
}

// loadSchemaFromDirWithoutValidation loads schemas from a directory without duplicate validation
func loadSchemaFromDirWithoutValidation(dir string) (*database.Schema, error) {
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

		schema, parseErr := ParseSQLSchemaWithDialectAndFilename(string(data), database.DialectPostgres, file)
		if parseErr != nil {
			return nil, fmt.Errorf("failed to parse SQL file %s: %w", file, parseErr)
		}

		allTables = append(allTables, schema.Tables...)
	}

	return &database.Schema{
		Tables:  allTables,
		Dialect: database.DialectPostgres,
	}, nil
}

// loadSQLSchemaWithoutValidation loads a single file without duplicate validation
func loadSQLSchemaWithoutValidation(path string) (*database.Schema, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read SQL file: %w", err)
	}

	schema, err := ParseSQLSchemaWithDialectAndFilename(string(data), database.DialectPostgres, path)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL DDL: %w", err)
	}

	return schema, nil
}

// parseErrorToDiagnostic attempts to extract structured information from an error message.
// It looks for patterns like "file:line:column" or "line N" in error messages.
func parseErrorToDiagnostic(err error, defaultPath string) Diagnostic {
	errMsg := err.Error()

	// Try to extract file:line:column pattern (e.g., "schema/users.lp.sql:5:1")
	fileLocPattern := regexp.MustCompile(`([^:]+):(\d+):(\d+)`)
	if matches := fileLocPattern.FindStringSubmatch(errMsg); len(matches) == 4 {
		file := matches[1]
		line, _ := strconv.Atoi(matches[2])
		column, _ := strconv.Atoi(matches[3])

		// Extract the actual error message (everything after the location)
		messageParts := strings.SplitN(errMsg, ":", 4)
		message := errMsg
		if len(messageParts) == 4 {
			message = strings.TrimSpace(messageParts[3])
		}

		return Diagnostic{
			Message:  message,
			Severity: "error",
			File:     file,
			Line:     line,
			Column:   column,
		}
	}

	// Try to extract "line N" pattern
	linePattern := regexp.MustCompile(`line (\d+)`)
	if matches := linePattern.FindStringSubmatch(errMsg); len(matches) == 2 {
		line, _ := strconv.Atoi(matches[1])
		return Diagnostic{
			Message:  errMsg,
			Severity: "error",
			File:     defaultPath,
			Line:     line,
			Column:   1,
		}
	}

	// If we can't extract location info, return a generic diagnostic
	return Diagnostic{
		Message:  errMsg,
		Severity: "error",
		File:     defaultPath,
		Line:     1,
		Column:   1,
	}
}
