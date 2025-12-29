package postgres

import (
	"strings"
	"testing"

	"github.com/lockplane/lockplane/internal/database"
	"github.com/lockplane/lockplane/internal/schema"
)

// Helper function to create a pointer to a string
func strPtr(s string) *string {
	return &s
}

func TestGenerator_FormatColumnDefinition(t *testing.T) {
	gen := NewGenerator()

	tests := []struct {
		name     string
		column   database.Column
		expected string
	}{
		{
			name:     "basic nullable column",
			column:   database.Column{Name: "name", Type: "text", Nullable: true},
			expected: "name text",
		},
		{
			name:     "not null column",
			column:   database.Column{Name: "email", Type: "text", Nullable: false},
			expected: "email text NOT NULL",
		},
		{
			name:     "column with default",
			column:   database.Column{Name: "age", Type: "integer", Nullable: true, Default: strPtr("0")},
			expected: "age integer DEFAULT 0",
		},
		{
			name:     "primary key column",
			column:   database.Column{Name: "id", Type: "integer", Nullable: false, IsPrimaryKey: true},
			expected: "id integer NOT NULL PRIMARY KEY",
		},
		{
			name:     "column with all constraints",
			column:   database.Column{Name: "id", Type: "serial", Nullable: false, Default: strPtr("nextval('seq')"), IsPrimaryKey: true},
			expected: "id serial NOT NULL DEFAULT nextval('seq') PRIMARY KEY",
		},
		{
			name:     "varchar with length",
			column:   database.Column{Name: "username", Type: "character varying", Nullable: false},
			expected: "username character varying NOT NULL",
		},
		{
			name:     "timestamp with default",
			column:   database.Column{Name: "created_at", Type: "timestamp without time zone", Nullable: false, Default: strPtr("CURRENT_TIMESTAMP")},
			expected: "created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP",
		},
		{
			name:     "boolean with default true",
			column:   database.Column{Name: "is_active", Type: "boolean", Nullable: false, Default: strPtr("true")},
			expected: "is_active boolean NOT NULL DEFAULT true",
		},
		{
			name:     "numeric with default",
			column:   database.Column{Name: "balance", Type: "numeric", Nullable: false, Default: strPtr("0.00")},
			expected: "balance numeric NOT NULL DEFAULT 0.00",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := gen.FormatColumnDefinition(tt.column)
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestGenerator_CreateTable(t *testing.T) {
	gen := NewGenerator()

	table := database.Table{
		Name: "users",
		Columns: []database.Column{
			{Name: "id", Type: "integer", Nullable: false, IsPrimaryKey: true},
			{Name: "email", Type: "text", Nullable: false},
			{Name: "age", Type: "integer", Nullable: true},
		},
	}

	sql := gen.CreateTable(table)

	if !strings.Contains(sql, "CREATE TABLE users") {
		t.Errorf("Expected SQL to contain 'CREATE TABLE users', got: %s", sql)
	}

	if !strings.Contains(sql, "id integer NOT NULL PRIMARY KEY") {
		t.Errorf("Expected SQL to contain id column definition, got: %s", sql)
	}

	if !strings.Contains(sql, "email text NOT NULL") {
		t.Errorf("Expected SQL to contain email column definition, got: %s", sql)
	}

	if !strings.Contains(sql, "age integer") && strings.Contains(sql, "age") {
		// Should have age without NOT NULL
		if strings.Contains(sql, "age integer NOT NULL") {
			t.Errorf("Expected age to be nullable, got: %s", sql)
		}
	}
}

func TestGenerator_CreateTable_WithDefaults(t *testing.T) {
	gen := NewGenerator()

	table := database.Table{
		Name: "posts",
		Columns: []database.Column{
			{Name: "id", Type: "serial", Nullable: false, IsPrimaryKey: true},
			{Name: "title", Type: "text", Nullable: false},
			{Name: "view_count", Type: "integer", Nullable: false, Default: strPtr("0")},
			{Name: "created_at", Type: "timestamp without time zone", Nullable: false, Default: strPtr("CURRENT_TIMESTAMP")},
			{Name: "is_published", Type: "boolean", Nullable: false, Default: strPtr("false")},
		},
	}

	sql := gen.CreateTable(table)

	expectedParts := []string{
		"CREATE TABLE posts",
		"id serial NOT NULL PRIMARY KEY",
		"title text NOT NULL",
		"view_count integer NOT NULL DEFAULT 0",
		"created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP",
		"is_published boolean NOT NULL DEFAULT false",
	}

	for _, part := range expectedParts {
		if !strings.Contains(sql, part) {
			t.Errorf("Expected SQL to contain %q, got:\n%s", part, sql)
		}
	}
}

func TestGenerator_CreateTable_SingleColumn(t *testing.T) {
	gen := NewGenerator()

	table := database.Table{
		Name: "simple",
		Columns: []database.Column{
			{Name: "id", Type: "integer", Nullable: false, IsPrimaryKey: true},
		},
	}

	sql := gen.CreateTable(table)
	expected := "CREATE TABLE simple (\n  id integer NOT NULL PRIMARY KEY\n);"

	if sql != expected {
		t.Errorf("Expected:\n%s\n\nGot:\n%s", expected, sql)
	}
}

func TestGenerator_DropTable(t *testing.T) {
	gen := NewGenerator()

	tests := []struct {
		name     string
		table    database.Table
		expected string
	}{
		{
			name:     "simple table",
			table:    database.Table{Name: "users"},
			expected: "DROP TABLE users CASCADE;",
		},
		{
			name:     "table with underscores",
			table:    database.Table{Name: "user_sessions"},
			expected: "DROP TABLE user_sessions CASCADE;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := gen.DropTable(tt.table)
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestGenerator_AddColumn(t *testing.T) {
	gen := NewGenerator()

	tests := []struct {
		name      string
		tableName string
		column    database.Column
		expected  string
	}{
		{
			name:      "add simple column",
			tableName: "users",
			column:    database.Column{Name: "nickname", Type: "text", Nullable: true},
			expected:  "ALTER TABLE users ADD COLUMN nickname text;",
		},
		{
			name:      "add not null column",
			tableName: "users",
			column:    database.Column{Name: "email", Type: "text", Nullable: false},
			expected:  "ALTER TABLE users ADD COLUMN email text NOT NULL;",
		},
		{
			name:      "add column with default",
			tableName: "users",
			column:    database.Column{Name: "status", Type: "text", Nullable: false, Default: strPtr("'active'")},
			expected:  "ALTER TABLE users ADD COLUMN status text NOT NULL DEFAULT 'active';",
		},
		{
			name:      "add timestamp with default",
			tableName: "posts",
			column:    database.Column{Name: "created_at", Type: "timestamp without time zone", Nullable: false, Default: strPtr("CURRENT_TIMESTAMP")},
			expected:  "ALTER TABLE posts ADD COLUMN created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := gen.AddColumn(tt.tableName, tt.column)
			if result != tt.expected {
				t.Errorf("Expected:\n%s\n\nGot:\n%s", tt.expected, result)
			}
		})
	}
}

func TestGenerator_DropColumn(t *testing.T) {
	gen := NewGenerator()

	tests := []struct {
		name      string
		tableName string
		column    database.Column
		expected  string
	}{
		{
			name:      "drop simple column",
			tableName: "users",
			column:    database.Column{Name: "nickname"},
			expected:  "ALTER TABLE users DROP COLUMN nickname;",
		},
		{
			name:      "drop column from different table",
			tableName: "posts",
			column:    database.Column{Name: "deprecated_field"},
			expected:  "ALTER TABLE posts DROP COLUMN deprecated_field;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := gen.DropColumn(tt.tableName, tt.column)
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestGenerator_ModifyColumn_TypeChange(t *testing.T) {
	gen := NewGenerator()

	diff := schema.ColumnDiff{
		ColumnName: "age",
		Old:        database.Column{Name: "age", Type: "integer"},
		New:        database.Column{Name: "age", Type: "bigint"},
		Changes:    []string{"type"},
	}

	result := gen.ModifyColumn("users", diff)
	expected := "ALTER TABLE users ALTER COLUMN age TYPE bigint;"

	if result != expected {
		t.Errorf("Expected:\n%s\n\nGot:\n%s", expected, result)
	}
}

func TestGenerator_ModifyColumn_NullableChange(t *testing.T) {
	gen := NewGenerator()

	tests := []struct {
		name     string
		diff     schema.ColumnDiff
		expected string
	}{
		{
			name: "set not null",
			diff: schema.ColumnDiff{
				ColumnName: "email",
				Old:        database.Column{Name: "email", Type: "text", Nullable: true},
				New:        database.Column{Name: "email", Type: "text", Nullable: false},
				Changes:    []string{"nullable"},
			},
			expected: "ALTER TABLE users ALTER COLUMN email SET NOT NULL;",
		},
		{
			name: "drop not null",
			diff: schema.ColumnDiff{
				ColumnName: "middle_name",
				Old:        database.Column{Name: "middle_name", Type: "text", Nullable: false},
				New:        database.Column{Name: "middle_name", Type: "text", Nullable: true},
				Changes:    []string{"nullable"},
			},
			expected: "ALTER TABLE users ALTER COLUMN middle_name DROP NOT NULL;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := gen.ModifyColumn("users", tt.diff)
			if result != tt.expected {
				t.Errorf("Expected:\n%s\n\nGot:\n%s", tt.expected, result)
			}
		})
	}
}

func TestGenerator_ModifyColumn_DefaultChange(t *testing.T) {
	gen := NewGenerator()

	tests := []struct {
		name     string
		diff     schema.ColumnDiff
		expected string
	}{
		{
			name: "set default",
			diff: schema.ColumnDiff{
				ColumnName: "status",
				Old:        database.Column{Name: "status", Type: "text", Default: nil},
				New:        database.Column{Name: "status", Type: "text", Default: strPtr("'active'")},
				Changes:    []string{"default"},
			},
			expected: "ALTER TABLE users ALTER COLUMN status SET DEFAULT 'active';",
		},
		{
			name: "drop default",
			diff: schema.ColumnDiff{
				ColumnName: "counter",
				Old:        database.Column{Name: "counter", Type: "integer", Default: strPtr("0")},
				New:        database.Column{Name: "counter", Type: "integer", Default: nil},
				Changes:    []string{"default"},
			},
			expected: "ALTER TABLE users ALTER COLUMN counter DROP DEFAULT;",
		},
		{
			name: "change default value",
			diff: schema.ColumnDiff{
				ColumnName: "priority",
				Old:        database.Column{Name: "priority", Type: "integer", Default: strPtr("1")},
				New:        database.Column{Name: "priority", Type: "integer", Default: strPtr("10")},
				Changes:    []string{"default"},
			},
			expected: "ALTER TABLE users ALTER COLUMN priority SET DEFAULT 10;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := gen.ModifyColumn("users", tt.diff)
			if result != tt.expected {
				t.Errorf("Expected:\n%s\n\nGot:\n%s", tt.expected, result)
			}
		})
	}
}

func TestGenerator_ModifyColumn_MultipleChanges(t *testing.T) {
	gen := NewGenerator()

	diff := schema.ColumnDiff{
		ColumnName: "age",
		Old:        database.Column{Name: "age", Type: "integer", Nullable: true, Default: nil},
		New:        database.Column{Name: "age", Type: "bigint", Nullable: false, Default: strPtr("0")},
		Changes:    []string{"type", "nullable", "default"},
	}

	result := gen.ModifyColumn("users", diff)

	// Check all three ALTER statements are present
	if !strings.Contains(result, "ALTER TABLE users ALTER COLUMN age TYPE bigint;") {
		t.Error("Expected type change statement")
	}
	if !strings.Contains(result, "ALTER TABLE users ALTER COLUMN age SET NOT NULL;") {
		t.Error("Expected set not null statement")
	}
	if !strings.Contains(result, "ALTER TABLE users ALTER COLUMN age SET DEFAULT 0;") {
		t.Error("Expected set default statement")
	}
}

func TestGenerator_GenerateMigration_AddTable(t *testing.T) {
	gen := NewGenerator()

	diff := &schema.SchemaDiff{
		AddedTables: []database.Table{
			{
				Name: "users",
				Columns: []database.Column{
					{Name: "id", Type: "serial", Nullable: false, IsPrimaryKey: true},
					{Name: "email", Type: "text", Nullable: false},
				},
			},
		},
	}

	sql := gen.GenerateMigration(diff)

	if !strings.Contains(sql, "CREATE TABLE users") {
		t.Error("Expected CREATE TABLE statement")
	}
	if !strings.Contains(sql, "id serial NOT NULL PRIMARY KEY") {
		t.Error("Expected id column definition")
	}
	if !strings.Contains(sql, "email text NOT NULL") {
		t.Error("Expected email column definition")
	}
}

func TestGenerator_GenerateMigration_DropTable(t *testing.T) {
	gen := NewGenerator()

	diff := &schema.SchemaDiff{
		RemovedTables: []database.Table{
			{Name: "old_table"},
		},
	}

	sql := gen.GenerateMigration(diff)
	expected := "DROP TABLE old_table CASCADE;"

	if sql != expected {
		t.Errorf("Expected:\n%s\n\nGot:\n%s", expected, sql)
	}
}

func TestGenerator_GenerateMigration_ModifyTable(t *testing.T) {
	gen := NewGenerator()

	diff := &schema.SchemaDiff{
		ModifiedTables: []schema.TableDiff{
			{
				TableName: "users",
				ModifiedColumns: []schema.ColumnDiff{
					{
						ColumnName: "age",
						Old:        database.Column{Name: "age", Type: "integer"},
						New:        database.Column{Name: "age", Type: "bigint"},
						Changes:    []string{"type"},
					},
				},
			},
		},
	}

	sql := gen.GenerateMigration(diff)
	expected := "ALTER TABLE users ALTER COLUMN age TYPE bigint;"

	if sql != expected {
		t.Errorf("Expected:\n%s\n\nGot:\n%s", expected, sql)
	}
}

func TestGenerator_GenerateMigration_Complex(t *testing.T) {
	gen := NewGenerator()

	diff := &schema.SchemaDiff{
		AddedTables: []database.Table{
			{
				Name: "posts",
				Columns: []database.Column{
					{Name: "id", Type: "serial", Nullable: false, IsPrimaryKey: true},
					{Name: "title", Type: "text", Nullable: false},
				},
			},
		},
		ModifiedTables: []schema.TableDiff{
			{
				TableName: "users",
				ModifiedColumns: []schema.ColumnDiff{
					{
						ColumnName: "age",
						Old:        database.Column{Name: "age", Type: "integer", Nullable: true},
						New:        database.Column{Name: "age", Type: "bigint", Nullable: false},
						Changes:    []string{"type", "nullable"},
					},
					{
						ColumnName: "status",
						Old:        database.Column{Name: "status", Type: "text", Default: nil},
						New:        database.Column{Name: "status", Type: "text", Default: strPtr("'active'")},
						Changes:    []string{"default"},
					},
				},
			},
		},
		RemovedTables: []database.Table{
			{Name: "deprecated_table"},
		},
	}

	sql := gen.GenerateMigration(diff)

	// Check added table
	if !strings.Contains(sql, "CREATE TABLE posts") {
		t.Error("Expected CREATE TABLE posts")
	}

	// Check modified columns
	if !strings.Contains(sql, "ALTER TABLE users ALTER COLUMN age TYPE bigint;") {
		t.Error("Expected age type change")
	}
	if !strings.Contains(sql, "ALTER TABLE users ALTER COLUMN age SET NOT NULL;") {
		t.Error("Expected age set not null")
	}
	if !strings.Contains(sql, "ALTER TABLE users ALTER COLUMN status SET DEFAULT 'active';") {
		t.Error("Expected status default change")
	}

	// Check removed table
	if !strings.Contains(sql, "DROP TABLE deprecated_table CASCADE;") {
		t.Error("Expected DROP TABLE deprecated_table")
	}
}

func TestGenerator_GenerateMigration_Empty(t *testing.T) {
	gen := NewGenerator()

	diff := &schema.SchemaDiff{}

	sql := gen.GenerateMigration(diff)

	if sql != "" {
		t.Errorf("Expected empty string for empty diff, got: %q", sql)
	}
}
func TestGenerator_GenerateMigration_EnableRLS(t *testing.T) {
	gen := NewGenerator()

	diff := &schema.SchemaDiff{
		ModifiedTables: []schema.TableDiff{
			{
				TableName:  "users",
				RLSChanged: true,
				RLSEnabled: true,
			},
		},
	}

	sql := gen.GenerateMigration(diff)

	expected := "ALTER TABLE users ENABLE ROW LEVEL SECURITY;"
	if sql != expected {
		t.Errorf("Expected:\n%s\n\nGot:\n%s", expected, sql)
	}
}

func TestGenerator_GenerateMigration_DisableRLS(t *testing.T) {
	gen := NewGenerator()

	diff := &schema.SchemaDiff{
		ModifiedTables: []schema.TableDiff{
			{
				TableName:  "users",
				RLSChanged: true,
				RLSEnabled: false,
			},
		},
	}

	sql := gen.GenerateMigration(diff)

	expected := "ALTER TABLE users DISABLE ROW LEVEL SECURITY;"
	if sql != expected {
		t.Errorf("Expected:\n%s\n\nGot:\n%s", expected, sql)
	}
}

func TestGenerator_GenerateMigration_CreateTableWithRLS(t *testing.T) {
	gen := NewGenerator()

	diff := &schema.SchemaDiff{
		AddedTables: []database.Table{
			{
				Name:       "users",
				RLSEnabled: true,
				Columns: []database.Column{
					{Name: "id", Type: "integer", IsPrimaryKey: true},
					{Name: "email", Type: "text", Nullable: false},
				},
			},
		},
	}

	sql := gen.GenerateMigration(diff)

	if !strings.Contains(sql, "CREATE TABLE users") {
		t.Error("Expected CREATE TABLE statement")
	}
	if !strings.Contains(sql, "ALTER TABLE users ENABLE ROW LEVEL SECURITY") {
		t.Error("Expected ENABLE RLS statement")
	}
}

func TestGenerator_GenerateMigration_CreateTableWithoutRLS(t *testing.T) {
	gen := NewGenerator()

	diff := &schema.SchemaDiff{
		AddedTables: []database.Table{
			{
				Name:       "users",
				RLSEnabled: false,
				Columns: []database.Column{
					{Name: "id", Type: "integer", IsPrimaryKey: true},
				},
			},
		},
	}

	sql := gen.GenerateMigration(diff)

	if !strings.Contains(sql, "CREATE TABLE users") {
		t.Error("Expected CREATE TABLE statement")
	}
	if strings.Contains(sql, "ROW LEVEL SECURITY") {
		t.Error("Did not expect RLS statement for table without RLS")
	}
}

func TestGenerator_GenerateMigration_MultipleTablesWithRLS(t *testing.T) {
	gen := NewGenerator()

	diff := &schema.SchemaDiff{
		ModifiedTables: []schema.TableDiff{
			{
				TableName:  "users",
				RLSChanged: true,
				RLSEnabled: true,
			},
			{
				TableName:  "posts",
				RLSChanged: true,
				RLSEnabled: false,
			},
		},
	}

	sql := gen.GenerateMigration(diff)

	if !strings.Contains(sql, "ALTER TABLE users ENABLE ROW LEVEL SECURITY") {
		t.Error("Expected ENABLE RLS for users table")
	}
	if !strings.Contains(sql, "ALTER TABLE posts DISABLE ROW LEVEL SECURITY") {
		t.Error("Expected DISABLE RLS for posts table")
	}
}

func TestGenerator_GenerateMigration_AddColumns(t *testing.T) {
	gen := NewGenerator()

	diff := &schema.SchemaDiff{
		ModifiedTables: []schema.TableDiff{
			{
				TableName: "users",
				AddedColumns: []database.Column{
					{Name: "other_id", Type: "integer", Nullable: true, IsPrimaryKey: false},
				},
			},
		},
	}

	sql := gen.GenerateMigration(diff)
	expected := "ALTER TABLE users ADD COLUMN other_id integer;"

	if sql != expected {
		t.Errorf("Expected:\n%s\n\nGot:\n%s", expected, sql)
	}
}

func TestGenerator_GenerateMigration_RemoveColumns(t *testing.T) {
	gen := NewGenerator()

	diff := &schema.SchemaDiff{
		ModifiedTables: []schema.TableDiff{
			{
				TableName: "users",
				RemovedColumns: []database.Column{
					{Name: "deprecated_field", Type: "text"},
				},
			},
		},
	}

	sql := gen.GenerateMigration(diff)
	expected := "ALTER TABLE users DROP COLUMN deprecated_field;"

	if sql != expected {
		t.Errorf("Expected:\n%s\n\nGot:\n%s", expected, sql)
	}
}

func TestGenerator_GenerateMigration_AddAndRemoveColumns(t *testing.T) {
	gen := NewGenerator()

	diff := &schema.SchemaDiff{
		ModifiedTables: []schema.TableDiff{
			{
				TableName: "users",
				AddedColumns: []database.Column{
					{Name: "new_field", Type: "text", Nullable: false},
				},
				RemovedColumns: []database.Column{
					{Name: "old_field", Type: "integer"},
				},
			},
		},
	}

	sql := gen.GenerateMigration(diff)

	if !strings.Contains(sql, "ALTER TABLE users ADD COLUMN new_field text NOT NULL") {
		t.Error("Expected ADD COLUMN statement for new_field")
	}
	if !strings.Contains(sql, "ALTER TABLE users DROP COLUMN old_field") {
		t.Error("Expected DROP COLUMN statement for old_field")
	}
}

func TestGenerator_GenerateMigration_RLSWithColumnsAndOtherChanges(t *testing.T) {
	// End-to-end test: Verify that RLS changes work correctly in combination
	// with other table modifications (columns, constraints, etc.)
	gen := NewGenerator()

	diff := &schema.SchemaDiff{
		ModifiedTables: []schema.TableDiff{
			{
				TableName: "users",
				AddedColumns: []database.Column{
					{Name: "phone", Type: "text", Nullable: true},
				},
				RemovedColumns: []database.Column{
					{Name: "deprecated", Type: "text"},
				},
				ModifiedColumns: []schema.ColumnDiff{
					{
						ColumnName: "age",
						Old:        database.Column{Name: "age", Type: "integer"},
						New:        database.Column{Name: "age", Type: "bigint"},
						Changes:    []string{"type"},
					},
				},
				RLSChanged: true,
				RLSEnabled: true,
			},
		},
	}

	sql := gen.GenerateMigration(diff)

	// Verify all changes are present in the migration
	if !strings.Contains(sql, "ALTER TABLE users ADD COLUMN phone text") {
		t.Error("Expected ADD COLUMN statement")
	}
	if !strings.Contains(sql, "ALTER TABLE users DROP COLUMN deprecated") {
		t.Error("Expected DROP COLUMN statement")
	}
	if !strings.Contains(sql, "ALTER TABLE users ALTER COLUMN age TYPE bigint") {
		t.Error("Expected ALTER COLUMN statement")
	}
	if !strings.Contains(sql, "ALTER TABLE users ENABLE ROW LEVEL SECURITY") {
		t.Error("Expected ENABLE RLS statement")
	}
}
