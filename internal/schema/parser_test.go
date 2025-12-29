package schema

import (
	"testing"

	"github.com/lockplane/lockplane/internal/database"
)

func TestParseBasicCreateTable(t *testing.T) {
	sql := `CREATE TABLE users (id INTEGER);`

	schema, err := ParseSQLSchemaWithDialect(sql, database.DialectPostgres)
	if err != nil {
		t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
	}

	if len(schema.Tables) != 1 {
		t.Fatalf("Expected 1 table, got %d", len(schema.Tables))
	}

	table := schema.Tables[0]
	if table.Name != "users" {
		t.Errorf("Expected table name 'users', got %q", table.Name)
	}

	if len(table.Columns) != 1 {
		t.Fatalf("Expected 1 column, got %d", len(table.Columns))
	}

	col := table.Columns[0]
	if col.Name != "id" {
		t.Errorf("Expected column name 'id', got %q", col.Name)
	}
	if col.Type != "integer" {
		t.Errorf("Expected column type 'integer', got %q", col.Type)
	}
	if !col.Nullable {
		t.Error("Expected column to be nullable by default")
	}
	if col.IsPrimaryKey {
		t.Error("Expected column to not be primary key")
	}
}

func TestParseTableWithMultipleColumns(t *testing.T) {
	sql := `
		CREATE TABLE products (
			id INTEGER,
			name TEXT,
			price NUMERIC,
			created_at TIMESTAMP
		);
	`

	schema, err := ParseSQLSchemaWithDialect(sql, database.DialectPostgres)
	if err != nil {
		t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
	}

	if len(schema.Tables) != 1 {
		t.Fatalf("Expected 1 table, got %d", len(schema.Tables))
	}

	table := schema.Tables[0]
	if len(table.Columns) != 4 {
		t.Fatalf("Expected 4 columns, got %d", len(table.Columns))
	}

	expectedColumns := []struct {
		name string
		typ  string
	}{
		{"id", "integer"},
		{"name", "text"},
		{"price", "numeric"},
		{"created_at", "timestamp without time zone"},
	}

	for i, expected := range expectedColumns {
		col := table.Columns[i]
		if col.Name != expected.name {
			t.Errorf("Column %d: expected name %q, got %q", i, expected.name, col.Name)
		}
		if col.Type != expected.typ {
			t.Errorf("Column %d: expected type %q, got %q", i, expected.typ, col.Type)
		}
	}
}

func TestParseIntegerTypeNormalization(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		expectedType string
	}{
		{"INT2", "CREATE TABLE t (col INT2);", "smallint"},
		{"SMALLINT", "CREATE TABLE t (col SMALLINT);", "smallint"},
		{"INT4", "CREATE TABLE t (col INT4);", "integer"},
		{"INTEGER", "CREATE TABLE t (col INTEGER);", "integer"},
		{"INT", "CREATE TABLE t (col INT);", "integer"},
		{"INT8", "CREATE TABLE t (col INT8);", "bigint"},
		{"BIGINT", "CREATE TABLE t (col BIGINT);", "bigint"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, err := ParseSQLSchemaWithDialect(tt.sql, database.DialectPostgres)
			if err != nil {
				t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
			}

			if len(schema.Tables[0].Columns) != 1 {
				t.Fatalf("Expected 1 column, got %d", len(schema.Tables[0].Columns))
			}

			col := schema.Tables[0].Columns[0]
			if col.Type != tt.expectedType {
				t.Errorf("Expected type %q, got %q", tt.expectedType, col.Type)
			}
		})
	}
}

func TestParseSerialTypeNormalization(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		expectedType string
	}{
		{"SERIAL2", "CREATE TABLE t (col SERIAL2);", "smallserial"},
		{"SMALLSERIAL", "CREATE TABLE t (col SMALLSERIAL);", "smallserial"},
		{"SERIAL4", "CREATE TABLE t (col SERIAL4);", "serial"},
		{"SERIAL", "CREATE TABLE t (col SERIAL);", "serial"},
		{"SERIAL8", "CREATE TABLE t (col SERIAL8);", "bigserial"},
		{"BIGSERIAL", "CREATE TABLE t (col BIGSERIAL);", "bigserial"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, err := ParseSQLSchemaWithDialect(tt.sql, database.DialectPostgres)
			if err != nil {
				t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
			}

			col := schema.Tables[0].Columns[0]
			if col.Type != tt.expectedType {
				t.Errorf("Expected type %q, got %q", tt.expectedType, col.Type)
			}
		})
	}
}

func TestParseBooleanTypeNormalization(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		expectedType string
	}{
		{"BOOL", "CREATE TABLE t (col BOOL);", "boolean"},
		{"BOOLEAN", "CREATE TABLE t (col BOOLEAN);", "boolean"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, err := ParseSQLSchemaWithDialect(tt.sql, database.DialectPostgres)
			if err != nil {
				t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
			}

			col := schema.Tables[0].Columns[0]
			if col.Type != tt.expectedType {
				t.Errorf("Expected type %q, got %q", tt.expectedType, col.Type)
			}
		})
	}
}

func TestParseFloatingPointTypeNormalization(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		expectedType string
	}{
		{"FLOAT4", "CREATE TABLE t (col FLOAT4);", "real"},
		{"REAL", "CREATE TABLE t (col REAL);", "real"},
		{"FLOAT8", "CREATE TABLE t (col FLOAT8);", "double precision"},
		{"DOUBLE_PRECISION", "CREATE TABLE t (col DOUBLE PRECISION);", "double precision"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, err := ParseSQLSchemaWithDialect(tt.sql, database.DialectPostgres)
			if err != nil {
				t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
			}

			col := schema.Tables[0].Columns[0]
			if col.Type != tt.expectedType {
				t.Errorf("Expected type %q, got %q", tt.expectedType, col.Type)
			}
		})
	}
}

func TestParseCharacterTypeNormalization(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		expectedType string
	}{
		{"VARCHAR", "CREATE TABLE t (col VARCHAR);", "varchar"},
		{"BPCHAR", "CREATE TABLE t (col CHAR);", "char(1)"}, // CHAR defaults to CHAR(1) in PostgreSQL
		{"TEXT", "CREATE TABLE t (col TEXT);", "text"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, err := ParseSQLSchemaWithDialect(tt.sql, database.DialectPostgres)
			if err != nil {
				t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
			}

			col := schema.Tables[0].Columns[0]
			if col.Type != tt.expectedType {
				t.Errorf("Expected type %q, got %q", tt.expectedType, col.Type)
			}
		})
	}
}

func TestParseTimestampTypeNormalization(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		expectedType string
	}{
		{"TIMESTAMP", "CREATE TABLE t (col TIMESTAMP);", "timestamp without time zone"},
		{"TIMESTAMPTZ", "CREATE TABLE t (col TIMESTAMPTZ);", "timestamp with time zone"},
		{"TIMESTAMP_WITH_TIME_ZONE", "CREATE TABLE t (col TIMESTAMP WITH TIME ZONE);", "timestamp with time zone"},
		{"TIME", "CREATE TABLE t (col TIME);", "time without time zone"},
		{"TIMETZ", "CREATE TABLE t (col TIMETZ);", "time with time zone"},
		{"TIME_WITH_TIME_ZONE", "CREATE TABLE t (col TIME WITH TIME ZONE);", "time with time zone"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, err := ParseSQLSchemaWithDialect(tt.sql, database.DialectPostgres)
			if err != nil {
				t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
			}

			col := schema.Tables[0].Columns[0]
			if col.Type != tt.expectedType {
				t.Errorf("Expected type %q, got %q", tt.expectedType, col.Type)
			}
		})
	}
}

func TestParseTypeWithModifiers(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		expectedType string
	}{
		{"VARCHAR_255", "CREATE TABLE t (col VARCHAR(255));", "varchar(255)"},
		{"VARCHAR_100", "CREATE TABLE t (col VARCHAR(100));", "varchar(100)"},
		{"CHAR_10", "CREATE TABLE t (col CHAR(10));", "char(10)"},
		{"NUMERIC_10_2", "CREATE TABLE t (col NUMERIC(10,2));", "numeric(10,2)"},
		{"DECIMAL_8_4", "CREATE TABLE t (col DECIMAL(8,4));", "numeric(8,4)"}, // PostgreSQL treats DECIMAL as NUMERIC
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, err := ParseSQLSchemaWithDialect(tt.sql, database.DialectPostgres)
			if err != nil {
				t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
			}

			col := schema.Tables[0].Columns[0]
			if col.Type != tt.expectedType {
				t.Errorf("Expected type %q, got %q", tt.expectedType, col.Type)
			}
		})
	}
}

func TestParseArrayTypes(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		expectedType string
	}{
		{"INTEGER_ARRAY", "CREATE TABLE t (col INTEGER[]);", "integer[]"},
		{"TEXT_ARRAY", "CREATE TABLE t (col TEXT[]);", "text[]"},
		{"VARCHAR_ARRAY", "CREATE TABLE t (col VARCHAR(50)[]);", "varchar(50)[]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, err := ParseSQLSchemaWithDialect(tt.sql, database.DialectPostgres)
			if err != nil {
				t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
			}

			col := schema.Tables[0].Columns[0]
			if col.Type != tt.expectedType {
				t.Errorf("Expected type %q, got %q", tt.expectedType, col.Type)
			}
		})
	}
}

func TestParseNotNullConstraint(t *testing.T) {
	sql := `CREATE TABLE users (id INTEGER NOT NULL);`

	schema, err := ParseSQLSchemaWithDialect(sql, database.DialectPostgres)
	if err != nil {
		t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
	}

	col := schema.Tables[0].Columns[0]
	if col.Nullable {
		t.Error("Expected column to be NOT NULL")
	}
}

func TestParseNullConstraint(t *testing.T) {
	sql := `CREATE TABLE users (id INTEGER NULL);`

	schema, err := ParseSQLSchemaWithDialect(sql, database.DialectPostgres)
	if err != nil {
		t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
	}

	col := schema.Tables[0].Columns[0]
	if !col.Nullable {
		t.Error("Expected column to be NULL")
	}
}

func TestParsePrimaryKeyConstraint(t *testing.T) {
	sql := `CREATE TABLE users (id INTEGER PRIMARY KEY);`

	schema, err := ParseSQLSchemaWithDialect(sql, database.DialectPostgres)
	if err != nil {
		t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
	}

	col := schema.Tables[0].Columns[0]
	if !col.IsPrimaryKey {
		t.Error("Expected column to be PRIMARY KEY")
	}
	if col.Nullable {
		t.Error("Expected PRIMARY KEY column to be NOT NULL")
	}
}

func TestParseDefaultIntegerLiteral(t *testing.T) {
	sql := `CREATE TABLE users (age INTEGER DEFAULT 0);`

	schema, err := ParseSQLSchemaWithDialect(sql, database.DialectPostgres)
	if err != nil {
		t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
	}

	col := schema.Tables[0].Columns[0]
	if col.Default == nil {
		t.Fatal("Expected column to have default value")
	}
	if *col.Default != "0" {
		t.Errorf("Expected default value '0', got %q", *col.Default)
	}
}

func TestParseDefaultStringLiteral(t *testing.T) {
	sql := `CREATE TABLE users (status TEXT DEFAULT 'active');`

	schema, err := ParseSQLSchemaWithDialect(sql, database.DialectPostgres)
	if err != nil {
		t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
	}

	col := schema.Tables[0].Columns[0]
	if col.Default == nil {
		t.Fatal("Expected column to have default value")
	}
	if *col.Default != "'active'" {
		t.Errorf("Expected default value \"'active'\", got %q", *col.Default)
	}
}

func TestParseDefaultCurrentTimestamp(t *testing.T) {
	sql := `CREATE TABLE users (created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);`

	schema, err := ParseSQLSchemaWithDialect(sql, database.DialectPostgres)
	if err != nil {
		t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
	}

	col := schema.Tables[0].Columns[0]
	if col.Default == nil {
		t.Fatal("Expected column to have default value")
	}
	if *col.Default != "CURRENT_TIMESTAMP" {
		t.Errorf("Expected default value 'CURRENT_TIMESTAMP', got %q", *col.Default)
	}
}

func TestParseDefaultNowFunction(t *testing.T) {
	sql := `CREATE TABLE users (created_at TIMESTAMP DEFAULT NOW());`

	schema, err := ParseSQLSchemaWithDialect(sql, database.DialectPostgres)
	if err != nil {
		t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
	}

	col := schema.Tables[0].Columns[0]
	if col.Default == nil {
		t.Fatal("Expected column to have default value")
	}
	if *col.Default != "now()" {
		t.Errorf("Expected default value 'now()', got %q", *col.Default)
	}
}

func TestParseSQLValueFunctions(t *testing.T) {
	tests := []struct {
		name          string
		sql           string
		expectedValue string
	}{
		{"CURRENT_DATE", "CREATE TABLE t (col DATE DEFAULT CURRENT_DATE);", "CURRENT_DATE"},
		{"CURRENT_TIME", "CREATE TABLE t (col TIME DEFAULT CURRENT_TIME);", "CURRENT_TIME"},
		{"CURRENT_TIMESTAMP", "CREATE TABLE t (col TIMESTAMP DEFAULT CURRENT_TIMESTAMP);", "CURRENT_TIMESTAMP"},
		{"LOCALTIME", "CREATE TABLE t (col TIME DEFAULT LOCALTIME);", "LOCALTIME"},
		{"LOCALTIMESTAMP", "CREATE TABLE t (col TIMESTAMP DEFAULT LOCALTIMESTAMP);", "LOCALTIMESTAMP"},
		{"CURRENT_USER", "CREATE TABLE t (col TEXT DEFAULT CURRENT_USER);", "CURRENT_USER"},
		{"SESSION_USER", "CREATE TABLE t (col TEXT DEFAULT SESSION_USER);", "SESSION_USER"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, err := ParseSQLSchemaWithDialect(tt.sql, database.DialectPostgres)
			if err != nil {
				t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
			}

			col := schema.Tables[0].Columns[0]
			if col.Default == nil {
				t.Fatal("Expected column to have default value")
			}
			if *col.Default != tt.expectedValue {
				t.Errorf("Expected default value %q, got %q", tt.expectedValue, *col.Default)
			}
		})
	}
}

func TestParseMultipleConstraints(t *testing.T) {
	sql := `CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		email TEXT NOT NULL,
		age INTEGER DEFAULT 0,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	);`

	schema, err := ParseSQLSchemaWithDialect(sql, database.DialectPostgres)
	if err != nil {
		t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
	}

	table := schema.Tables[0]
	if len(table.Columns) != 4 {
		t.Fatalf("Expected 4 columns, got %d", len(table.Columns))
	}

	// Check id column
	id := table.Columns[0]
	if id.Name != "id" {
		t.Errorf("Expected column name 'id', got %q", id.Name)
	}
	if !id.IsPrimaryKey {
		t.Error("Expected id to be PRIMARY KEY")
	}
	if id.Nullable {
		t.Error("Expected id to be NOT NULL (implied by PRIMARY KEY)")
	}

	// Check email column
	email := table.Columns[1]
	if email.Name != "email" {
		t.Errorf("Expected column name 'email', got %q", email.Name)
	}
	if email.Nullable {
		t.Error("Expected email to be NOT NULL")
	}

	// Check age column
	age := table.Columns[2]
	if age.Name != "age" {
		t.Errorf("Expected column name 'age', got %q", age.Name)
	}
	if age.Default == nil || *age.Default != "0" {
		t.Errorf("Expected age default to be '0', got %v", age.Default)
	}

	// Check created_at column
	createdAt := table.Columns[3]
	if createdAt.Name != "created_at" {
		t.Errorf("Expected column name 'created_at', got %q", createdAt.Name)
	}
	if createdAt.Nullable {
		t.Error("Expected created_at to be NOT NULL")
	}
	if createdAt.Default == nil || *createdAt.Default != "CURRENT_TIMESTAMP" {
		t.Errorf("Expected created_at default to be 'CURRENT_TIMESTAMP', got %v", createdAt.Default)
	}
}

func TestParseMultipleTables(t *testing.T) {
	sql := `
		CREATE TABLE users (id INTEGER);
		CREATE TABLE posts (id INTEGER, user_id INTEGER);
	`

	schema, err := ParseSQLSchemaWithDialect(sql, database.DialectPostgres)
	if err != nil {
		t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
	}

	if len(schema.Tables) != 2 {
		t.Fatalf("Expected 2 tables, got %d", len(schema.Tables))
	}

	if schema.Tables[0].Name != "users" {
		t.Errorf("Expected first table to be 'users', got %q", schema.Tables[0].Name)
	}
	if schema.Tables[1].Name != "posts" {
		t.Errorf("Expected second table to be 'posts', got %q", schema.Tables[1].Name)
	}

	if len(schema.Tables[0].Columns) != 1 {
		t.Errorf("Expected users table to have 1 column, got %d", len(schema.Tables[0].Columns))
	}
	if len(schema.Tables[1].Columns) != 2 {
		t.Errorf("Expected posts table to have 2 columns, got %d", len(schema.Tables[1].Columns))
	}
}

func TestParseInvalidSQL(t *testing.T) {
	sql := `CREATE TABLE users id INTEGER);` // Missing opening paren

	_, err := ParseSQLSchemaWithDialect(sql, database.DialectPostgres)
	if err == nil {
		t.Fatal("Expected error for invalid SQL, got nil")
	}
}

func TestParseEmptySQL(t *testing.T) {
	sql := ``

	schema, err := ParseSQLSchemaWithDialect(sql, database.DialectPostgres)
	if err != nil {
		t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
	}

	if len(schema.Tables) != 0 {
		t.Errorf("Expected 0 tables for empty SQL, got %d", len(schema.Tables))
	}
}

func TestParseUnsupportedDialect(t *testing.T) {
	sql := `CREATE TABLE users (id INTEGER);`

	_, err := ParseSQLSchemaWithDialect(sql, "mysql")
	if err == nil {
		t.Fatal("Expected error for unsupported dialect, got nil")
	}
	if err.Error() != "unsupported dialect mysql" {
		t.Errorf("Expected error message 'unsupported dialect mysql', got %q", err.Error())
	}
}

func TestParseSchemaDialect(t *testing.T) {
	sql := `CREATE TABLE users (id INTEGER);`

	schema, err := ParseSQLSchemaWithDialect(sql, database.DialectPostgres)
	if err != nil {
		t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
	}

	if schema.Dialect != database.DialectPostgres {
		t.Errorf("Expected schema dialect to be %q, got %q", database.DialectPostgres, schema.Dialect)
	}
}

func TestParseUUIDType(t *testing.T) {
	sql := `CREATE TABLE users (id UUID PRIMARY KEY);`

	schema, err := ParseSQLSchemaWithDialect(sql, database.DialectPostgres)
	if err != nil {
		t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
	}

	col := schema.Tables[0].Columns[0]
	if col.Type != "uuid" {
		t.Errorf("Expected type 'uuid', got %q", col.Type)
	}
}

func TestParseJSONBType(t *testing.T) {
	sql := `CREATE TABLE documents (data JSONB);`

	schema, err := ParseSQLSchemaWithDialect(sql, database.DialectPostgres)
	if err != nil {
		t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
	}

	col := schema.Tables[0].Columns[0]
	if col.Type != "jsonb" {
		t.Errorf("Expected type 'jsonb', got %q", col.Type)
	}
}

func TestParseBytesTypes(t *testing.T) {
	sql := `CREATE TABLE files (data BYTEA);`

	schema, err := ParseSQLSchemaWithDialect(sql, database.DialectPostgres)
	if err != nil {
		t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
	}

	col := schema.Tables[0].Columns[0]
	if col.Type != "bytea" {
		t.Errorf("Expected type 'bytea', got %q", col.Type)
	}
}

func TestParseJSONType(t *testing.T) {
	sql := `CREATE TABLE documents (data JSON);`

	schema, err := ParseSQLSchemaWithDialect(sql, database.DialectPostgres)
	if err != nil {
		t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
	}

	col := schema.Tables[0].Columns[0]
	if col.Type != "json" {
		t.Errorf("Expected type 'json', got %q", col.Type)
	}
}

func TestParseDateType(t *testing.T) {
	sql := `CREATE TABLE events (event_date DATE);`

	schema, err := ParseSQLSchemaWithDialect(sql, database.DialectPostgres)
	if err != nil {
		t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
	}

	col := schema.Tables[0].Columns[0]
	if col.Type != "date" {
		t.Errorf("Expected type 'date', got %q", col.Type)
	}
}

func TestParseIntervalType(t *testing.T) {
	sql := `CREATE TABLE schedules (duration INTERVAL);`

	schema, err := ParseSQLSchemaWithDialect(sql, database.DialectPostgres)
	if err != nil {
		t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
	}

	col := schema.Tables[0].Columns[0]
	if col.Type != "interval" {
		t.Errorf("Expected type 'interval', got %q", col.Type)
	}
}

func TestParseMoneyType(t *testing.T) {
	sql := `CREATE TABLE transactions (amount MONEY);`

	schema, err := ParseSQLSchemaWithDialect(sql, database.DialectPostgres)
	if err != nil {
		t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
	}

	col := schema.Tables[0].Columns[0]
	if col.Type != "money" {
		t.Errorf("Expected type 'money', got %q", col.Type)
	}
}

func TestParseDefaultBooleanLiteral(t *testing.T) {
	tests := []struct {
		name            string
		sql             string
		expectedDefault string
	}{
		{"TRUE", "CREATE TABLE t (col BOOLEAN DEFAULT TRUE);", "true"},
		{"FALSE", "CREATE TABLE t (col BOOLEAN DEFAULT FALSE);", "false"},
		{"true", "CREATE TABLE t (col BOOLEAN DEFAULT true);", "true"},
		{"false", "CREATE TABLE t (col BOOLEAN DEFAULT false);", "false"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, err := ParseSQLSchemaWithDialect(tt.sql, database.DialectPostgres)
			if err != nil {
				t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
			}

			col := schema.Tables[0].Columns[0]
			if col.Default == nil {
				t.Fatal("Expected column to have default value")
			}
			if *col.Default != tt.expectedDefault {
				t.Errorf("Expected default value %q, got %q", tt.expectedDefault, *col.Default)
			}
		})
	}
}

func TestParseDefaultNullLiteral(t *testing.T) {
	sql := `CREATE TABLE users (middle_name TEXT DEFAULT NULL);`

	schema, err := ParseSQLSchemaWithDialect(sql, database.DialectPostgres)
	if err != nil {
		t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
	}

	col := schema.Tables[0].Columns[0]
	if col.Default == nil {
		t.Fatal("Expected column to have default value")
	}
	if *col.Default != "NULL" {
		t.Errorf("Expected default value 'NULL', got %q", *col.Default)
	}
}

func TestParseComplexRealWorldTable(t *testing.T) {
	sql := `
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			username VARCHAR(50) NOT NULL,
			email VARCHAR(255) NOT NULL,
			password_hash TEXT NOT NULL,
			full_name TEXT,
			age INTEGER DEFAULT 0,
			balance NUMERIC(10,2) DEFAULT 0.00,
			is_active BOOLEAN NOT NULL DEFAULT TRUE,
			tags TEXT[],
			metadata JSONB,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP
		);
	`

	schema, err := ParseSQLSchemaWithDialect(sql, database.DialectPostgres)
	if err != nil {
		t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
	}

	if len(schema.Tables) != 1 {
		t.Fatalf("Expected 1 table, got %d", len(schema.Tables))
	}

	table := schema.Tables[0]
	if table.Name != "users" {
		t.Errorf("Expected table name 'users', got %q", table.Name)
	}

	if len(table.Columns) != 12 {
		t.Fatalf("Expected 12 columns, got %d", len(table.Columns))
	}

	// Verify a few key columns
	id := table.Columns[0]
	if id.Name != "id" || id.Type != "serial" || !id.IsPrimaryKey {
		t.Errorf("id column incorrect: name=%q, type=%q, pk=%v", id.Name, id.Type, id.IsPrimaryKey)
	}

	username := table.Columns[1]
	if username.Name != "username" || username.Type != "varchar(50)" || username.Nullable {
		t.Errorf("username column incorrect: name=%q, type=%q, nullable=%v", username.Name, username.Type, username.Nullable)
	}

	tags := table.Columns[8]
	if tags.Name != "tags" || tags.Type != "text[]" {
		t.Errorf("tags column incorrect: name=%q, type=%q", tags.Name, tags.Type)
	}

	createdAt := table.Columns[10]
	if createdAt.Name != "created_at" || createdAt.Nullable {
		t.Errorf("created_at column incorrect: name=%q, nullable=%v", createdAt.Name, createdAt.Nullable)
	}
	if createdAt.Default == nil || *createdAt.Default != "CURRENT_TIMESTAMP" {
		t.Errorf("created_at default incorrect: %v", createdAt.Default)
	}
}

func TestParseEnableRLS(t *testing.T) {
	sql := `
		CREATE TABLE users (id INTEGER);
		ALTER TABLE users ENABLE ROW LEVEL SECURITY;
	`

	schema, err := ParseSQLSchemaWithDialect(sql, database.DialectPostgres)
	if err != nil {
		t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
	}

	if len(schema.Tables) != 1 {
		t.Fatalf("Expected 1 table, got %d", len(schema.Tables))
	}

	table := schema.Tables[0]
	if !table.RLSEnabled {
		t.Error("Expected RLS to be enabled, but it was not")
	}
}

func TestParseDisableRLS(t *testing.T) {
	sql := `
		CREATE TABLE users (id INTEGER);
		ALTER TABLE users ENABLE ROW LEVEL SECURITY;
		ALTER TABLE users DISABLE ROW LEVEL SECURITY;
	`

	schema, err := ParseSQLSchemaWithDialect(sql, database.DialectPostgres)
	if err != nil {
		t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
	}

	if len(schema.Tables) != 1 {
		t.Fatalf("Expected 1 table, got %d", len(schema.Tables))
	}

	table := schema.Tables[0]
	if table.RLSEnabled {
		t.Error("Expected RLS to be disabled, but it was enabled")
	}
}

func TestParseRLSDefaultDisabled(t *testing.T) {
	sql := `CREATE TABLE users (id INTEGER);`

	schema, err := ParseSQLSchemaWithDialect(sql, database.DialectPostgres)
	if err != nil {
		t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
	}

	table := schema.Tables[0]
	if table.RLSEnabled {
		t.Error("Expected RLS to be disabled by default, but it was enabled")
	}
}

func TestParseRLSMultipleTables(t *testing.T) {
	sql := `
		CREATE TABLE users (id INTEGER);
		CREATE TABLE posts (id INTEGER);
		ALTER TABLE users ENABLE ROW LEVEL SECURITY;
	`

	schema, err := ParseSQLSchemaWithDialect(sql, database.DialectPostgres)
	if err != nil {
		t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
	}

	if len(schema.Tables) != 2 {
		t.Fatalf("Expected 2 tables, got %d", len(schema.Tables))
	}

	users := schema.Tables[0]
	if !users.RLSEnabled {
		t.Error("Expected users table to have RLS enabled")
	}

	posts := schema.Tables[1]
	if posts.RLSEnabled {
		t.Error("Expected posts table to have RLS disabled")
	}
}

func TestParseAlterTableDefaultsToPublicSchema(t *testing.T) {
	// Test that ALTER TABLE without schema qualifier defaults to public schema,
	// not the first matching table name
	sql := `
		CREATE TABLE auth.users (id INTEGER);
		CREATE TABLE public.users (id INTEGER);
		ALTER TABLE users ENABLE ROW LEVEL SECURITY;
	`

	schema, err := ParseSQLSchemaWithDialect(sql, database.DialectPostgres)
	if err != nil {
		t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
	}

	if len(schema.Tables) != 2 {
		t.Fatalf("Expected 2 tables, got %d", len(schema.Tables))
	}

	// Find auth.users and public.users
	var authUsers, publicUsers *database.Table
	for i := range schema.Tables {
		if schema.Tables[i].Name == "users" && schema.Tables[i].Schema == "auth" {
			authUsers = &schema.Tables[i]
		}
		if schema.Tables[i].Name == "users" && schema.Tables[i].Schema == "public" {
			publicUsers = &schema.Tables[i]
		}
	}

	if authUsers == nil {
		t.Fatal("Expected to find auth.users table")
	}
	if publicUsers == nil {
		t.Fatal("Expected to find public.users table")
	}

	// ALTER TABLE users (without schema) should only affect public.users
	if authUsers.RLSEnabled {
		t.Error("Expected auth.users to have RLS disabled (ALTER TABLE should not affect it)")
	}
	if !publicUsers.RLSEnabled {
		t.Error("Expected public.users to have RLS enabled (ALTER TABLE should affect it)")
	}
}

func TestParseAlterTableExplicitSchema(t *testing.T) {
	// Test that ALTER TABLE with explicit schema affects the correct table
	sql := `
		CREATE TABLE auth.users (id INTEGER);
		CREATE TABLE public.users (id INTEGER);
		ALTER TABLE auth.users ENABLE ROW LEVEL SECURITY;
	`

	schema, err := ParseSQLSchemaWithDialect(sql, database.DialectPostgres)
	if err != nil {
		t.Fatalf("ParseSQLSchemaWithDialect failed: %v", err)
	}

	if len(schema.Tables) != 2 {
		t.Fatalf("Expected 2 tables, got %d", len(schema.Tables))
	}

	// Find auth.users and public.users
	var authUsers, publicUsers *database.Table
	for i := range schema.Tables {
		if schema.Tables[i].Name == "users" && schema.Tables[i].Schema == "auth" {
			authUsers = &schema.Tables[i]
		}
		if schema.Tables[i].Name == "users" && schema.Tables[i].Schema == "public" {
			publicUsers = &schema.Tables[i]
		}
	}

	if authUsers == nil {
		t.Fatal("Expected to find auth.users table")
	}
	if publicUsers == nil {
		t.Fatal("Expected to find public.users table")
	}

	// ALTER TABLE auth.users should only affect auth.users
	if !authUsers.RLSEnabled {
		t.Error("Expected auth.users to have RLS enabled")
	}
	if publicUsers.RLSEnabled {
		t.Error("Expected public.users to have RLS disabled (ALTER TABLE should not affect it)")
	}
}
