package schema

import (
	"testing"

	"github.com/lockplane/lockplane/internal/database"
)

// Helper function to create a pointer to a string
func strPtr(s string) *string {
	return &s
}

func TestDiffSchemas_AddedTables(t *testing.T) {
	current := &database.Schema{
		Tables: []database.Table{},
	}

	desired := &database.Schema{
		Tables: []database.Table{
			{
				Name:   "users",
				Schema: "public",
				Columns: []database.Column{
					{Name: "id", Type: "integer"},
					{Name: "name", Type: "text"},
				},
			},
		},
	}

	diff := DiffSchemas(current, desired)

	if len(diff.AddedTables) != 1 {
		t.Fatalf("Expected 1 added table, got %d", len(diff.AddedTables))
	}

	if diff.AddedTables[0].Name != "users" {
		t.Errorf("Expected added table 'users', got %q", diff.AddedTables[0].Name)
	}

	if len(diff.RemovedTables) != 0 {
		t.Errorf("Expected no removed tables, got %d", len(diff.RemovedTables))
	}

	if len(diff.ModifiedTables) != 0 {
		t.Errorf("Expected no modified tables, got %d", len(diff.ModifiedTables))
	}
}

func TestDiffSchemas_RemovedTables(t *testing.T) {
	current := &database.Schema{
		Tables: []database.Table{
			{
				Name:   "users",
				Schema: "public",
				Columns: []database.Column{
					{Name: "id", Type: "integer"},
				},
			},
			{
				Name:   "posts",
				Schema: "public",
				Columns: []database.Column{
					{Name: "id", Type: "integer"},
				},
			},
		},
	}

	desired := &database.Schema{
		Tables: []database.Table{
			{
				Name:   "users",
				Schema: "public",
				Columns: []database.Column{
					{Name: "id", Type: "integer"},
				},
			},
		},
	}

	diff := DiffSchemas(current, desired)

	if len(diff.RemovedTables) != 1 {
		t.Fatalf("Expected 1 removed table, got %d", len(diff.RemovedTables))
	}

	if diff.RemovedTables[0].Name != "posts" {
		t.Errorf("Expected removed table 'posts', got %q", diff.RemovedTables[0].Name)
	}

	if len(diff.AddedTables) != 0 {
		t.Errorf("Expected no added tables, got %d", len(diff.AddedTables))
	}

	if len(diff.ModifiedTables) != 0 {
		t.Errorf("Expected no modified tables, got %d", len(diff.ModifiedTables))
	}
}

func TestDiffSchemas_NoChanges(t *testing.T) {
	schema := &database.Schema{
		Tables: []database.Table{
			{
				Name:   "users",
				Schema: "public",
				Columns: []database.Column{
					{Name: "id", Type: "integer", Nullable: false, IsPrimaryKey: true},
					{Name: "name", Type: "text", Nullable: true},
				},
			},
		},
	}

	diff := DiffSchemas(schema, schema)

	if !diff.IsEmpty() {
		t.Error("Expected no differences for identical schemas")
	}

	if len(diff.AddedTables) != 0 {
		t.Errorf("Expected no added tables, got %d", len(diff.AddedTables))
	}

	if len(diff.RemovedTables) != 0 {
		t.Errorf("Expected no removed tables, got %d", len(diff.RemovedTables))
	}

	if len(diff.ModifiedTables) != 0 {
		t.Errorf("Expected no modified tables, got %d", len(diff.ModifiedTables))
	}
}

func TestDiffSchemas_EmptySchemas(t *testing.T) {
	current := &database.Schema{Tables: []database.Table{}}
	desired := &database.Schema{Tables: []database.Table{}}

	diff := DiffSchemas(current, desired)

	if !diff.IsEmpty() {
		t.Error("Expected no differences for empty schemas")
	}
}

func TestDiffTables_AddedColumns(t *testing.T) {
	current := &database.Table{
		Name: "users",
		Columns: []database.Column{
			{Name: "id", Type: "integer"},
		},
	}

	desired := &database.Table{
		Name: "users",
		Columns: []database.Column{
			{Name: "id", Type: "integer"},
			{Name: "email", Type: "text"},
			{Name: "age", Type: "integer"},
		},
	}

	diff := diffTables(current, desired)

	if len(diff.AddedColumns) != 2 {
		t.Fatalf("Expected 2 added columns, got %d", len(diff.AddedColumns))
	}

	// Check that email and age were added (order may vary)
	addedNames := make(map[string]bool)
	for _, col := range diff.AddedColumns {
		addedNames[col.Name] = true
	}

	if !addedNames["email"] {
		t.Error("Expected 'email' column to be added")
	}
	if !addedNames["age"] {
		t.Error("Expected 'age' column to be added")
	}
}

func TestDiffTables_RemovedColumns(t *testing.T) {
	current := &database.Table{
		Name: "users",
		Columns: []database.Column{
			{Name: "id", Type: "integer"},
			{Name: "deprecated_field", Type: "text"},
		},
	}

	desired := &database.Table{
		Name: "users",
		Columns: []database.Column{
			{Name: "id", Type: "integer"},
		},
	}

	diff := diffTables(current, desired)

	if len(diff.RemovedColumns) != 1 {
		t.Fatalf("Expected 1 removed column, got %d", len(diff.RemovedColumns))
	}

	if diff.RemovedColumns[0].Name != "deprecated_field" {
		t.Errorf("Expected removed column 'deprecated_field', got %q", diff.RemovedColumns[0].Name)
	}
}

func TestDiffTables_ModifiedColumns(t *testing.T) {
	current := &database.Table{
		Name: "users",
		Columns: []database.Column{
			{Name: "age", Type: "integer", Nullable: true},
		},
	}

	desired := &database.Table{
		Name: "users",
		Columns: []database.Column{
			{Name: "age", Type: "bigint", Nullable: false},
		},
	}

	diff := diffTables(current, desired)

	if len(diff.ModifiedColumns) != 1 {
		t.Fatalf("Expected 1 modified column, got %d", len(diff.ModifiedColumns))
	}

	colDiff := diff.ModifiedColumns[0]
	if colDiff.ColumnName != "age" {
		t.Errorf("Expected modified column 'age', got %q", colDiff.ColumnName)
	}

	if len(colDiff.Changes) != 2 {
		t.Fatalf("Expected 2 changes, got %d", len(colDiff.Changes))
	}

	// Check that both type and nullable changed
	changes := make(map[string]bool)
	for _, change := range colDiff.Changes {
		changes[change] = true
	}

	if !changes["type"] {
		t.Error("Expected 'type' to be in changes")
	}
	if !changes["nullable"] {
		t.Error("Expected 'nullable' to be in changes")
	}
}

func TestDiffColumns_TypeChange(t *testing.T) {
	current := &database.Column{
		Name: "age",
		Type: "integer",
	}

	desired := &database.Column{
		Name: "age",
		Type: "bigint",
	}

	diff := diffColumns(current, desired)

	if diff == nil {
		t.Fatal("Expected diff, got nil")
	}

	if len(diff.Changes) != 1 {
		t.Fatalf("Expected 1 change, got %d", len(diff.Changes))
	}

	if diff.Changes[0] != "type" {
		t.Errorf("Expected change 'type', got %q", diff.Changes[0])
	}
}

func TestDiffColumns_NullableChange(t *testing.T) {
	current := &database.Column{
		Name:     "email",
		Type:     "text",
		Nullable: true,
	}

	desired := &database.Column{
		Name:     "email",
		Type:     "text",
		Nullable: false,
	}

	diff := diffColumns(current, desired)

	if diff == nil {
		t.Fatal("Expected diff, got nil")
	}

	if len(diff.Changes) != 1 {
		t.Fatalf("Expected 1 change, got %d", len(diff.Changes))
	}

	if diff.Changes[0] != "nullable" {
		t.Errorf("Expected change 'nullable', got %q", diff.Changes[0])
	}
}

func TestDiffColumns_DefaultChange(t *testing.T) {
	tests := []struct {
		name           string
		currentDefault *string
		desiredDefault *string
		expectChange   bool
	}{
		{
			name:           "nil to value",
			currentDefault: nil,
			desiredDefault: strPtr("0"),
			expectChange:   true,
		},
		{
			name:           "value to nil",
			currentDefault: strPtr("0"),
			desiredDefault: nil,
			expectChange:   true,
		},
		{
			name:           "value to different value",
			currentDefault: strPtr("0"),
			desiredDefault: strPtr("42"),
			expectChange:   true,
		},
		{
			name:           "same value",
			currentDefault: strPtr("0"),
			desiredDefault: strPtr("0"),
			expectChange:   false,
		},
		{
			name:           "both nil",
			currentDefault: nil,
			desiredDefault: nil,
			expectChange:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			current := &database.Column{
				Name:    "count",
				Type:    "integer",
				Default: tt.currentDefault,
			}

			desired := &database.Column{
				Name:    "count",
				Type:    "integer",
				Default: tt.desiredDefault,
			}

			diff := diffColumns(current, desired)

			if tt.expectChange {
				if diff == nil {
					t.Fatal("Expected diff, got nil")
				}
				if len(diff.Changes) != 1 {
					t.Fatalf("Expected 1 change, got %d", len(diff.Changes))
				}
				if diff.Changes[0] != "default" {
					t.Errorf("Expected change 'default', got %q", diff.Changes[0])
				}
			} else {
				if diff != nil {
					t.Errorf("Expected no diff, got %+v", diff)
				}
			}
		})
	}
}

func TestDiffColumns_PrimaryKeyChange(t *testing.T) {
	current := &database.Column{
		Name:         "id",
		Type:         "integer",
		IsPrimaryKey: false,
	}

	desired := &database.Column{
		Name:         "id",
		Type:         "integer",
		IsPrimaryKey: true,
	}

	diff := diffColumns(current, desired)

	if diff == nil {
		t.Fatal("Expected diff, got nil")
	}

	if len(diff.Changes) != 1 {
		t.Fatalf("Expected 1 change, got %d", len(diff.Changes))
	}

	if diff.Changes[0] != "is_primary_key" {
		t.Errorf("Expected change 'is_primary_key', got %q", diff.Changes[0])
	}
}

func TestDiffColumns_MultipleChanges(t *testing.T) {
	current := &database.Column{
		Name:         "id",
		Type:         "integer",
		Nullable:     true,
		Default:      strPtr("0"),
		IsPrimaryKey: false,
	}

	desired := &database.Column{
		Name:         "id",
		Type:         "bigint",
		Nullable:     false,
		Default:      strPtr("1"),
		IsPrimaryKey: true,
	}

	diff := diffColumns(current, desired)

	if diff == nil {
		t.Fatal("Expected diff, got nil")
	}

	if len(diff.Changes) != 4 {
		t.Fatalf("Expected 4 changes, got %d", len(diff.Changes))
	}

	// Check all expected changes are present
	changes := make(map[string]bool)
	for _, change := range diff.Changes {
		changes[change] = true
	}

	expectedChanges := []string{"type", "nullable", "default", "is_primary_key"}
	for _, expected := range expectedChanges {
		if !changes[expected] {
			t.Errorf("Expected change %q to be present", expected)
		}
	}
}

func TestDiffColumns_NoChanges(t *testing.T) {
	column := &database.Column{
		Name:         "id",
		Type:         "integer",
		Nullable:     false,
		Default:      strPtr("0"),
		IsPrimaryKey: true,
	}

	diff := diffColumns(column, column)

	if diff != nil {
		t.Errorf("Expected no diff for identical columns, got %+v", diff)
	}
}

func TestEqualDefaults(t *testing.T) {
	tests := []struct {
		name     string
		a        *string
		b        *string
		expected bool
	}{
		{
			name:     "both nil",
			a:        nil,
			b:        nil,
			expected: true,
		},
		{
			name:     "first nil",
			a:        nil,
			b:        strPtr("value"),
			expected: false,
		},
		{
			name:     "second nil",
			a:        strPtr("value"),
			b:        nil,
			expected: false,
		},
		{
			name:     "same value",
			a:        strPtr("value"),
			b:        strPtr("value"),
			expected: true,
		},
		{
			name:     "different values",
			a:        strPtr("value1"),
			b:        strPtr("value2"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := equalDefaults(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestTableDiff_IsEmpty(t *testing.T) {
	tests := []struct {
		name     string
		diff     *TableDiff
		expected bool
	}{
		{
			name: "empty diff",
			diff: &TableDiff{
				TableName:       "users",
				AddedColumns:    []database.Column{},
				RemovedColumns:  []database.Column{},
				ModifiedColumns: []ColumnDiff{},
			},
			expected: true,
		},
		{
			name: "with added column",
			diff: &TableDiff{
				TableName: "users",
				AddedColumns: []database.Column{
					{Name: "email", Type: "text"},
				},
			},
			expected: false,
		},
		{
			name: "with removed column",
			diff: &TableDiff{
				TableName: "users",
				RemovedColumns: []database.Column{
					{Name: "deprecated", Type: "text"},
				},
			},
			expected: false,
		},
		{
			name: "with modified column",
			diff: &TableDiff{
				TableName: "users",
				ModifiedColumns: []ColumnDiff{
					{ColumnName: "age", Changes: []string{"type"}},
				},
			},
			expected: false,
		},
		{
			name: "with RLS change only",
			diff: &TableDiff{
				TableName:  "users",
				RLSChanged: true,
				RLSEnabled: true,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.diff.IsEmpty()
			if result != tt.expected {
				t.Errorf("Expected IsEmpty() = %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestSchemaDiff_IsEmpty(t *testing.T) {
	tests := []struct {
		name     string
		diff     *SchemaDiff
		expected bool
	}{
		{
			name: "empty diff",
			diff: &SchemaDiff{
				AddedTables:    []database.Table{},
				RemovedTables:  []database.Table{},
				ModifiedTables: []TableDiff{},
			},
			expected: true,
		},
		{
			name: "with added table",
			diff: &SchemaDiff{
				AddedTables: []database.Table{
					{Name: "users"},
				},
			},
			expected: false,
		},
		{
			name: "with removed table",
			diff: &SchemaDiff{
				RemovedTables: []database.Table{
					{Name: "users"},
				},
			},
			expected: false,
		},
		{
			name: "with modified table",
			diff: &SchemaDiff{
				ModifiedTables: []TableDiff{
					{TableName: "users"},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.diff.IsEmpty()
			if result != tt.expected {
				t.Errorf("Expected IsEmpty() = %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestDiffSchemas_ComplexScenario(t *testing.T) {
	current := &database.Schema{
		Tables: []database.Table{
			{
				Name:   "users",
				Schema: "public",
				Columns: []database.Column{
					{Name: "id", Type: "integer", Nullable: false, IsPrimaryKey: true},
					{Name: "name", Type: "varchar", Nullable: false},
					{Name: "age", Type: "integer", Nullable: true},
					{Name: "deprecated", Type: "text", Nullable: true},
				},
			},
			{
				Name:   "posts",
				Schema: "public",
				Columns: []database.Column{
					{Name: "id", Type: "integer"},
					{Name: "title", Type: "text"},
				},
			},
			{
				Name:   "comments",
				Schema: "public",
				Columns: []database.Column{
					{Name: "id", Type: "integer"},
				},
			},
		},
	}

	desired := &database.Schema{
		Tables: []database.Table{
			{
				Name:   "users",
				Schema: "public",
				Columns: []database.Column{
					{Name: "id", Type: "integer", Nullable: false, IsPrimaryKey: true},
					{Name: "name", Type: "text", Nullable: false},   // type changed
					{Name: "age", Type: "integer", Nullable: false}, // nullable changed
					{Name: "email", Type: "text", Nullable: false},  // added
					// deprecated removed
				},
			},
			{
				Name:   "posts",
				Schema: "public",
				Columns: []database.Column{
					{Name: "id", Type: "integer"},
					{Name: "title", Type: "text"},
				},
			},
			// comments removed
			{
				Name:   "tags",
				Schema: "public",
				Columns: []database.Column{
					{Name: "id", Type: "integer"},
					{Name: "name", Type: "text"},
				},
			},
		},
	}

	diff := DiffSchemas(current, desired)

	// Check added tables
	if len(diff.AddedTables) != 1 {
		t.Errorf("Expected 1 added table, got %d", len(diff.AddedTables))
	} else if diff.AddedTables[0].Name != "tags" {
		t.Errorf("Expected added table 'tags', got %q", diff.AddedTables[0].Name)
	}

	// Check removed tables
	if len(diff.RemovedTables) != 1 {
		t.Errorf("Expected 1 removed table, got %d", len(diff.RemovedTables))
	} else if diff.RemovedTables[0].Name != "comments" {
		t.Errorf("Expected removed table 'comments', got %q", diff.RemovedTables[0].Name)
	}

	// Check modified tables
	if len(diff.ModifiedTables) != 1 {
		t.Fatalf("Expected 1 modified table, got %d", len(diff.ModifiedTables))
	}

	usersDiff := diff.ModifiedTables[0]
	if usersDiff.TableName != "users" {
		t.Errorf("Expected modified table 'users', got %q", usersDiff.TableName)
	}

	// Check users table changes
	if len(usersDiff.AddedColumns) != 1 {
		t.Errorf("Expected 1 added column in users, got %d", len(usersDiff.AddedColumns))
	} else if usersDiff.AddedColumns[0].Name != "email" {
		t.Errorf("Expected added column 'email', got %q", usersDiff.AddedColumns[0].Name)
	}

	if len(usersDiff.RemovedColumns) != 1 {
		t.Errorf("Expected 1 removed column in users, got %d", len(usersDiff.RemovedColumns))
	} else if usersDiff.RemovedColumns[0].Name != "deprecated" {
		t.Errorf("Expected removed column 'deprecated', got %q", usersDiff.RemovedColumns[0].Name)
	}

	if len(usersDiff.ModifiedColumns) != 2 {
		t.Fatalf("Expected 2 modified columns in users, got %d", len(usersDiff.ModifiedColumns))
	}

	// Verify name and age columns were modified
	modifiedNames := make(map[string][]string)
	for _, colDiff := range usersDiff.ModifiedColumns {
		modifiedNames[colDiff.ColumnName] = colDiff.Changes
	}

	if changes, ok := modifiedNames["name"]; !ok {
		t.Error("Expected 'name' column to be modified")
	} else if len(changes) != 1 || changes[0] != "type" {
		t.Errorf("Expected 'name' to have type change, got %v", changes)
	}

	if changes, ok := modifiedNames["age"]; !ok {
		t.Error("Expected 'age' column to be modified")
	} else if len(changes) != 1 || changes[0] != "nullable" {
		t.Errorf("Expected 'age' to have nullable change, got %v", changes)
	}
}
func TestDiffTables_RLSEnabled(t *testing.T) {
	current := &database.Table{
		Name:       "users",
		RLSEnabled: false,
		Columns: []database.Column{
			{Name: "id", Type: "integer"},
		},
	}

	desired := &database.Table{
		Name:       "users",
		RLSEnabled: true,
		Columns: []database.Column{
			{Name: "id", Type: "integer"},
		},
	}

	diff := diffTables(current, desired)

	if !diff.RLSChanged {
		t.Error("Expected RLSChanged to be true")
	}
	if !diff.RLSEnabled {
		t.Error("Expected RLSEnabled to be true")
	}
}

func TestDiffTables_RLSDisabled(t *testing.T) {
	current := &database.Table{
		Name:       "users",
		RLSEnabled: true,
		Columns: []database.Column{
			{Name: "id", Type: "integer"},
		},
	}

	desired := &database.Table{
		Name:       "users",
		RLSEnabled: false,
		Columns: []database.Column{
			{Name: "id", Type: "integer"},
		},
	}

	diff := diffTables(current, desired)

	if !diff.RLSChanged {
		t.Error("Expected RLSChanged to be true")
	}
	if diff.RLSEnabled {
		t.Error("Expected RLSEnabled to be false")
	}
}

func TestDiffTables_RLSUnchanged(t *testing.T) {
	current := &database.Table{
		Name:       "users",
		RLSEnabled: true,
		Columns: []database.Column{
			{Name: "id", Type: "integer"},
		},
	}

	desired := &database.Table{
		Name:       "users",
		RLSEnabled: true,
		Columns: []database.Column{
			{Name: "id", Type: "integer"},
		},
	}

	diff := diffTables(current, desired)

	if diff.RLSChanged {
		t.Error("Expected RLSChanged to be false when RLS status is unchanged")
	}
}

func TestDiffSchemas_RLSOnlyChange(t *testing.T) {
	// Test that a schema diff with only RLS changes is correctly detected
	current := &database.Schema{
		Tables: []database.Table{
			{
				Name:       "users",
				RLSEnabled: false,
				Columns: []database.Column{
					{Name: "id", Type: "integer"},
				},
			},
		},
	}

	desired := &database.Schema{
		Tables: []database.Table{
			{
				Name:       "users",
				RLSEnabled: true,
				Columns: []database.Column{
					{Name: "id", Type: "integer"},
				},
			},
		},
	}

	diff := DiffSchemas(current, desired)

	if diff.IsEmpty() {
		t.Error("Expected diff to NOT be empty when RLS changes")
	}

	if len(diff.ModifiedTables) != 1 {
		t.Fatalf("Expected 1 modified table, got %d", len(diff.ModifiedTables))
	}

	tableDiff := diff.ModifiedTables[0]
	if !tableDiff.RLSChanged {
		t.Error("Expected RLSChanged to be true")
	}
	if !tableDiff.RLSEnabled {
		t.Error("Expected RLSEnabled to be true")
	}
	if tableDiff.IsEmpty() {
		t.Error("Expected TableDiff to NOT be empty when RLS changes")
	}
}
