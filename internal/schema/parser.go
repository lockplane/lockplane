package schema

import (
	"fmt"
	"strings"

	"github.com/lockplane/lockplane/internal/database"
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// ParseSQLSchemaWithDialect parses SQL DDL for the requested dialect.
func ParseSQLSchemaWithDialect(sql string, dialect database.Dialect) (*database.Schema, error) {
	switch dialect {
	case database.DialectPostgres:
		return parsePostgresSQLSchema(sql)
	default:
		return nil, fmt.Errorf("unsupported dialect %v", dialect)
	}
}

// parsePostgresSQLSchema parses SQL DDL via pg_query for PostgreSQL schemas.
func parsePostgresSQLSchema(sql string) (*database.Schema, error) {
	// Parse the SQL
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}

	schema := &database.Schema{
		Tables:  []database.Table{},
		Dialect: database.DialectPostgres,
	}

	// Walk the parse tree
	for _, stmt := range tree.Stmts {
		if stmt.Stmt == nil {
			continue
		}

		switch node := stmt.Stmt.Node.(type) {
		case *pg_query.Node_CreateStmt:
			table, err := parseCreateTable(node.CreateStmt)
			if err != nil {
				return nil, fmt.Errorf("failed to parse CREATE TABLE: %w", err)
			}
			schema.Tables = append(schema.Tables, *table)

		case *pg_query.Node_AlterTableStmt:
			// Handle ALTER TABLE for RLS and other commands
			err := parseAlterTable(schema, node.AlterTableStmt)
			if err != nil {
				return nil, fmt.Errorf("failed to parse ALTER TABLE: %w", err)
			}

			// 	case *pg_query.Node_IndexStmt:
			// 		// Handle CREATE INDEX separately (will add to existing table)
			// 		err := parseCreateIndex(schema, node.IndexStmt)
			// 		if err != nil {
			// 			return nil, fmt.Errorf("failed to parse CREATE INDEX: %w", err)
			// 		}
		}
	}

	return schema, nil
}

// parseCreateTable converts a CreateStmt AST node to a Table
func parseCreateTable(stmt *pg_query.CreateStmt) (*database.Table, error) {
	if stmt.Relation == nil {
		return nil, fmt.Errorf("CREATE TABLE missing relation")
	}

	table := &database.Table{
		Name:    stmt.Relation.Relname,
		Schema:  stmt.Relation.Schemaname, // Extract schema name if specified
		Columns: []database.Column{},
		// Indexes:     []database.Index{},
		// ForeignKeys: []database.ForeignKey{},
	}

	// Parse columns and constraints
	for _, elt := range stmt.TableElts {
		if elt.Node == nil {
			continue
		}

		switch node := elt.Node.(type) {
		case *pg_query.Node_ColumnDef:
			col, err := parseColumnDef(node.ColumnDef)
			if err != nil {
				return nil, err
			}
			table.Columns = append(table.Columns, *col)

			// case *pg_query.Node_Constraint:
			// 	err := parseTableConstraint(table, node.Constraint)
			// 	if err != nil {
			// 		return nil, err
			// 	}
		}
	}

	return table, nil
}

// parseColumnDef converts a ColumnDef AST node to a Column
func parseColumnDef(colDef *pg_query.ColumnDef) (*database.Column, error) {
	if colDef.Colname == "" {
		return nil, fmt.Errorf("column missing name")
	}

	col := &database.Column{
		Name:         colDef.Colname,
		Nullable:     true, // Default to nullable unless NOT NULL is specified
		IsPrimaryKey: false,
	}

	// Parse type
	if colDef.TypeName != nil {
		colType := formatTypeName(colDef.TypeName)
		col.Type = colType
	}

	// Parse constraints (NOT NULL, DEFAULT, PRIMARY KEY, etc.)
	for _, constraint := range colDef.Constraints {
		if constraint.Node == nil {
			continue
		}

		if cons, ok := constraint.Node.(*pg_query.Node_Constraint); ok {
			parseColumnConstraint(col, cons.Constraint)
		}
	}

	return col, nil
}

// formatTypeName converts TypeName AST to a string representation with metadata.
func formatTypeName(typeName *pg_query.TypeName) string {
	if len(typeName.Names) == 0 {
		return ""
	}

	// Get the type name (last element in Names)
	var parts []string
	for _, name := range typeName.Names {
		if nameNode, ok := name.Node.(*pg_query.Node_String_); ok {
			parts = append(parts, nameNode.String_.Sval)
		}
	}

	rawBase := strings.Join(parts, ".")
	typeStr := rawBase

	if len(parts) > 1 && parts[0] == "pg_catalog" {
		typeStr = parts[len(parts)-1]
	}

	// Normalize PostgreSQL internal types to standard SQL types
	typeStr = normalizePostgreSQLType(typeStr)

	// Add type modifiers (e.g., VARCHAR(255))
	if len(typeName.Typmods) > 0 {
		var mods []string
		for _, mod := range typeName.Typmods {
			if constNode, ok := mod.Node.(*pg_query.Node_AConst); ok {
				if ival := constNode.AConst.GetIval(); ival != nil {
					mods = append(mods, fmt.Sprintf("%d", ival.Ival))
				}
			}
		}
		if len(mods) > 0 {
			modStr := strings.Join(mods, ",")
			typeStr = fmt.Sprintf("%s(%s)", typeStr, modStr)
		}
	}

	// Add array notation if needed
	if len(typeName.ArrayBounds) > 0 {
		typeStr += "[]"
	}

	return typeStr
}

var typeMap = map[string]string{
	// Integer types
	"int2":    "smallint",
	"int4":    "integer",
	"int8":    "bigint",
	"serial":  "serial",
	"serial2": "smallserial",
	"serial4": "serial",
	"serial8": "bigserial",

	// Boolean
	"bool": "boolean",

	// Character types
	"varchar": "varchar",
	"bpchar":  "char",

	// Floating point
	"float4": "real",
	"float8": "double precision",

	// Timestamp types
	"timestamp":   "timestamp without time zone",
	"timestamptz": "timestamp with time zone",
	"time":        "time without time zone",
	"timetz":      "time with time zone",

	// Text (keep as-is, but explicitly map)
	"text": "text",

	// Numeric
	"numeric": "numeric",
	"decimal": "decimal",
}

// normalizePostgreSQLType converts PostgreSQL internal type names to standard SQL types
// This is necessary because we use pg_query (PostgreSQL parser) for all SQL parsing,
// and it normalizes types to PostgreSQL internal names like "int4", "int8", "bool", etc.
func normalizePostgreSQLType(pgType string) string {
	// Map PostgreSQL internal types to standard SQL types
	if normalized, ok := typeMap[strings.ToLower(pgType)]; ok {
		return normalized
	}

	return pgType
}

// parseColumnConstraint applies a column-level constraint to a Column
func parseColumnConstraint(col *database.Column, constraint *pg_query.Constraint) {
	switch constraint.Contype {
	case pg_query.ConstrType_CONSTR_NOTNULL:
		col.Nullable = false

	case pg_query.ConstrType_CONSTR_NULL:
		col.Nullable = true

	case pg_query.ConstrType_CONSTR_DEFAULT:
		if constraint.RawExpr != nil {
			// Format the default expression
			defaultStr := formatExpr(constraint.RawExpr)
			col.Default = &defaultStr
		}

	case pg_query.ConstrType_CONSTR_PRIMARY:
		col.IsPrimaryKey = true
		col.Nullable = false // PRIMARY KEY implies NOT NULL
	}
}

// formatExpr converts an expression AST to string
func formatExpr(node *pg_query.Node) string {
	if node == nil {
		return ""
	}

	switch expr := node.Node.(type) {
	case *pg_query.Node_AConst:
		// Check different types of constants
		if ival := expr.AConst.GetIval(); ival != nil {
			return fmt.Sprintf("%d", ival.Ival)
		}
		if fval := expr.AConst.GetFval(); fval != nil {
			return fval.Fval
		}
		if sval := expr.AConst.GetSval(); sval != nil {
			return fmt.Sprintf("'%s'", sval.Sval)
		}
		if bsval := expr.AConst.GetBsval(); bsval != nil {
			return bsval.Bsval
		}
		if boolval := expr.AConst.GetBoolval(); boolval != nil {
			return fmt.Sprintf("%t", boolval.Boolval)
		}
		if nullval := expr.AConst.GetIsnull(); nullval {
			return "NULL"
		}

	case *pg_query.Node_FuncCall:
		// Handle function calls like NOW(), CURRENT_TIMESTAMP, datetime('now'), etc.
		if len(expr.FuncCall.Funcname) > 0 {
			if nameNode, ok := expr.FuncCall.Funcname[0].Node.(*pg_query.Node_String_); ok {
				funcName := nameNode.String_.Sval

				// Format arguments
				var args []string
				for _, argNode := range expr.FuncCall.Args {
					argStr := formatExpr(argNode)
					args = append(args, argStr)
				}

				if len(args) > 0 {
					return fmt.Sprintf("%s(%s)", funcName, strings.Join(args, ", "))
				}
				return funcName + "()"
			}
		}

	case *pg_query.Node_TypeCast:
		// Handle type casts
		if expr.TypeCast.Arg != nil {
			return formatExpr(expr.TypeCast.Arg)
		}

	case *pg_query.Node_SqlvalueFunction:
		// Handle SQL value functions like CURRENT_TIMESTAMP, CURRENT_USER, etc.
		// Based on PostgreSQL's SVFOp enum (1-indexed)
		// See: https://github.com/postgres/postgres/blob/master/src/include/nodes/primnodes.h
		switch expr.SqlvalueFunction.Op {
		case 1: // SVFOP_CURRENT_DATE
			return "CURRENT_DATE"
		case 2: // SVFOP_CURRENT_TIME
			return "CURRENT_TIME"
		case 3: // SVFOP_CURRENT_TIME_N (CURRENT_TIME with precision)
			return "CURRENT_TIME"
		case 4: // SVFOP_CURRENT_TIMESTAMP
			return "CURRENT_TIMESTAMP"
		case 5: // SVFOP_CURRENT_TIMESTAMP_N (CURRENT_TIMESTAMP with precision)
			return "CURRENT_TIMESTAMP"
		case 6: // SVFOP_LOCALTIME
			return "LOCALTIME"
		case 7: // SVFOP_LOCALTIME_N (LOCALTIME with precision)
			return "LOCALTIME"
		case 8: // SVFOP_LOCALTIMESTAMP
			return "LOCALTIMESTAMP"
		case 9: // SVFOP_LOCALTIMESTAMP_N (LOCALTIMESTAMP with precision)
			return "LOCALTIMESTAMP"
		case 10: // SVFOP_CURRENT_ROLE
			return "CURRENT_ROLE"
		case 11: // SVFOP_CURRENT_USER
			return "CURRENT_USER"
		case 12: // SVFOP_USER
			return "USER"
		case 13: // SVFOP_SESSION_USER
			return "SESSION_USER"
		case 14: // SVFOP_CURRENT_CATALOG
			return "CURRENT_CATALOG"
		case 15: // SVFOP_CURRENT_SCHEMA
			return "CURRENT_SCHEMA"
		}
	}

	// For anything else, return a placeholder
	return "UNDEFINED_EXPRESSION"
}

// parseAlterTable handles ALTER TABLE statements, currently focusing on RLS
func parseAlterTable(schema *database.Schema, stmt *pg_query.AlterTableStmt) error {
	if stmt.Relation == nil {
		return fmt.Errorf("ALTER TABLE missing relation")
	}

	tableName := stmt.Relation.Relname
	tableSchema := stmt.Relation.Schemaname

	// If no schema specified, default to "public" (matches CREATE TABLE behavior)
	if tableSchema == "" {
		tableSchema = "public"
	}

	// Find the table in the schema (match by both schema and name)
	var tableIndex = -1
	for i, table := range schema.Tables {
		// Match by (schema, name) - treating empty schema as "public"
		tblSchema := table.Schema
		if tblSchema == "" {
			tblSchema = "public"
		}

		if table.Name == tableName && tblSchema == tableSchema {
			tableIndex = i
			break
		}
	}

	// If table doesn't exist yet, we can't apply ALTER TABLE to it
	if tableIndex == -1 {
		// This is OK - ALTER TABLE might come after CREATE TABLE in the same schema
		// or might reference a table that already exists in the database
		// For now, we'll skip it
		return nil
	}

	// Process each command in the ALTER TABLE statement
	for _, cmd := range stmt.Cmds {
		if cmd.Node == nil {
			continue
		}

		if alterCmd, ok := cmd.Node.(*pg_query.Node_AlterTableCmd); ok {
			switch alterCmd.AlterTableCmd.Subtype {
			case pg_query.AlterTableType_AT_EnableRowSecurity:
				schema.Tables[tableIndex].RLSEnabled = true
			case pg_query.AlterTableType_AT_DisableRowSecurity:
				schema.Tables[tableIndex].RLSEnabled = false
			}
		}
	}

	return nil
}
