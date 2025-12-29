package cmd

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/lockplane/lockplane/internal/schema"
	"github.com/spf13/cobra"
)

var checkSchemaPrintSchema bool

func init() {
	rootCmd.AddCommand(checkSchemaCmd)
	checkSchemaCmd.Flags().BoolVar(&checkSchemaPrintSchema, "print-schema", false, "Print the parsed schema as JSON to stdout")
}

var checkSchemaCmd = &cobra.Command{
	Use:   "check-schema [schema dir or .lp.sql file]",
	Short: "Check .lp.sql schema files for errors",
	Long: `Check .lp.sql schema files for errors and print a JSON summary

When provided a directory, lockplane will check all .lp.sql files in the root
of that directory.

Examples:
lockplane check-schema schema/
lockplane check-schema my-schema.lp.sql
lockplane check-schema my-schema.lp.sql > report.json
lockplane check-schema --print-schema schema/  # Print parsed schema as JSON
`,
	Run: runCheckSchema,
}

func runCheckSchema(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		fmt.Printf(`Missing a schema file.

Usage: lockplane check-schema [schema dir or .lp.sql file]
Help: lockplane check-schema --help
`)
		os.Exit(1)
	}
	schemaPath := args[0]

	// If --print-schema flag is set, load and print the schema as JSON
	if checkSchemaPrintSchema {
		loadedSchema, err := schema.LoadSchema(schemaPath)
		if err != nil {
			log.Fatalf("Failed to load schema: %v", err)
		}

		schemaJson, err := json.MarshalIndent(loadedSchema, "", "  ")
		if err != nil {
			log.Fatalf("Failed to marshal schema to JSON: %v", err)
		}

		fmt.Println(string(schemaJson))
		return
	}

	// Normal check-schema behavior
	reportJson, err := schema.CheckSchema(schemaPath)
	if err != nil {
		log.Fatalf("Failed to check schema: %v", err)
	}
	fmt.Print(reportJson)
}
