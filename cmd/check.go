package cmd

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/lockplane/lockplane/internal/schema"
	"github.com/spf13/cobra"
)

var checkPrintSchema bool
var checkOutput string

func init() {
	rootCmd.AddCommand(checkCmd)
	checkCmd.Flags().BoolVar(&checkPrintSchema, "print-schema", false, "Print the parsed schema as JSON to stdout")
	checkCmd.Flags().StringVar(&checkOutput, "output", "", "Output format: 'json' for structured diagnostics")
}

var checkCmd = &cobra.Command{
	Use:   "check [schema dir or .lp.sql file]",
	Short: "Check .lp.sql schema files for errors",
	Long: `Check .lp.sql schema files for errors and print a JSON summary

When provided a directory, lockplane will check all .lp.sql files in the root
of that directory.

Examples:
lockplane check schema/
lockplane check my-schema.lp.sql
lockplane check my-schema.lp.sql > report.json
lockplane check --print-schema schema/  # Print parsed schema as JSON
`,
	Run: runCheck,
}

func runCheck(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		fmt.Printf(`Missing a schema file.

Usage: lockplane check [schema dir or .lp.sql file]
Help: lockplane check --help
`)
		os.Exit(1)
	}
	schemaPath := args[0]

	// Validate that conflicting flags aren't used together
	if checkPrintSchema && checkOutput == "json" {
		fmt.Fprintf(os.Stderr, "Error: --print-schema and --output json cannot be used together\n")
		fmt.Fprintf(os.Stderr, "  --print-schema: prints the parsed schema structure\n")
		fmt.Fprintf(os.Stderr, "  --output json: prints validation diagnostics\n")
		os.Exit(1)
	}

	// If --print-schema flag is set, load and print the schema as JSON
	if checkPrintSchema {
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

	// If --output json is set, return structured diagnostics
	if checkOutput == "json" {
		reportJson, err := schema.CheckSchema(schemaPath)
		if err != nil {
			log.Fatalf("Failed to check schema: %v", err)
		}
		fmt.Print(reportJson)
		return
	}

	// Default behavior: validate and print human-readable output
	_, err := schema.LoadSchema(schemaPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("✓ Schema is valid")
}
