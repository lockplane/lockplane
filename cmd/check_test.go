package cmd

import (
	"os"
	"os/exec"
	"testing"
)

func TestCheckConflictingFlags(t *testing.T) {
	// Check if we're being run as a subprocess
	if os.Getenv("TEST_CHECK_CONFLICT") == "1" {
		// This will run in the subprocess
		tmpFile := os.Getenv("TEST_TMPFILE")
		if tmpFile == "" {
			t.Fatal("TEST_TMPFILE not set")
		}

		// Try to run check with both --print-schema and --output json
		rootCmd.SetArgs([]string{"check", "--print-schema", "--output", "json", tmpFile})
		_ = rootCmd.Execute() // Error handling is done via os.Exit in runCheck
		return
	}

	// Main test process - spawn subprocess
	tmpFile, err := os.CreateTemp("", "test-*.lp.sql")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer func() {
		_ = os.Remove(tmpFile.Name())
	}()

	// Write valid SQL to the temp file
	if _, err := tmpFile.WriteString("CREATE TABLE users (id BIGINT PRIMARY KEY);"); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	if err := tmpFile.Close(); err != nil {
		t.Fatalf("Failed to close temp file: %v", err)
	}

	args := []string{"-test.run=TestCheckConflictingFlags"}
	if coverDir := os.Getenv("GOCOVERDIR"); coverDir != "" {
		args = append(args, "-test.gocoverdir="+coverDir)
	}

	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = append(os.Environ(), "TEST_CHECK_CONFLICT=1", "TEST_TMPFILE="+tmpFile.Name())

	err = cmd.Run()

	// We expect an error (exit code 1) because of conflicting flags
	if err == nil {
		t.Fatal("Expected command to fail with conflicting flags, but it succeeded")
	}

	// Check that it's an exit error
	if exitErr, ok := err.(*exec.ExitError); ok {
		if exitErr.ExitCode() != 1 {
			t.Errorf("Expected exit code 1, got %d", exitErr.ExitCode())
		}
	} else {
		t.Errorf("Expected *exec.ExitError, got %T: %v", err, err)
	}
}
