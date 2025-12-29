import * as cp from "child_process";
import * as vscode from "vscode";

export interface CheckResult {
  file: string;
  line: number;
  column: number;
  severity: "error" | "warning" | "info";
  message: string;
  code?: string;
}

/**
 * Lockplane CLI diagnostic output structure (from check --output json)
 */
export interface LockplaneDiagnostic {
  code?: string;
  message: string;
  severity: "error" | "warning" | "info";
  // Optional location information if available
  file?: string;
  line?: number;
  column?: number;
}

export interface LockplaneSummary {
  errors: number;
  warnings?: number;
  valid: boolean;
}

export interface LockplaneCheckOutput {
  diagnostics?: LockplaneDiagnostic[];
  summary?: LockplaneSummary;
}

/**
 * Legacy interface (kept for backwards compatibility)
 */
export interface SQLCheckIssue {
  file: string;
  line: number;
  column: number;
  severity: "error" | "warning";
  message: string;
  code?: string;
}

export interface SQLCheckResult {
  valid: boolean;
  issues: SQLCheckIssue[] | null;
}

export async function checkSchema(schemaPath: string): Promise<CheckResult[]> {
  return new Promise((resolve, reject) => {
    const config = vscode.workspace.getConfiguration("lockplane");
    const lockplanePath = config.get<string>("cliPath", "lockplane");

    // Get workspace folder for cwd
    const workspaceFolders = vscode.workspace.workspaceFolders;
    const cwd = workspaceFolders ? workspaceFolders[0].uri.fsPath : undefined;

    const cmd = `${lockplanePath} check --output json "${schemaPath}"`;

    console.log(`[Lockplane] Running command: ${cmd}`);
    console.log(`[Lockplane] Working directory: ${cwd}`);

    cp.exec(
      cmd,
      { cwd, maxBuffer: 10 * 1024 * 1024 },
      (error, stdout, stderr) => {
        console.log(`[Lockplane] stdout:`, stdout);
        console.log(`[Lockplane] stderr:`, stderr);
        console.log(`[Lockplane] error:`, error);

        // Check if lockplane CLI is not found
        if (
          error &&
          (error.message.includes("not found") ||
            error.message.includes("ENOENT"))
        ) {
          reject(
            new Error(
              "Lockplane CLI not found. Make sure it is installed and in your PATH."
            )
          );
          return;
        }

        // Parse JSON output from check command
        try {
          const output = JSON.parse(stdout);
          console.log(`[Lockplane] Check output:`, output);

          // Try to parse as the new lockplane format (with diagnostics and summary)
          const lockplaneOutput = output as LockplaneCheckOutput;

          // Check if this is the expected format
          if (
            lockplaneOutput.summary !== undefined ||
            lockplaneOutput.diagnostics !== undefined
          ) {
            // New format detected
            const isValid = lockplaneOutput.summary?.valid ?? true;
            const diagnostics = lockplaneOutput.diagnostics ?? [];

            console.log(
              `[Lockplane] Schema is ${isValid ? "valid" : "invalid"}, found ${
                diagnostics.length
              } diagnostics`
            );

            if (isValid && diagnostics.length === 0) {
              // Schema is valid and no diagnostics
              console.log("[Lockplane] Schema is valid");
              resolve([]);
              return;
            }

            // Convert LockplaneDiagnostic[] to CheckResult[]
            const results: CheckResult[] = diagnostics.map((diagnostic) => ({
              // Use provided file/line/column if available, otherwise default to schema file
              file: diagnostic.file || schemaPath,
              line: diagnostic.line || 1,
              column: diagnostic.column || 1,
              severity: diagnostic.severity || "error",
              message: diagnostic.message,
              code: diagnostic.code,
            }));

            console.log(`[Lockplane] Parsed ${results.length} issues`);
            resolve(results);
            return;
          }

          // Unknown format
          console.error("[Lockplane] Unexpected JSON structure:", output);
          reject(
            new Error(
              "Unexpected JSON structure from lockplane CLI. Expected 'diagnostics' and 'summary' fields."
            )
          );
        } catch (parseError) {
          // If JSON parsing fails, treat as a general error
          console.error("[Lockplane] Failed to parse JSON output:", parseError);

          // Check if there's a validation error in stderr
          if (stderr && stderr.includes("Failed to parse SQL")) {
            const lineMatch = stderr.match(/line (\d+)/i);
            const lineNum = lineMatch ? parseInt(lineMatch[1]) : 1;

            resolve([
              {
                file: schemaPath,
                line: lineNum,
                column: 1,
                severity: "error",
                message: stderr.trim(),
              },
            ]);
          } else {
            reject(
              new Error(`Failed to parse schema-check output: ${parseError}`)
            );
          }
        }
      }
    );
  });
}
