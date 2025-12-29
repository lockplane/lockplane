package schema

// Diagnostic represents a validation error, warning, or info message
type Diagnostic struct {
	Code     string `json:"code,omitempty"`
	Message  string `json:"message"`
	Severity string `json:"severity"` // "error", "warning", "info"
	File     string `json:"file,omitempty"`
	Line     int    `json:"line,omitempty"`
	Column   int    `json:"column,omitempty"`
}

// Summary represents a summary of the validation results
type Summary struct {
	Errors   int  `json:"errors"`
	Warnings int  `json:"warnings,omitempty"`
	Valid    bool `json:"valid"`
}

// CheckOutput represents the structured output of schema validation
type CheckOutput struct {
	Diagnostics []Diagnostic `json:"diagnostics,omitempty"`
	Summary     Summary      `json:"summary"`
}

// NewCheckOutput creates a new CheckOutput with an empty diagnostics list
func NewCheckOutput() *CheckOutput {
	return &CheckOutput{
		Diagnostics: []Diagnostic{},
		Summary: Summary{
			Valid: true,
		},
	}
}

// AddError adds an error diagnostic
func (c *CheckOutput) AddError(message string, file string, line int, column int) {
	c.Diagnostics = append(c.Diagnostics, Diagnostic{
		Message:  message,
		Severity: "error",
		File:     file,
		Line:     line,
		Column:   column,
	})
	c.Summary.Errors++
	c.Summary.Valid = false
}

// AddWarning adds a warning diagnostic
func (c *CheckOutput) AddWarning(message string, file string, line int, column int) {
	c.Diagnostics = append(c.Diagnostics, Diagnostic{
		Message:  message,
		Severity: "warning",
		File:     file,
		Line:     line,
		Column:   column,
	})
	c.Summary.Warnings++
}
