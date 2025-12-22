// Package main implements the pglink documentation generator.
// It reads type definitions and comments from pkg/config and generates
// documentation for the pglink.json configuration file format.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/doc"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"text/template"
)

// FieldDoc describes a JSON field in the configuration.
type FieldDoc struct {
	Name        string // JSON field name
	GoName      string // Go field name
	Type        string // JSON/Go type
	Required    bool   // Whether the field is required
	Default     string // Default value, if any
	Description string // Description from doc comment
	Nested      string // Name of nested type, if applicable
}

// TypeDoc describes a configuration type.
type TypeDoc struct {
	Name        string     // Type name (e.g., "Config", "BackendConfig")
	Description string     // Type description from doc comment
	Fields      []FieldDoc // Fields in the type
}

// EnumDoc describes an enumeration type.
type EnumDoc struct {
	Name        string            // Type name
	Description string            // Type description
	Values      map[string]string // Value -> description
}

// ConfigDocs holds all extracted documentation.
type ConfigDocs struct {
	Types []TypeDoc
	Enums []EnumDoc
}

// TemplateData holds all data passed to the README template.
type TemplateData struct {
	Banner     string
	Flags      []FlagDoc
	Config     ConfigDocs
	CLIExample string
}

// FlagDoc describes a CLI flag.
type FlagDoc struct {
	Name        string
	Type        string
	Default     string
	Description string
}

var (
	inputFile  = flag.String("in", "README.in.md", "input template file")
	outputFile = flag.String("out", "README.md", "output file")
	configPkg  = flag.String("config-pkg", "pkg/config", "path to config package")
)

func main() {
	flag.Parse()

	// Parse the config package
	docs, err := parseConfigPackage(*configPkg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing config package: %v\n", err)
		os.Exit(1)
	}

	// Read template
	tmplContent, err := os.ReadFile(*inputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading template: %v\n", err)
		os.Exit(1)
	}

	// Prepare template data
	data := TemplateData{
		Banner: renderBanner(),
		Flags:  getCLIFlags(),
		Config: docs,
		CLIExample: `pglink -config /etc/pglink/pglink.json
pglink -config pglink.json -json`,
	}

	// Parse and execute template
	tmpl, err := template.New("readme").Funcs(templateFuncs()).Parse(string(tmplContent))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing template: %v\n", err)
		os.Exit(1)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		fmt.Fprintf(os.Stderr, "Error executing template: %v\n", err)
		os.Exit(1)
	}

	// Write output
	if err := os.WriteFile(*outputFile, buf.Bytes(), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing output: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Generated %s from %s\n", *outputFile, *inputFile)
}

func templateFuncs() template.FuncMap {
	return template.FuncMap{
		"indent": func(spaces int, s string) string {
			prefix := strings.Repeat(" ", spaces)
			lines := strings.Split(s, "\n")
			for i, line := range lines {
				if line != "" {
					lines[i] = prefix + line
				}
			}
			return strings.Join(lines, "\n")
		},
		"lower": strings.ToLower,
		"typeAnchor": func(name string) string {
			return strings.ToLower(name)
		},
		"jsonType": func(goType string) string {
			switch goType {
			case "string", "*string", "AuthMethod", "SSLMode", "ListenAddr", "SecretRef":
				return "string"
			case "int32", "*int32", "int", "*int":
				return "integer"
			case "*uint16", "uint16":
				return "integer"
			case "bool", "*bool":
				return "boolean"
			case "[]string":
				return "array"
			case "[]ListenAddr":
				return "array"
			case "[]UserConfig":
				return "array"
			case "map[string]*DatabaseConfig":
				return "object"
			case "PgStartupParameters":
				return "object"
			default:
				if strings.HasPrefix(goType, "*") {
					return strings.TrimPrefix(goType, "*")
				}
				return goType
			}
		},
	}
}

func renderBanner() string {
	lines := []string{
		`                  __ _         __   `,
		`    ____   ____ _/ /(_)____   / /__ `,
		`   / __ \ / __ '/ // // __ \ / //_/ `,
		`  / /_/ // /_/ / // // / / // ,<    `,
		` / .___/ \__, /_//_//_/ /_//_/|_|   `,
		`/_/     /____/                      `,
	}
	return strings.Join(lines, "\n")
}

func getCLIFlags() []FlagDoc {
	return []FlagDoc{
		{
			Name:        "config",
			Type:        "string",
			Default:     "",
			Description: "Path to pglink.json config file (required)",
		},
		{
			Name:        "json",
			Type:        "bool",
			Default:     "false",
			Description: "Output logs in JSON format",
		},
		{
			Name:        "help",
			Type:        "bool",
			Default:     "false",
			Description: "Show full documentation",
		},
	}
}

func parseConfigPackage(pkgPath string) (ConfigDocs, error) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, pkgPath, nil, parser.ParseComments)
	if err != nil {
		return ConfigDocs{}, fmt.Errorf("parsing package: %w", err)
	}

	var docs ConfigDocs

	for _, pkg := range pkgs {
		// Use go/doc to extract documentation
		docPkg := doc.New(pkg, pkgPath, doc.AllDecls)

		// Extract type docs
		for _, t := range docPkg.Types {
			typeDoc := extractTypeDoc(t)
			if typeDoc != nil {
				docs.Types = append(docs.Types, *typeDoc)
			}

			enumDoc := extractEnumDoc(t)
			if enumDoc != nil {
				docs.Enums = append(docs.Enums, *enumDoc)
			}
		}
	}

	// Sort types by importance
	typeOrder := map[string]int{
		"Config":         0,
		"DatabaseConfig": 1,
		"UserConfig":     2,
		"BackendConfig":  3,
		"JsonTLSConfig":  4,
		"SecretRef":      5,
	}
	sort.Slice(docs.Types, func(i, j int) bool {
		oi, ok := typeOrder[docs.Types[i].Name]
		if !ok {
			oi = 100
		}
		oj, ok := typeOrder[docs.Types[j].Name]
		if !ok {
			oj = 100
		}
		return oi < oj
	})

	return docs, nil
}

func extractTypeDoc(t *doc.Type) *TypeDoc {
	// Only process struct types
	if t.Decl.Tok != token.TYPE {
		return nil
	}

	for _, spec := range t.Decl.Specs {
		ts, ok := spec.(*ast.TypeSpec)
		if !ok {
			continue
		}

		st, ok := ts.Type.(*ast.StructType)
		if !ok {
			continue
		}

		// Skip types without JSON fields
		hasJSONFields := false
		for _, field := range st.Fields.List {
			if field.Tag != nil && strings.Contains(field.Tag.Value, `json:`) {
				hasJSONFields = true
				break
			}
		}
		if !hasJSONFields {
			continue
		}

		// Skip internal types
		if ts.Name.Name == "SecretCache" || ts.Name.Name == "TLSResult" || ts.Name.Name == "PgStartupParameters" {
			continue
		}

		typeDoc := &TypeDoc{
			Name:        ts.Name.Name,
			Description: strings.TrimSpace(t.Doc),
		}

		// Extract fields with their docs
		for _, field := range st.Fields.List {
			fieldDoc := extractFieldDoc(field)
			if fieldDoc != nil {
				typeDoc.Fields = append(typeDoc.Fields, *fieldDoc)
			}
		}

		if len(typeDoc.Fields) > 0 {
			return typeDoc
		}
	}

	return nil
}

func extractFieldDoc(field *ast.Field) *FieldDoc {
	if field.Tag == nil {
		return nil
	}

	tag := reflect.StructTag(strings.Trim(field.Tag.Value, "`"))
	jsonTag := tag.Get("json")
	if jsonTag == "" || jsonTag == "-" {
		return nil
	}

	parts := strings.Split(jsonTag, ",")
	jsonName := parts[0]
	if jsonName == "" {
		return nil
	}

	// Check if optional (omitempty or omitzero)
	isOptional := false
	for _, part := range parts[1:] {
		if part == "omitempty" || part == "omitzero" {
			isOptional = true
			break
		}
	}

	// Get field name
	goName := ""
	if len(field.Names) > 0 {
		goName = field.Names[0].Name
	}

	// Get type
	goType := formatType(field.Type)

	// Get doc comment
	docComment := ""
	if field.Doc != nil {
		docComment = strings.TrimSpace(field.Doc.Text())
	} else if field.Comment != nil {
		docComment = strings.TrimSpace(field.Comment.Text())
	}

	// Extract default value from doc if present
	defaultVal := extractDefault(docComment)

	// Determine if this is a nested type
	nested := ""
	switch goType {
	case "*JsonTLSConfig":
		nested = "JsonTLSConfig"
	case "*DatabaseConfig", "map[string]*DatabaseConfig":
		nested = "DatabaseConfig"
	case "BackendConfig":
		nested = "BackendConfig"
	case "[]UserConfig":
		nested = "UserConfig"
	case "SecretRef":
		nested = "SecretRef"
	}

	return &FieldDoc{
		Name:        jsonName,
		GoName:      goName,
		Type:        goType,
		Required:    !isOptional,
		Default:     defaultVal,
		Description: docComment,
		Nested:      nested,
	}
}

func formatType(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.StarExpr:
		return "*" + formatType(t.X)
	case *ast.ArrayType:
		return "[]" + formatType(t.Elt)
	case *ast.MapType:
		return "map[" + formatType(t.Key) + "]" + formatType(t.Value)
	case *ast.SelectorExpr:
		return formatType(t.X) + "." + t.Sel.Name
	default:
		return "unknown"
	}
}

func extractDefault(doc string) string {
	// Look for "Defaults to X" or "defaults to X" patterns
	re := regexp.MustCompile(`[Dd]efaults?\s+to\s+(\S+|\d+|"[^"]*")`)
	matches := re.FindStringSubmatch(doc)
	if len(matches) > 1 {
		return matches[1]
	}
	return ""
}

func extractEnumDoc(t *doc.Type) *EnumDoc {
	// Check if this is an enum type (string or int alias with const values)
	if t.Decl.Tok != token.TYPE {
		return nil
	}

	for _, spec := range t.Decl.Specs {
		ts, ok := spec.(*ast.TypeSpec)
		if !ok {
			continue
		}

		// Only process named types that are aliases
		ident, ok := ts.Type.(*ast.Ident)
		if !ok {
			continue
		}

		if ident.Name != "string" {
			continue
		}

		// Check for const values
		if len(t.Consts) == 0 {
			continue
		}

		// Skip certain types
		if ts.Name.Name == "ListenAddr" {
			continue
		}

		enumDoc := &EnumDoc{
			Name:        ts.Name.Name,
			Description: strings.TrimSpace(t.Doc),
			Values:      make(map[string]string),
		}

		for _, c := range t.Consts {
			for _, spec := range c.Decl.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}

				// Get the string value
				for i, val := range vs.Values {
					if lit, ok := val.(*ast.BasicLit); ok && lit.Kind == token.STRING {
						constName := ""
						if i < len(vs.Names) {
							constName = vs.Names[i].Name
						}
						strVal := strings.Trim(lit.Value, `"`)

						// Get doc comment for this value
						constDoc := ""
						if vs.Doc != nil {
							constDoc = strings.TrimSpace(vs.Doc.Text())
						} else if vs.Comment != nil {
							constDoc = strings.TrimSpace(vs.Comment.Text())
						}

						// Use const name as key for lookup, but store the string value
						_ = constName // avoid unused variable
						enumDoc.Values[strVal] = constDoc
					}
				}
			}
		}

		if len(enumDoc.Values) > 0 {
			return enumDoc
		}
	}

	return nil
}

// Generate compact help text
func GenerateCompactHelp() string {
	return `pglink - PostgreSQL wire protocol proxy

Usage: pglink -config <path> [options]

Options:
  -config string   Path to pglink.json config file (required)
  -json            Output logs in JSON format
  -help            Show full documentation

Example:
  pglink -config /etc/pglink/pglink.json

Run 'pglink -help' for full configuration documentation.`
}

// GetDocMarkdownPath returns the path to the generated README.md
func GetDocMarkdownPath() string {
	// Try to find README.md relative to the executable
	execPath, err := os.Executable()
	if err != nil {
		return ""
	}

	// Check common locations
	candidates := []string{
		filepath.Join(filepath.Dir(execPath), "..", "README.md"),
		filepath.Join(filepath.Dir(execPath), "README.md"),
		"README.md",
	}

	for _, path := range candidates {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}

	return ""
}
