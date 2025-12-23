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
	Type        string // Go type
	JSONType    string // JSON type for display
	Required    bool   // Whether the field is required
	Default     string // Default value, if any
	Description string // Description from doc comment (cleaned)
	Nested      string // Name of nested type, if applicable
}

// TypeDoc describes a configuration type.
type TypeDoc struct {
	Name        string     // Type name (e.g., "Config", "BackendConfig")
	DisplayName string     // Display name for docs (may differ from Name)
	Description string     // Type description from doc comment
	Fields      []FieldDoc // Fields in the type
}

// EnumValue describes a single enum value.
type EnumValue struct {
	Value       string // The string value
	Description string // Description from doc comment
}

// EnumDoc describes an enumeration type.
type EnumDoc struct {
	Name        string      // Type name
	Description string      // Type description
	Values      []EnumValue // Values in declaration order
}

// ConfigDocs holds all extracted documentation.
type ConfigDocs struct {
	Types []TypeDoc
	Enums []EnumDoc
}

// FieldNameMap maps Go field names to their JSON names and containing type.
type FieldNameMap map[string]FieldNameInfo

type FieldNameInfo struct {
	JSONName string
	TypeName string // The type containing this field
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
		"lower":      strings.ToLower,
		"trimPrefix": strings.TrimPrefix,
		"typeAnchor": func(name string) string {
			return strings.ToLower(name)
		},
		"getType": func(types []TypeDoc, name string) *TypeDoc {
			for i := range types {
				if types[i].Name == name {
					return &types[i]
				}
			}
			return nil
		},
		"getEnum": func(enums []EnumDoc, name string) *EnumDoc {
			for i := range enums {
				if enums[i].Name == name {
					return &enums[i]
				}
			}
			return nil
		},
		"oneline": func(s string) string {
			// Replace newlines with spaces and collapse multiple spaces
			s = strings.ReplaceAll(s, "\n", " ")
			return strings.Join(strings.Fields(s), " ")
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

	// Sort types in depth-first pre-order based on field references
	// Start with Config, then follow nested type references
	docs.Types = sortTypesDepthFirst(docs.Types)

	// Build field name map and fix references in descriptions
	fieldMap := buildFieldNameMap(docs.Types)
	for i := range docs.Types {
		docs.Types[i].Description = replaceFieldReferences(docs.Types[i].Description, fieldMap)
		for j := range docs.Types[i].Fields {
			docs.Types[i].Fields[j].Description = replaceFieldReferences(docs.Types[i].Fields[j].Description, fieldMap)
		}
	}

	// Sort enums by importance
	enumOrder := map[string]int{
		"AuthMethod": 0,
		"SSLMode":    1,
	}
	sort.Slice(docs.Enums, func(i, j int) bool {
		oi, ok := enumOrder[docs.Enums[i].Name]
		if !ok {
			oi = 100
		}
		oj, ok := enumOrder[docs.Enums[j].Name]
		if !ok {
			oj = 100
		}
		return oi < oj
	})

	return docs, nil
}

// buildFieldNameMap creates a mapping from Go field names to JSON field names.
func buildFieldNameMap(types []TypeDoc) FieldNameMap {
	m := make(FieldNameMap)
	for _, t := range types {
		for _, f := range t.Fields {
			if f.GoName != "" && f.Name != "" {
				m[f.GoName] = FieldNameInfo{
					JSONName: f.Name,
					TypeName: t.Name,
				}
			}
		}
	}
	return m
}

// replaceFieldReferences replaces Go field names with JSON field names in descriptions.
// It also wraps them in backticks for code formatting.
// Only replaces PascalCase compound names to avoid false positives with common words.
func replaceFieldReferences(desc string, fieldMap FieldNameMap) string {
	if desc == "" {
		return desc
	}

	// Sort field names by length (longest first) to avoid partial replacements
	var goNames []string
	for name := range fieldMap {
		// Only include names that are clearly PascalCase compound words
		// (have a lowercase letter followed by uppercase, or are all caps with length > 3)
		// This avoids replacing common words like "TLS", "Host", "Port", "Key"
		if isPascalCaseCompound(name) {
			goNames = append(goNames, name)
		}
	}
	sort.Slice(goNames, func(i, j int) bool {
		return len(goNames[i]) > len(goNames[j])
	})

	// Replace each Go field name with its JSON equivalent
	for _, goName := range goNames {
		info := fieldMap[goName]
		// Match the Go name as a whole word (not part of another word)
		pattern := regexp.MustCompile(`\b` + regexp.QuoteMeta(goName) + `\b`)
		replacement := "`" + info.JSONName + "`"
		desc = pattern.ReplaceAllString(desc, replacement)
	}

	return desc
}

// isPascalCaseCompound returns true if the name is a PascalCase compound word
// (has transitions from lowercase to uppercase like "AwsSecretArn" or "CertPath")
func isPascalCaseCompound(name string) bool {
	if len(name) < 2 {
		return false
	}
	// Look for lowercase followed by uppercase (indicates compound word)
	for i := 0; i < len(name)-1; i++ {
		if name[i] >= 'a' && name[i] <= 'z' && name[i+1] >= 'A' && name[i+1] <= 'Z' {
			return true
		}
	}
	return false
}

// sortTypesDepthFirst sorts types in depth-first pre-order based on field references.
// Starting from Config, when we encounter a field that references another type,
// we output that type immediately after before continuing with other fields.
func sortTypesDepthFirst(types []TypeDoc) []TypeDoc {
	// Build a map for quick lookup
	typeMap := make(map[string]*TypeDoc)
	for i := range types {
		typeMap[types[i].Name] = &types[i]
	}

	var result []TypeDoc
	visited := make(map[string]bool)

	// Helper to get referenced type name from a field
	getNestedType := func(field FieldDoc) string {
		switch field.Type {
		case "*JsonTLSConfig":
			return "JsonTLSConfig"
		case "map[string]*DatabaseConfig":
			return "DatabaseConfig"
		case "BackendConfig":
			return "BackendConfig"
		case "[]UserConfig":
			return "UserConfig"
		case "SecretRef":
			return "SecretRef"
		}
		return ""
	}

	// Depth-first traversal
	var visit func(name string)
	visit = func(name string) {
		if visited[name] {
			return
		}
		t, ok := typeMap[name]
		if !ok {
			return
		}
		visited[name] = true
		result = append(result, *t)

		// Visit nested types in field order
		for _, field := range t.Fields {
			if nested := getNestedType(field); nested != "" {
				visit(nested)
			}
		}
	}

	// Start with Config
	visit("Config")

	// Add any remaining types that weren't reachable from Config
	for _, t := range types {
		if !visited[t.Name] {
			result = append(result, t)
		}
	}

	return result
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

		// Extract display name and description from doc comment
		docText := strings.TrimSpace(t.Doc)
		displayName := ts.Name.Name
		description := docText

		// Check for @docname annotation
		if idx := strings.Index(docText, "@docname "); idx != -1 {
			// Extract the display name
			rest := docText[idx+len("@docname "):]
			if endIdx := strings.IndexAny(rest, " \n\t"); endIdx != -1 {
				displayName = rest[:endIdx]
				// Remove the @docname line from description
				description = strings.TrimSpace(docText[:idx] + rest[endIdx:])
			} else {
				displayName = rest
				description = strings.TrimSpace(docText[:idx])
			}
		}

		typeDoc := &TypeDoc{
			Name:        ts.Name.Name,
			DisplayName: displayName,
			Description: cleanTypeDescription(displayName, description),
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

// cleanTypeDescription removes redundant "TypeName ..." prefix from type descriptions.
func cleanTypeDescription(typeName, desc string) string {
	// Remove patterns like "TypeName holds..." or "TypeName configures..."
	prefixes := []string{
		typeName + " holds ",
		typeName + " configures ",
		typeName + " represents ",
		typeName + " identifies ",
		typeName + " is ",
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(desc, prefix) {
			desc = strings.TrimPrefix(desc, prefix)
			// Capitalize first letter
			if len(desc) > 0 {
				desc = strings.ToUpper(desc[:1]) + desc[1:]
			}
			break
		}
	}
	return desc
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

	// Clean up the doc comment
	docComment = cleanFieldDescription(goName, docComment)

	// Extract default value from doc if present
	defaultVal := extractDefault(docComment)

	// Remove the "Defaults to X" from description since we show it separately
	if defaultVal != "" {
		docComment = removeDefaultFromDescription(docComment)
	}

	// Determine JSON type for display
	jsonType := goTypeToJSONType(goType)

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
		JSONType:    jsonType,
		Required:    !isOptional,
		Default:     defaultVal,
		Description: docComment,
		Nested:      nested,
	}
}

// cleanFieldDescription removes redundant "FieldName is/are" prefix from field descriptions.
func cleanFieldDescription(fieldName, desc string) string {
	if desc == "" {
		return ""
	}

	// Remove patterns like "FieldName is..." or "FieldName specifies..."
	prefixes := []string{
		fieldName + " is ",
		fieldName + " are ",
		fieldName + " specifies ",
		fieldName + " controls ",
		fieldName + " configures ",
		fieldName + " enables ",
		fieldName + " adds ",
		fieldName + " maps ",
		fieldName + ", when ",
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(desc, prefix) {
			desc = strings.TrimPrefix(desc, prefix)
			// Capitalize first letter
			if len(desc) > 0 {
				desc = strings.ToUpper(desc[:1]) + desc[1:]
			}
			break
		}
	}
	return desc
}

func goTypeToJSONType(goType string) string {
	switch goType {
	case "string", "*string", "AuthMethod", "SSLMode", "ListenAddr":
		return "string"
	case "SecretRef":
		return "[SecretRef](#secretref)"
	case "int32", "*int32", "int", "*int":
		return "integer"
	case "*uint16", "uint16":
		return "integer"
	case "bool", "*bool":
		return "boolean"
	case "[]string":
		return "[]string"
	case "[]ListenAddr":
		return "[]string"
	case "[]UserConfig":
		return "[][UserConfig](#userconfig)"
	case "map[string]*DatabaseConfig":
		return "map[string][DatabaseConfig](#databaseconfig)"
	case "PgStartupParameters":
		return "map[string]string"
	case "*JsonTLSConfig":
		return "[TLSConfig](#tlsconfig)"
	case "BackendConfig":
		return "[BackendConfig](#backendconfig)"
	default:
		if strings.HasPrefix(goType, "*") {
			return goTypeToJSONType(strings.TrimPrefix(goType, "*"))
		}
		if strings.HasPrefix(goType, "[]") {
			inner := goTypeToJSONType(strings.TrimPrefix(goType, "[]"))
			return "[]" + inner
		}
		return goType
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
		// Remove trailing punctuation
		return strings.TrimRight(matches[1], ".,;:")
	}
	return ""
}

// removeDefaultFromDescription removes "Defaults to X" sentences from a description
// since the default is shown separately in the table.
func removeDefaultFromDescription(desc string) string {
	// Remove sentences containing "Defaults to" or "defaults to"
	// Handle both mid-sentence and end-of-sentence cases
	re := regexp.MustCompile(`\s*[Dd]efaults?\s+to\s+[^.]+\.?`)
	desc = re.ReplaceAllString(desc, "")
	// Clean up any double spaces or trailing spaces
	desc = strings.TrimSpace(desc)
	desc = regexp.MustCompile(`\s+`).ReplaceAllString(desc, " ")
	return desc
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
			Description: cleanTypeDescription(ts.Name.Name, strings.TrimSpace(t.Doc)),
			Values:      []EnumValue{},
		}

		for _, c := range t.Consts {
			for _, spec := range c.Decl.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}

				// Get the string value and doc for each const
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

						// Clean up the const doc - remove the const name prefix
						constDoc = cleanEnumValueDescription(constName, constDoc)

						enumDoc.Values = append(enumDoc.Values, EnumValue{
							Value:       strVal,
							Description: constDoc,
						})
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

// cleanEnumValueDescription removes the const name prefix from enum value descriptions.
func cleanEnumValueDescription(constName, desc string) string {
	if desc == "" {
		return ""
	}

	// Remove patterns like "ConstName uses..." or "ConstName is..."
	prefixes := []string{
		constName + " uses ",
		constName + " is ",
		constName + " means ",
		constName + " represents ",
		constName + " requires ",
		constName + " disables ",
		constName + " accepts ",
		constName + " prefers ",
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(desc, prefix) {
			desc = strings.TrimPrefix(desc, prefix)
			// Capitalize first letter
			if len(desc) > 0 {
				desc = strings.ToUpper(desc[:1]) + desc[1:]
			}
			break
		}
	}
	return desc
}
