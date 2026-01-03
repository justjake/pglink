//go:build ignore

package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/printer"
	"go/token"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var from = flag.String("from", "", "the origin of the message: Client or Server (filters //pgwire: comments)")

func main() {
	flag.Parse()

	if *from == "" {
		log.Fatal("-from is required (Client or Server)")
	}

	// Get the source file from GOFILE env var (set by go generate)
	gofile := os.Getenv("GOFILE")
	if gofile == "" {
		gofile = "generate_templates.go"
	}

	// Parse the source file with comments
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, gofile, nil, parser.ParseComments)
	if err != nil {
		log.Fatalf("failed to parse %s: %v", gofile, err)
	}

	// Extract package name
	pkgName := file.Name.Name

	// Extract imports
	imports := extractImports(fset, file)

	// Find all template functions with //pgwire: comments matching -from
	var groups []typeGroup
	for _, decl := range file.Decls {
		funcDecl, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}

		// Look for //pgwire: comment above the function
		config := findPgwireComment(file, funcDecl)
		if config == nil {
			continue
		}

		// Check if -from matches
		if config.from != *from {
			continue
		}

		// Extract types from the type switch
		types := extractTypesFromFunc(fset, file, funcDecl)
		if len(types) == 0 {
			continue
		}

		groups = append(groups, typeGroup{
			prefix:  config.typePrefix,
			types:   types,
			methods: config.methods,
		})
	}

	if len(groups) == 0 {
		log.Fatalf("no template functions found for -from=%s", *from)
	}

	// Generate the output
	output := generateCode(pkgName, imports, *from, groups)

	// Write to output file
	outFile := toSnakeCase(*from) + "_generated.go"
	outPath := filepath.Join(filepath.Dir(gofile), outFile)
	if err := os.WriteFile(outPath, output, 0644); err != nil {
		log.Fatalf("failed to write %s: %v", outPath, err)
	}

	totalTypes := 0
	for _, g := range groups {
		totalTypes += len(g.types)
	}
	fmt.Printf("Generated %s with %d groups, %d types\n", outFile, len(groups), totalTypes)
}

type pgwireConfig struct {
	from       string
	typePrefix string
	methods    []methodDef
}

type methodDef struct {
	name       string // e.g., "ParseFrontend"
	returnType string // e.g., "pgproto3.FrontendMessage"
	body       string // e.g., "m.Parse()"
}

// findPgwireComment looks for a //pgwire: comment above the function
// and parses -from=X and -type=Y from it.
func findPgwireComment(file *ast.File, funcDecl *ast.FuncDecl) *pgwireConfig {
	funcPos := funcDecl.Pos()

	// Find the comment group closest to and immediately preceding the function
	var closestGroup *ast.CommentGroup
	for _, cg := range file.Comments {
		// Comment must end before function starts
		if cg.End() < funcPos {
			// Track the closest one (later in file = closer to function)
			if closestGroup == nil || cg.End() > closestGroup.End() {
				closestGroup = cg
			}
		}
	}

	if closestGroup == nil {
		return nil
	}

	// Check if the closest comment group has a pgwire directive
	// Handle both "//pgwire:" and "// pgwire:" formats
	for _, c := range closestGroup.List {
		text := c.Text
		if strings.HasPrefix(text, "//pgwire:") {
			text = strings.TrimPrefix(text, "//pgwire:")
			return parsePgwireComment(text)
		}
		if strings.HasPrefix(text, "// pgwire:") {
			text = strings.TrimPrefix(text, "// pgwire:")
			return parsePgwireComment(text)
		}
	}
	return nil
}

func parsePgwireComment(text string) *pgwireConfig {
	config := &pgwireConfig{}

	// Parse -from=X
	fromRe := regexp.MustCompile(`-from=(\w+)`)
	if matches := fromRe.FindStringSubmatch(text); len(matches) >= 2 {
		config.from = matches[1]
	}

	// Parse -type=X
	typeRe := regexp.MustCompile(`-type=(\w+)`)
	if matches := typeRe.FindStringSubmatch(text); len(matches) >= 2 {
		config.typePrefix = matches[1]
	}

	// Parse -method=Name:ReturnType:Body (can appear multiple times)
	methodRe := regexp.MustCompile(`-method=(\w+):([^:]+):([^\s]+)`)
	for _, m := range methodRe.FindAllStringSubmatch(text, -1) {
		config.methods = append(config.methods, methodDef{
			name:       m[1],
			returnType: m[2],
			body:       m[3],
		})
	}

	if config.from == "" || config.typePrefix == "" {
		return nil
	}
	return config
}

type typeGroup struct {
	prefix  string
	types   []typeInfo
	methods []methodDef
}

type typeInfo struct {
	qualified string   // e.g., "*pgproto3.GSSEncRequest"
	shortName string   // e.g., "GSSEncRequest"
	comments  []string // comments from the case clause
}

func toSnakeCase(s string) string {
	var result []rune
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			result = append(result, '_')
		}
		result = append(result, r)
	}
	return strings.ToLower(string(result))
}

func extractImports(fset *token.FileSet, file *ast.File) []string {
	var imports []string
	for _, imp := range file.Imports {
		var buf bytes.Buffer
		printer.Fprint(&buf, fset, imp)
		impStr := buf.String()
		if strings.Contains(impStr, "pgproto3") {
			imports = append(imports, impStr)
		}
	}
	return imports
}

func extractTypesFromFunc(fset *token.FileSet, file *ast.File, funcDecl *ast.FuncDecl) []typeInfo {
	// Find the type switch in the function body
	var typeSwitch *ast.TypeSwitchStmt
	ast.Inspect(funcDecl.Body, func(n ast.Node) bool {
		if ts, ok := n.(*ast.TypeSwitchStmt); ok {
			typeSwitch = ts
			return false
		}
		return true
	})
	if typeSwitch == nil {
		return nil
	}

	// Extract types from the switch cases
	var types []typeInfo
	caseClauses := typeSwitch.Body.List
	for i, stmt := range caseClauses {
		caseClause, ok := stmt.(*ast.CaseClause)
		if !ok || caseClause.List == nil {
			continue
		}

		var nextCasePos token.Pos
		if i+1 < len(caseClauses) {
			nextCasePos = caseClauses[i+1].Pos()
		} else {
			nextCasePos = typeSwitch.Body.Rbrace
		}

		comments := extractCaseComments(file, caseClause.Colon, nextCasePos)

		for _, expr := range caseClause.List {
			ti := extractTypeInfo(expr)
			if ti.shortName != "" {
				ti.comments = comments
				types = append(types, ti)
			}
		}
	}
	return types
}

func extractCaseComments(file *ast.File, colonPos, nextCasePos token.Pos) []string {
	var comments []string
	for _, cg := range file.Comments {
		if cg.Pos() > colonPos && cg.End() < nextCasePos {
			for _, c := range cg.List {
				text := strings.TrimPrefix(c.Text, "//")
				text = strings.TrimPrefix(text, " ")
				comments = append(comments, text)
			}
		}
	}
	return comments
}

func extractTypeInfo(expr ast.Expr) typeInfo {
	var ti typeInfo

	if star, ok := expr.(*ast.StarExpr); ok {
		inner := extractTypeInfo(star.X)
		ti.qualified = "*" + inner.qualified
		ti.shortName = inner.shortName
		return ti
	}

	if sel, ok := expr.(*ast.SelectorExpr); ok {
		if ident, ok := sel.X.(*ast.Ident); ok {
			ti.qualified = ident.Name + "." + sel.Sel.Name
			ti.shortName = sel.Sel.Name
			return ti
		}
	}

	if ident, ok := expr.(*ast.Ident); ok {
		ti.qualified = ident.Name
		ti.shortName = ident.Name
		return ti
	}

	return ti
}

func generateCode(pkgName string, imports []string, from string, groups []typeGroup) []byte {
	var buf bytes.Buffer

	// Header
	buf.WriteString("// Code generated by generate.go; DO NOT EDIT.\n\n")
	fmt.Fprintf(&buf, "package %s\n\n", pkgName)

	// Imports
	buf.WriteString("import (\n")
	buf.WriteString("\t\"context\"\n")
	buf.WriteString("\t\"fmt\"\n\n")
	for _, imp := range imports {
		fmt.Fprintf(&buf, "\t%s\n", imp)
	}
	buf.WriteString(")\n\n")

	lazyType := "From" + from

	// Collect all types for the unified handler
	var allTypes []unifiedType

	// Generate each group
	for _, group := range groups {
		interfaceName := from + group.prefix

		// Interface
		fmt.Fprintf(&buf, "// %s is implemented by all %s %s message wrapper types.\n", interfaceName, from, group.prefix)
		fmt.Fprintf(&buf, "type %s interface {\n", interfaceName)
		fmt.Fprintf(&buf, "\t%s()\n", from)
		fmt.Fprintf(&buf, "\t%s()\n", group.prefix)
		fmt.Fprintf(&buf, "\tMsgType() MsgType\n")
		for _, md := range group.methods {
			fmt.Fprintf(&buf, "\t%s() %s\n", md.name, md.returnType)
		}
		buf.WriteString("}\n\n")

		// Compile-time checks
		buf.WriteString("// Compile-time checks that all wrapper types implement the interface.\n")
		buf.WriteString("var (\n")
		for _, ti := range group.types {
			newTypeName := typeName(from, ti.shortName)
			fmt.Fprintf(&buf, "\t_ %s = (*%s)(nil)\n", interfaceName, newTypeName)
		}
		buf.WriteString(")\n\n")

		// Type definitions
		for _, ti := range group.types {
			newTypeName := typeName(from, ti.shortName)

			if len(ti.comments) > 0 {
				for _, comment := range ti.comments {
					fmt.Fprintf(&buf, "// %s\n", comment)
				}
			} else {
				fmt.Fprintf(&buf, "// %s wraps %s from the %s.\n", newTypeName, ti.qualified, strings.ToLower(from))
			}

			fmt.Fprintf(&buf, "type %s %s[%s]\n\n", newTypeName, lazyType, ti.qualified)

			// Marker methods
			fmt.Fprintf(&buf, "func (*%s) %s() {}\n", newTypeName, from)
			fmt.Fprintf(&buf, "func (*%s) %s() {}\n", newTypeName, group.prefix)
			fmt.Fprintf(&buf, "func (t *%s) MsgType() MsgType { return t.source.MessageType() }\n", newTypeName)

			// Parse method
			fmt.Fprintf(&buf, "func (m *%s) Parse() %s { return (*%s[%s])(m).Parse() }\n", newTypeName, ti.qualified, lazyType, ti.qualified)

			// Additional methods from -method= flags
			for _, md := range group.methods {
				fmt.Fprintf(&buf, "func (m *%s) %s() %s { return %s }\n", newTypeName, md.name, md.returnType, md.body)
			}

			// Retain method
			fmt.Fprintf(&buf, "\n// Retain returns a copy of this message with retained source bytes.\n")
			fmt.Fprintf(&buf, "// Use this when the message must outlive the current iteration.\n")
			fmt.Fprintf(&buf, "func (m %s) Retain() %s {\n", newTypeName, newTypeName)
			fmt.Fprintf(&buf, "\tsrc, parsed, isParsed := (*%s[%s])(&m).retainFields()\n", lazyType, ti.qualified)
			fmt.Fprintf(&buf, "\treturn %s{source: src, parsed: parsed, isParsed: isParsed}\n", newTypeName)
			fmt.Fprintf(&buf, "}\n\n")

			// Add to unified types
			allTypes = append(allTypes, unifiedType{
				groupPrefix:   group.prefix,
				interfaceName: interfaceName,
				typeName:      newTypeName,
				shortName:     ti.shortName,
				qualified:     ti.qualified,
			})
		}

		// Conversion function
		newLazyFunc := from + "Parsed"
		funcName := "To" + interfaceName
		fmt.Fprintf(&buf, "// %s converts a %s to a %s if it matches one of the known types.\n", funcName, inputType(from), interfaceName)
		fmt.Fprintf(&buf, "// Note: This allocates. For zero-allocation iteration, use Cursor.As%s().\n", from)
		fmt.Fprintf(&buf, "func %s(msg %s) (%s, bool) {\n", funcName, inputType(from), interfaceName)
		buf.WriteString("\tswitch m := msg.(type) {\n")
		for _, ti := range group.types {
			newTypeName := typeName(from, ti.shortName)
			fmt.Fprintf(&buf, "\tcase %s:\n", ti.qualified)
			fmt.Fprintf(&buf, "\t\treturn (*%s)(%s(m)), true\n", newTypeName, newLazyFunc)
		}
		buf.WriteString("\t}\n")
		buf.WriteString("\treturn nil, false\n")
		buf.WriteString("}\n\n")

		// Group-specific handlers
		generateGroupHandlers(&buf, from, group.prefix, interfaceName, group.types)
	}

	// Generate unified handlers for all types
	generateUnifiedHandlers(&buf, from, groups, allTypes)

	// Format the code
	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		log.Printf("warning: generated code has formatting issues: %v", err)
		return buf.Bytes()
	}
	return formatted
}

type unifiedType struct {
	groupPrefix   string
	interfaceName string
	typeName      string
	shortName     string
	qualified     string
}

func inputType(from string) string {
	if from == "Client" {
		return "pgproto3.FrontendMessage"
	}
	return "pgproto3.BackendMessage"
}

// typeName generates the wrapper type name for a message type.
// e.g., typeName("Client", "GSSEncRequest") -> "ClientGSSEncRequest"
func typeName(from, shortName string) string {
	return from + shortName
}

func generateGroupHandlers(buf *bytes.Buffer, from, prefix, interfaceName string, types []typeInfo) {
	handlersName := from + prefix + "Handlers"

	// Handlers struct
	fmt.Fprintf(buf, "// %s provides type-safe handlers for each %s variant.\n", handlersName, interfaceName)
	fmt.Fprintf(buf, "type %s[T any] struct {\n", handlersName)
	fmt.Fprintf(buf, "\tDefault func(msg %s) (T, error)\n", interfaceName)
	for _, ti := range types {
		newTypeName := typeName(from, ti.shortName)
		fmt.Fprintf(buf, "\t%s func(msg *%s) (T, error)\n", ti.shortName, newTypeName)
	}
	buf.WriteString("}\n\n")

	// HandleDefault
	fmt.Fprintf(buf, "// HandleDefault dispatches to the appropriate handler, or calls defaultHandler if the handler is nil.\n")
	fmt.Fprintf(buf, "func (h %s[T]) HandleDefault(msg %s, defaultHandler func(msg %s) (T, error)) (r T, err error) {\n", handlersName, interfaceName, interfaceName)
	buf.WriteString("\tif h.Default != nil {\n")
	buf.WriteString("\t\tdefaultHandler = h.Default\n")
	buf.WriteString("\t} else if defaultHandler == nil {\n")
	fmt.Fprintf(buf, "\t\tdefaultHandler = func(msg %s) (T, error) {\n", interfaceName)
	fmt.Fprintf(buf, "\t\t\tpanic(fmt.Sprintf(\"no handler defined for %s message: %%T\", msg))\n", strings.ToLower(from+" "+prefix))
	buf.WriteString("\t\t}\n")
	buf.WriteString("\t}\n")
	buf.WriteString("\tswitch msg := msg.(type) {\n")
	for _, ti := range types {
		newTypeName := typeName(from, ti.shortName)
		fmt.Fprintf(buf, "\tcase *%s:\n", newTypeName)
		fmt.Fprintf(buf, "\t\tif h.%s != nil {\n", ti.shortName)
		fmt.Fprintf(buf, "\t\t\treturn h.%s(msg)\n", ti.shortName)
		buf.WriteString("\t\t}\n")
		buf.WriteString("\t\treturn defaultHandler(msg)\n")
	}
	buf.WriteString("\t}\n")
	fmt.Fprintf(buf, "\terr = fmt.Errorf(\"unknown %s message: %%T\", msg)\n", strings.ToLower(from+" "+prefix))
	buf.WriteString("\treturn\n")
	buf.WriteString("}\n\n")

	// Handle
	fmt.Fprintf(buf, "// Handle dispatches to the appropriate handler, or panics if the handler is nil.\n")
	fmt.Fprintf(buf, "func (h %s[T]) Handle(msg %s) (T, error) {\n", handlersName, interfaceName)
	buf.WriteString("\treturn h.HandleDefault(msg, nil)\n")
	buf.WriteString("}\n\n")

	// Context-aware handlers
	handlersCtxName := from + prefix + "HandlersCtx"
	fmt.Fprintf(buf, "// %s provides type-safe handlers with context and an argument for each %s variant.\n", handlersCtxName, interfaceName)
	fmt.Fprintf(buf, "type %s[Arg, Result any] struct {\n", handlersCtxName)
	fmt.Fprintf(buf, "\tDefault func(ctx context.Context, msg %s, arg Arg) (Result, error)\n", interfaceName)
	for _, ti := range types {
		newTypeName := typeName(from, ti.shortName)
		fmt.Fprintf(buf, "\t%s func(ctx context.Context, msg *%s, arg Arg) (Result, error)\n", ti.shortName, newTypeName)
	}
	buf.WriteString("}\n\n")

	// HandleDefault for Ctx
	fmt.Fprintf(buf, "// HandleDefault dispatches to the appropriate handler, or calls defaultHandler if the handler is nil.\n")
	fmt.Fprintf(buf, "func (h %s[Arg, Result]) HandleDefault(ctx context.Context, msg %s, arg Arg, defaultHandler func(ctx context.Context, msg %s, arg Arg) (Result, error)) (r Result, err error) {\n", handlersCtxName, interfaceName, interfaceName)
	buf.WriteString("\tif h.Default != nil {\n")
	buf.WriteString("\t\tdefaultHandler = h.Default\n")
	buf.WriteString("\t} else if defaultHandler == nil {\n")
	fmt.Fprintf(buf, "\t\tdefaultHandler = func(ctx context.Context, msg %s, arg Arg) (Result, error) {\n", interfaceName)
	fmt.Fprintf(buf, "\t\t\tpanic(fmt.Sprintf(\"no handler defined for %s message: %%T\", msg))\n", strings.ToLower(from+" "+prefix))
	buf.WriteString("\t\t}\n")
	buf.WriteString("\t}\n")
	buf.WriteString("\tswitch msg := msg.(type) {\n")
	for _, ti := range types {
		newTypeName := typeName(from, ti.shortName)
		fmt.Fprintf(buf, "\tcase *%s:\n", newTypeName)
		fmt.Fprintf(buf, "\t\tif h.%s != nil {\n", ti.shortName)
		fmt.Fprintf(buf, "\t\t\treturn h.%s(ctx, msg, arg)\n", ti.shortName)
		buf.WriteString("\t\t}\n")
		buf.WriteString("\t\treturn defaultHandler(ctx, msg, arg)\n")
	}
	buf.WriteString("\t}\n")
	fmt.Fprintf(buf, "\terr = fmt.Errorf(\"unknown %s message: %%T\", msg)\n", strings.ToLower(from+" "+prefix))
	buf.WriteString("\treturn\n")
	buf.WriteString("}\n\n")

	// Handle for Ctx
	fmt.Fprintf(buf, "// Handle dispatches to the appropriate handler, or panics if the handler is nil.\n")
	fmt.Fprintf(buf, "func (h %s[Arg, Result]) Handle(ctx context.Context, msg %s, arg Arg) (Result, error) {\n", handlersCtxName, interfaceName)
	buf.WriteString("\treturn h.HandleDefault(ctx, msg, arg, nil)\n")
	buf.WriteString("}\n\n")
}

func generateUnifiedHandlers(buf *bytes.Buffer, from string, groups []typeGroup, allTypes []unifiedType) {
	handlersName := from + "Handlers"
	msgInterface := from + "Message"

	// Unified handlers struct with context
	fmt.Fprintf(buf, "// %s provides type-safe dispatch for all %s variants.\n", handlersName, from)
	fmt.Fprintf(buf, "type %s[Arg, Result any] struct {\n", handlersName)
	fmt.Fprintf(buf, "\tDefault func(ctx context.Context, msg %s, arg Arg) (Result, error)\n", msgInterface)

	// Group by prefix for comments and group handler pointers
	currentPrefix := ""
	for _, ut := range allTypes {
		if ut.groupPrefix != currentPrefix {
			if currentPrefix != "" {
				buf.WriteString("\n")
			}
			currentPrefix = ut.groupPrefix
			fmt.Fprintf(buf, "\t// %s\n", ut.groupPrefix)
			handlersCtxName := from + ut.groupPrefix + "HandlersCtx"
			fmt.Fprintf(buf, "\t%s *%s[Arg, Result]\n", ut.groupPrefix, handlersCtxName)
		}
		fmt.Fprintf(buf, "\t%s func(ctx context.Context, msg *%s, arg Arg) (Result, error)\n", ut.shortName, ut.typeName)
	}
	buf.WriteString("}\n\n")

	// HandleDefault
	fmt.Fprintf(buf, "// HandleDefault dispatches to the appropriate handler, or calls defaultHandler if the handler is nil.\n")
	fmt.Fprintf(buf, "func (h %s[Arg, Result]) HandleDefault(ctx context.Context, msg %s, arg Arg, defaultHandler func(ctx context.Context, msg %s, arg Arg) (Result, error)) (r Result, err error) {\n", handlersName, msgInterface, msgInterface)
	buf.WriteString("\tif h.Default != nil {\n")
	buf.WriteString("\t\tdefaultHandler = h.Default\n")
	buf.WriteString("\t} else if defaultHandler == nil {\n")
	fmt.Fprintf(buf, "\t\tdefaultHandler = func(ctx context.Context, msg %s, arg Arg) (Result, error) {\n", msgInterface)
	fmt.Fprintf(buf, "\t\t\tpanic(fmt.Sprintf(\"no handler defined for %s message: %%T\", msg))\n", strings.ToLower(from))
	buf.WriteString("\t\t}\n")
	buf.WriteString("\t}\n")
	buf.WriteString("\tswitch msg := msg.(type) {\n")
	for _, ut := range allTypes {
		fmt.Fprintf(buf, "\tcase *%s:\n", ut.typeName)
		fmt.Fprintf(buf, "\t\tif h.%s != nil {\n", ut.shortName)
		fmt.Fprintf(buf, "\t\t\treturn h.%s(ctx, msg, arg)\n", ut.shortName)
		fmt.Fprintf(buf, "\t\t} else if h.%s != nil && h.%s.%s != nil {\n", ut.groupPrefix, ut.groupPrefix, ut.shortName)
		fmt.Fprintf(buf, "\t\t\treturn h.%s.%s(ctx, msg, arg)\n", ut.groupPrefix, ut.shortName)
		fmt.Fprintf(buf, "\t\t} else if h.%s != nil && h.%s.Default != nil {\n", ut.groupPrefix, ut.groupPrefix)
		fmt.Fprintf(buf, "\t\t\treturn h.%s.Default(ctx, msg, arg)\n", ut.groupPrefix)
		buf.WriteString("\t\t}\n")
		buf.WriteString("\t\treturn defaultHandler(ctx, msg, arg)\n")
	}
	buf.WriteString("\t}\n")
	fmt.Fprintf(buf, "\terr = fmt.Errorf(\"unknown %s message: %%T\", msg)\n", strings.ToLower(from))
	buf.WriteString("\treturn\n")
	buf.WriteString("}\n\n")

	// Handle
	fmt.Fprintf(buf, "// Handle dispatches to the appropriate handler, or panics if the handler is nil.\n")
	fmt.Fprintf(buf, "func (h %s[Arg, Result]) Handle(ctx context.Context, msg %s, arg Arg) (Result, error) {\n", handlersName, msgInterface)
	buf.WriteString("\treturn h.HandleDefault(ctx, msg, arg, nil)\n")
	buf.WriteString("}\n")
}
