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
	"strings"
)

var from = flag.String("from", "", "the origin of the message: Frontend or Backend")
var typePrefix = flag.String("type", "", "the type prefix for generated types")
var fn = flag.String("fn", "", "the function name containing the type switch")
var methods = flag.String("methods", "", "comma-separated list of additional no-arg methods to generate")

// returnFlags collects multiple -return flags
var returnFlags returnFlagList

// returnSpec defines a method that returns a value
type returnSpec struct {
	name       string // method name
	returnType string // return type
	expr       string // expression to return (use 't' for the receiver)
}

// returnFlagList implements flag.Value for collecting multiple -return flags
type returnFlagList []returnSpec

func (r *returnFlagList) String() string {
	return fmt.Sprintf("%v", *r)
}

func (r *returnFlagList) Set(value string) error {
	// Parse: name=returnType=expr
	parts := strings.SplitN(value, "=", 3)
	if len(parts) != 3 {
		return fmt.Errorf("invalid -return format: expected name=returnType=expr, got %q", value)
	}
	*r = append(*r, returnSpec{
		name:       parts[0],
		returnType: parts[1],
		expr:       parts[2],
	})
	return nil
}

func init() {
	flag.Var(&returnFlags, "return", "method with return value: name=returnType=expr (can be specified multiple times)")
}

func main() {
	flag.Parse()

	if *from == "" {
		log.Fatal("-from is required (Frontend or Backend)")
	}
	if *typePrefix == "" {
		log.Fatal("-type is required")
	}
	if *fn == "" {
		log.Fatal("-fn is required")
	}

	// Get the source file from GOFILE env var (set by go generate)
	gofile := os.Getenv("GOFILE")
	if gofile == "" {
		gofile = "messages.go"
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

	// Find the function
	var targetFunc *ast.FuncDecl
	for _, decl := range file.Decls {
		if funcDecl, ok := decl.(*ast.FuncDecl); ok {
			if funcDecl.Name.Name == *fn {
				targetFunc = funcDecl
				break
			}
		}
	}
	if targetFunc == nil {
		log.Fatalf("function %s not found in %s", *fn, gofile)
	}

	// Extract the input parameter type from the function signature
	var inputType string
	if targetFunc.Type.Params != nil && len(targetFunc.Type.Params.List) > 0 {
		param := targetFunc.Type.Params.List[0]
		var buf bytes.Buffer
		printer.Fprint(&buf, fset, param.Type)
		inputType = buf.String()
	}
	if inputType == "" {
		log.Fatalf("function %s has no parameters", *fn)
	}

	// Find the type switch in the function body
	var typeSwitch *ast.TypeSwitchStmt
	ast.Inspect(targetFunc.Body, func(n ast.Node) bool {
		if ts, ok := n.(*ast.TypeSwitchStmt); ok {
			typeSwitch = ts
			return false
		}
		return true
	})
	if typeSwitch == nil {
		log.Fatalf("no type switch found in function %s", *fn)
	}

	// Extract types from the switch cases
	var types []typeInfo
	caseClauses := typeSwitch.Body.List
	for i, stmt := range caseClauses {
		caseClause, ok := stmt.(*ast.CaseClause)
		if !ok {
			continue
		}
		// Skip default case (List is nil)
		if caseClause.List == nil {
			continue
		}

		// Find the end boundary for comment extraction
		// (either the next case or the end of the switch body)
		var nextCasePos token.Pos
		if i+1 < len(caseClauses) {
			nextCasePos = caseClauses[i+1].Pos()
		} else {
			nextCasePos = typeSwitch.Body.Rbrace
		}

		// Extract comments from the case body
		comments := extractCaseComments(file, caseClause.Colon, nextCasePos)

		for _, expr := range caseClause.List {
			ti := extractTypeInfo(expr)
			if ti.shortName != "" {
				ti.comments = comments
				types = append(types, ti)
			}
		}
	}

	if len(types) == 0 {
		log.Fatalf("no types found in type switch in function %s", *fn)
	}

	// Parse additional methods
	var extraMethods []string
	if *methods != "" {
		extraMethods = strings.Split(*methods, ",")
	}

	// Generate the output
	output := generateCode(pkgName, imports, *from, *typePrefix, inputType, extraMethods, returnFlags, types)

	// Write to output file
	outFile := toSnakeCase(*from) + "_" + toSnakeCase(*typePrefix) + ".go"
	outPath := filepath.Join(filepath.Dir(gofile), outFile)
	if err := os.WriteFile(outPath, output, 0644); err != nil {
		log.Fatalf("failed to write %s: %v", outPath, err)
	}

	fmt.Printf("Generated %s with %d types\n", outFile, len(types))
}

type typeInfo struct {
	// The full qualified type as it appears in code, e.g. "*pgproto3.GSSEncRequest"
	qualified string
	// Just the type name without package, e.g. "GSSEncRequest"
	shortName string
	// Comments from the case clause body
	comments []string
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
		// Only include imports that are likely used in the generated code
		// (imports containing pgproto3 which defines the message types)
		if strings.Contains(impStr, "pgproto3") {
			imports = append(imports, impStr)
		}
	}
	return imports
}

func extractCaseComments(file *ast.File, colonPos, nextCasePos token.Pos) []string {
	var comments []string

	for _, cg := range file.Comments {
		// Comments must be after the colon and before the next case
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

	// Handle pointer types
	if star, ok := expr.(*ast.StarExpr); ok {
		inner := extractTypeInfo(star.X)
		ti.qualified = "*" + inner.qualified
		ti.shortName = inner.shortName
		return ti
	}

	// Handle selector expressions (pkg.Type)
	if sel, ok := expr.(*ast.SelectorExpr); ok {
		if ident, ok := sel.X.(*ast.Ident); ok {
			ti.qualified = ident.Name + "." + sel.Sel.Name
			ti.shortName = sel.Sel.Name
			return ti
		}
	}

	// Handle simple identifiers
	if ident, ok := expr.(*ast.Ident); ok {
		ti.qualified = ident.Name
		ti.shortName = ident.Name
		return ti
	}

	return ti
}

func generateCode(pkgName string, imports []string, from, prefix, inputType string, extraMethods []string, returnMethods []returnSpec, types []typeInfo) []byte {
	var buf bytes.Buffer

	// Header
	buf.WriteString("// Code generated by generate.go; DO NOT EDIT.\n\n")
	fmt.Fprintf(&buf, "package %s\n\n", pkgName)

	// Imports
	buf.WriteString("import (\n")
	buf.WriteString("\t\"fmt\"\n\n")
	for _, imp := range imports {
		fmt.Fprintf(&buf, "\t%s\n", imp)
	}
	buf.WriteString(")\n\n")

	// Build set of methods with return values (to avoid generating void versions)
	returnMethodNames := make(map[string]bool)
	for _, rm := range returnMethods {
		returnMethodNames[rm.name] = true
	}

	// Interface
	interfaceName := from + prefix
	fmt.Fprintf(&buf, "// %s is implemented by all %s %s message wrapper types.\n", interfaceName, from, prefix)
	fmt.Fprintf(&buf, "type %s interface {\n", interfaceName)
	// Only generate void method if not overridden by a return method
	if !returnMethodNames[from] {
		fmt.Fprintf(&buf, "\t%s()\n", from)
	}
	if !returnMethodNames[prefix] {
		fmt.Fprintf(&buf, "\t%s()\n", prefix)
	}
	for _, method := range extraMethods {
		if !returnMethodNames[method] {
			fmt.Fprintf(&buf, "\t%s()\n", method)
		}
	}
	for _, rm := range returnMethods {
		fmt.Fprintf(&buf, "\t%s() %s\n", rm.name, rm.returnType)
	}
	buf.WriteString("}\n\n")

	// Compile-time interface checks
	buf.WriteString("// Compile-time checks that all wrapper types implement the interface.\n")
	buf.WriteString("var (\n")
	for _, ti := range types {
		newTypeName := from + prefix + ti.shortName
		fmt.Fprintf(&buf, "\t_ %s = %s{}\n", interfaceName, newTypeName)
	}
	buf.WriteString(")\n\n")

	// Type definitions and methods
	for _, ti := range types {
		newTypeName := from + prefix + ti.shortName

		// Write comments from the source case clause
		if len(ti.comments) > 0 {
			for _, comment := range ti.comments {
				fmt.Fprintf(&buf, "// %s\n", comment)
			}
		} else {
			// Fallback comment if no source comments
			fmt.Fprintf(&buf, "// %s wraps %s from the %s.\n", newTypeName, ti.qualified, strings.ToLower(from))
		}

		// Type definition (defined type, not alias, so we can add methods)
		fmt.Fprintf(&buf, "type %s From%s[%s]\n\n", newTypeName, from, ti.qualified)

		// Marker methods for interface satisfaction (skip if overridden by return method)
		if !returnMethodNames[from] {
			fmt.Fprintf(&buf, "func (%s) %s() {}\n", newTypeName, from)
		}
		if !returnMethodNames[prefix] {
			fmt.Fprintf(&buf, "func (%s) %s() {}\n", newTypeName, prefix)
		}
		for _, method := range extraMethods {
			if !returnMethodNames[method] {
				fmt.Fprintf(&buf, "func (%s) %s() {}\n", newTypeName, method)
			}
		}
		// Methods with return values
		for _, rm := range returnMethods {
			fmt.Fprintf(&buf, "func (t %s) %s() %s { return %s }\n", newTypeName, rm.name, rm.returnType, rm.expr)
		}
		buf.WriteString("\n")
	}

	// Conversion function: To<From><Prefix>(inputType) -> (interface, bool)
	funcName := "To" + from + prefix
	fmt.Fprintf(&buf, "// %s converts a %s to a %s if it matches one of the known types.\n", funcName, inputType, interfaceName)
	fmt.Fprintf(&buf, "func %s(msg %s) (%s, bool) {\n", funcName, inputType, interfaceName)
	buf.WriteString("\tswitch m := msg.(type) {\n")
	for _, ti := range types {
		newTypeName := from + prefix + ti.shortName
		fmt.Fprintf(&buf, "\tcase %s:\n", ti.qualified)
		fmt.Fprintf(&buf, "\t\treturn %s{m}, true\n", newTypeName)
	}
	buf.WriteString("\t}\n")
	buf.WriteString("\treturn nil, false\n")
	buf.WriteString("}\n\n")

	// Handler struct: <From><Prefix>Handlers[T any]
	handlersName := from + prefix + "Handlers"
	fmt.Fprintf(&buf, "// %s provides type-safe handlers for each %s variant.\n", handlersName, interfaceName)
	fmt.Fprintf(&buf, "type %s[T any] struct {\n", handlersName)
	for _, ti := range types {
		newTypeName := from + prefix + ti.shortName
		fmt.Fprintf(&buf, "\t%s func(msg %s) (T, error)\n", ti.shortName, newTypeName)
	}
	buf.WriteString("}\n\n")

	// HandleDefault method
	fmt.Fprintf(&buf, "// HandleDefault dispatches to the appropriate handler, or calls defaultHandler if the handler is nil.\n")
	fmt.Fprintf(&buf, "func (h %s[T]) HandleDefault(msg %s, defaultHandler func(msg %s) (T, error)) (r T, err error) {\n", handlersName, interfaceName, interfaceName)
	buf.WriteString("\tswitch msg := msg.(type) {\n")
	for _, ti := range types {
		newTypeName := from + prefix + ti.shortName
		fmt.Fprintf(&buf, "\tcase %s:\n", newTypeName)
		fmt.Fprintf(&buf, "\t\tif h.%s != nil {\n", ti.shortName)
		fmt.Fprintf(&buf, "\t\t\treturn h.%s(msg)\n", ti.shortName)
		buf.WriteString("\t\t} else {\n")
		buf.WriteString("\t\t\treturn defaultHandler(msg)\n")
		buf.WriteString("\t\t}\n")
	}
	buf.WriteString("\t}\n")
	fmt.Fprintf(&buf, "\terr = fmt.Errorf(\"unknown %s message: %%T\", msg)\n", strings.ToLower(from+" "+prefix))
	buf.WriteString("\treturn\n")
	buf.WriteString("}\n\n")

	// Handle method (panics on unhandled)
	fmt.Fprintf(&buf, "// Handle dispatches to the appropriate handler, or panics if the handler is nil.\n")
	fmt.Fprintf(&buf, "func (h %s[T]) Handle(msg %s) (T, error) {\n", handlersName, interfaceName)
	fmt.Fprintf(&buf, "\treturn h.HandleDefault(msg, func(msg %s) (T, error) {\n", interfaceName)
	fmt.Fprintf(&buf, "\t\tpanic(fmt.Sprintf(\"no handler defined for %s message: %%T\", msg))\n", strings.ToLower(from+" "+prefix))
	buf.WriteString("\t})\n")
	buf.WriteString("}\n")

	// Format the code
	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		log.Printf("warning: generated code has formatting issues: %v", err)
		return buf.Bytes()
	}
	return formatted
}
