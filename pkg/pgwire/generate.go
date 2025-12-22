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

	// Generate the output
	output := generateCode(pkgName, imports, *from, *typePrefix, inputType, types)

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
		imports = append(imports, buf.String())
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

func generateCode(pkgName string, imports []string, from, prefix, inputType string, types []typeInfo) []byte {
	var buf bytes.Buffer

	// Header
	buf.WriteString("// Code generated by generate.go; DO NOT EDIT.\n\n")
	fmt.Fprintf(&buf, "package %s\n\n", pkgName)

	// Imports
	if len(imports) > 0 {
		buf.WriteString("import (\n")
		for _, imp := range imports {
			fmt.Fprintf(&buf, "\t%s\n", imp)
		}
		buf.WriteString(")\n\n")
	}

	// Interface
	interfaceName := from + prefix
	fmt.Fprintf(&buf, "// %s is implemented by all %s %s message wrapper types.\n", interfaceName, from, prefix)
	fmt.Fprintf(&buf, "type %s interface {\n", interfaceName)
	fmt.Fprintf(&buf, "\t%s()\n", from)
	fmt.Fprintf(&buf, "\t%s()\n", prefix)
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

		// Marker methods for interface satisfaction
		fmt.Fprintf(&buf, "func (%s) %s() {}\n", newTypeName, from)
		fmt.Fprintf(&buf, "func (%s) %s() {}\n\n", newTypeName, prefix)
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
	buf.WriteString("}\n")

	// Format the code
	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		log.Printf("warning: generated code has formatting issues: %v", err)
		return buf.Bytes()
	}
	return formatted
}
