package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type FileDoc struct {
	RelPath         string
	PackageName     string
	AbsPath         string
	LineCount       int
	Imports         []string
	TopLevelDecls   []DeclInfo
	Functions       []*FunctionInfo
	RustCounterpart []string
}

type DeclInfo struct {
	Kind      string
	Names     []string
	StartLine int
	EndLine   int
	Summary   string
	Details   []string
}

type FunctionInfo struct {
	ID              string
	PackageName     string
	FileRelPath     string
	Name            string
	Receiver        string
	ReceiverType    string
	Signature       string
	DocComment      string
	StartLine       int
	EndLine         int
	Effect          string
	CallNames       []string
	CallDisplay     []string
	PotentialLocal  []string
	ExternalCalls   []string
	PossibleCallers []string
	ControlFlow     []string
	Literals        []LiteralInfo
	Statements      []StmtInfo
	SourceLines     []string
}

type LiteralInfo struct {
	Line  int
	Value string
	Kind  string
}

type StmtInfo struct {
	StartLine int
	EndLine   int
	Kind      string
	Summary   string
}

type funcIndex struct {
	ByPackage map[string]map[string][]string
	ByName    map[string][]string
	Defs      map[string]*FunctionInfo
}

func main() {
	rootFlag := flag.String("root", ".", "repository root")
	outFlag := flag.String("out", "seaweed-volume/docs/go-volume-server", "output directory for generated markdown")
	flag.Parse()

	root, err := filepath.Abs(*rootFlag)
	if err != nil {
		fail("resolve root", err)
	}
	outDir := filepath.Join(root, *outFlag)

	paths, err := collectSourceFiles(root)
	if err != nil {
		fail("collect source files", err)
	}

	docs, idx, err := parseFiles(root, paths)
	if err != nil {
		fail("parse source files", err)
	}

	linkCallers(idx)

	if err := os.RemoveAll(outDir); err != nil {
		fail("clear output directory", err)
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		fail("create output directory", err)
	}

	for _, doc := range docs {
		target := filepath.Join(outDir, filepath.FromSlash(doc.RelPath+".md"))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			fail("create doc parent", err)
		}
		content := renderFileDoc(doc)
		if err := os.WriteFile(target, []byte(content), 0o644); err != nil {
			fail("write doc file", err)
		}
	}

	readme := renderIndexReadme(*outFlag, docs)
	if err := os.WriteFile(filepath.Join(outDir, "README.md"), []byte(readme), 0o644); err != nil {
		fail("write index", err)
	}

	fmt.Printf("Generated %d Markdown files under %s\n", len(docs)+1, outDir)
}

func fail(action string, err error) {
	fmt.Fprintf(os.Stderr, "%s: %v\n", action, err)
	os.Exit(1)
}

func collectSourceFiles(root string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			base := filepath.Base(path)
			if base == ".git" || base == "target" || base == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if shouldInclude(rel) {
			files = append(files, rel)
		}
		return nil
	})
	sort.Strings(files)
	return files, err
}

func shouldInclude(rel string) bool {
	switch {
	case rel == "weed/command/volume.go":
		return true
	case rel == "weed/server/common.go":
		return true
	case rel == "weed/server/constants/volume.go":
		return true
	case rel == "weed/server/volume_server_ui/templates.go":
		return true
	case strings.HasPrefix(rel, "weed/server/volume") && strings.HasSuffix(rel, ".go"):
		return true
	case strings.HasPrefix(rel, "weed/storage/"):
		return true
	case strings.HasPrefix(rel, "weed/images/"):
		return true
	case strings.HasPrefix(rel, "weed/security/"):
		return true
	case strings.HasPrefix(rel, "weed/stats/"):
		return true
	default:
		return false
	}
}

func parseFiles(root string, relPaths []string) ([]*FileDoc, *funcIndex, error) {
	fset := token.NewFileSet()
	var docs []*FileDoc
	index := &funcIndex{
		ByPackage: map[string]map[string][]string{},
		ByName:    map[string][]string{},
		Defs:      map[string]*FunctionInfo{},
	}

	for _, rel := range relPaths {
		abs := filepath.Join(root, filepath.FromSlash(rel))
		src, err := os.ReadFile(abs)
		if err != nil {
			return nil, nil, err
		}
		fileAst, err := parser.ParseFile(fset, abs, src, parser.ParseComments)
		if err != nil {
			return nil, nil, err
		}

		lines := splitLines(string(src))
		doc := &FileDoc{
			RelPath:         rel,
			PackageName:     fileAst.Name.Name,
			AbsPath:         abs,
			LineCount:       len(lines),
			Imports:         collectImports(fileAst),
			TopLevelDecls:   collectDecls(fset, fileAst),
			RustCounterpart: rustCounterparts(rel),
		}

		for _, decl := range fileAst.Decls {
			funcDecl, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			info := collectFunctionInfo(fset, rel, fileAst.Name.Name, funcDecl, lines)
			doc.Functions = append(doc.Functions, info)
			index.Defs[info.ID] = info

			if index.ByPackage[info.PackageName] == nil {
				index.ByPackage[info.PackageName] = map[string][]string{}
			}
			index.ByPackage[info.PackageName][info.Name] = append(index.ByPackage[info.PackageName][info.Name], info.ID)
			index.ByName[info.Name] = append(index.ByName[info.Name], info.ID)
		}

		sort.Slice(doc.Functions, func(i, j int) bool {
			return doc.Functions[i].StartLine < doc.Functions[j].StartLine
		})
		docs = append(docs, doc)
	}

	sort.Slice(docs, func(i, j int) bool { return docs[i].RelPath < docs[j].RelPath })
	for _, ids := range index.ByName {
		sort.Strings(ids)
	}
	for _, byName := range index.ByPackage {
		for _, ids := range byName {
			sort.Strings(ids)
		}
	}
	return docs, index, nil
}

func collectImports(fileAst *ast.File) []string {
	var imports []string
	for _, imp := range fileAst.Imports {
		path := strings.Trim(imp.Path.Value, "\"")
		if imp.Name != nil {
			imports = append(imports, imp.Name.Name+" "+path)
		} else {
			imports = append(imports, path)
		}
	}
	sort.Strings(imports)
	return imports
}

func collectDecls(fset *token.FileSet, fileAst *ast.File) []DeclInfo {
	var decls []DeclInfo
	for _, decl := range fileAst.Decls {
		genDecl, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		info := DeclInfo{
			Kind:      strings.ToLower(genDecl.Tok.String()),
			StartLine: fset.Position(genDecl.Pos()).Line,
			EndLine:   fset.Position(genDecl.End()).Line,
		}
		for _, spec := range genDecl.Specs {
			switch s := spec.(type) {
			case *ast.TypeSpec:
				info.Names = append(info.Names, s.Name.Name)
				info.Details = append(info.Details, summarizeTypeSpec(fset, s)...)
			case *ast.ValueSpec:
				for i, name := range s.Names {
					info.Names = append(info.Names, name.Name)
					value := ""
					if i < len(s.Values) {
						value = nodeString(fset, s.Values[i])
					}
					switch {
					case value != "":
						info.Details = append(info.Details, fmt.Sprintf("L%d `%s` = `%s`", fset.Position(name.Pos()).Line, name.Name, sanitizeInline(value)))
					case s.Type != nil:
						info.Details = append(info.Details, fmt.Sprintf("L%d `%s` has declared type `%s`", fset.Position(name.Pos()).Line, name.Name, sanitizeInline(nodeString(fset, s.Type))))
					default:
						info.Details = append(info.Details, fmt.Sprintf("L%d `%s` is declared without an inline initializer", fset.Position(name.Pos()).Line, name.Name))
					}
				}
			}
		}
		if len(info.Names) == 0 {
			info.Names = []string{"<anonymous>"}
		}
		info.Summary = fmt.Sprintf("%s declaration covering %s", info.Kind, strings.Join(info.Names, ", "))
		decls = append(decls, info)
	}
	return decls
}

func summarizeTypeSpec(fset *token.FileSet, spec *ast.TypeSpec) []string {
	var details []string
	switch t := spec.Type.(type) {
	case *ast.StructType:
		for _, field := range t.Fields.List {
			names := []string{"<embedded>"}
			if len(field.Names) > 0 {
				names = nil
				for _, name := range field.Names {
					names = append(names, name.Name)
				}
			}
			line := fset.Position(field.Pos()).Line
			msg := fmt.Sprintf("L%d fields `%s` have type `%s`", line, strings.Join(names, "`, `"), sanitizeInline(nodeString(fset, field.Type)))
			if field.Tag != nil {
				msg += fmt.Sprintf(" with tag `%s`", sanitizeInline(field.Tag.Value))
			}
			details = append(details, msg)
		}
	case *ast.InterfaceType:
		for _, field := range t.Methods.List {
			names := []string{"<embedded>"}
			if len(field.Names) > 0 {
				names = nil
				for _, name := range field.Names {
					names = append(names, name.Name)
				}
			}
			details = append(details, fmt.Sprintf("L%d interface item `%s` has type `%s`", fset.Position(field.Pos()).Line, strings.Join(names, "`, `"), sanitizeInline(nodeString(fset, field.Type))))
		}
	default:
		details = append(details, fmt.Sprintf("L%d `%s` resolves to `%s`", fset.Position(spec.Pos()).Line, spec.Name.Name, sanitizeInline(nodeString(fset, spec.Type))))
	}
	return details
}

func collectFunctionInfo(fset *token.FileSet, relPath, pkgName string, decl *ast.FuncDecl, lines []string) *FunctionInfo {
	startLine := fset.Position(decl.Pos()).Line
	endLine := fset.Position(decl.End()).Line
	if endLine > len(lines) {
		endLine = len(lines)
	}
	sourceLines := make([]string, 0, endLine-startLine+1)
	for i := startLine; i <= endLine; i++ {
		sourceLines = append(sourceLines, lines[i-1])
	}

	info := &FunctionInfo{
		PackageName: pkgName,
		FileRelPath: relPath,
		Name:        decl.Name.Name,
		StartLine:   startLine,
		EndLine:     endLine,
		DocComment:  cleanDocComment(decl.Doc),
		Signature:   buildSignature(fset, decl),
		SourceLines: sourceLines,
	}
	if decl.Recv != nil && len(decl.Recv.List) > 0 {
		field := decl.Recv.List[0]
		info.ReceiverType = nodeString(fset, field.Type)
		if len(field.Names) > 0 {
			info.Receiver = field.Names[0].Name
		}
		info.ID = pkgName + "::" + normalizeReceiverType(info.ReceiverType) + "." + info.Name
	} else {
		info.ID = pkgName + "::" + info.Name
	}

	if decl.Body != nil {
		callNames, callDisplay := collectCalls(fset, decl.Body)
		info.CallNames = callNames
		info.CallDisplay = callDisplay
		info.ControlFlow = collectControlFlow(fset, decl.Body)
		info.Literals = collectLiterals(fset, decl.Body)
		info.Statements = collectStatements(fset, decl.Body)
	}

	info.Effect = deriveEffect(info, decl)
	return info
}

func buildSignature(fset *token.FileSet, decl *ast.FuncDecl) string {
	typeText := sanitizeInline(strings.TrimSpace(nodeString(fset, decl.Type)))
	typeText = strings.TrimPrefix(typeText, "func")
	typeText = strings.TrimSpace(typeText)
	if decl.Recv != nil {
		return fmt.Sprintf("func (%s) %s%s", sanitizeInline(fieldListString(fset, decl.Recv)), decl.Name.Name, typeText)
	}
	return fmt.Sprintf("func %s%s", decl.Name.Name, typeText)
}

func collectCalls(fset *token.FileSet, body *ast.BlockStmt) ([]string, []string) {
	nameSet := map[string]struct{}{}
	displaySet := map[string]struct{}{}
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		name := simpleCallName(call.Fun)
		if name != "" {
			nameSet[name] = struct{}{}
		}
		display := sanitizeInline(nodeString(fset, call.Fun))
		if display != "" {
			displaySet[display] = struct{}{}
		}
		return true
	})
	return sortedKeys(nameSet), sortedKeys(displaySet)
}

func collectControlFlow(fset *token.FileSet, body *ast.BlockStmt) []string {
	var items []string
	ast.Inspect(body, func(n ast.Node) bool {
		switch s := n.(type) {
		case *ast.IfStmt:
			msg := fmt.Sprintf("L%d branches when `%s`", fset.Position(s.Pos()).Line, sanitizeInline(nodeString(fset, s.Cond)))
			if s.Init != nil {
				msg += fmt.Sprintf(" after `%s`", sanitizeInline(nodeString(fset, s.Init)))
			}
			items = append(items, msg)
		case *ast.ForStmt:
			cond := "forever"
			if s.Cond != nil {
				cond = sanitizeInline(nodeString(fset, s.Cond))
			}
			items = append(items, fmt.Sprintf("L%d loops while `%s`", fset.Position(s.Pos()).Line, cond))
		case *ast.RangeStmt:
			items = append(items, fmt.Sprintf("L%d ranges `%s` over `%s`", fset.Position(s.Pos()).Line, sanitizeInline(nodeString(fset, s.Key)), sanitizeInline(nodeString(fset, s.X))))
		case *ast.SwitchStmt:
			tag := "<implicit true>"
			if s.Tag != nil {
				tag = sanitizeInline(nodeString(fset, s.Tag))
			}
			items = append(items, fmt.Sprintf("L%d switches on `%s`", fset.Position(s.Pos()).Line, tag))
		case *ast.TypeSwitchStmt:
			items = append(items, fmt.Sprintf("L%d performs a type switch on `%s`", fset.Position(s.Pos()).Line, sanitizeInline(nodeString(fset, s.Assign))))
		case *ast.SelectStmt:
			items = append(items, fmt.Sprintf("L%d selects across channel cases", fset.Position(s.Pos()).Line))
		case *ast.DeferStmt:
			items = append(items, fmt.Sprintf("L%d defers `%s`", fset.Position(s.Pos()).Line, sanitizeInline(nodeString(fset, s.Call))))
		case *ast.GoStmt:
			items = append(items, fmt.Sprintf("L%d launches goroutine `%s`", fset.Position(s.Pos()).Line, sanitizeInline(nodeString(fset, s.Call))))
		case *ast.ReturnStmt:
			items = append(items, fmt.Sprintf("L%d returns `%s`", fset.Position(s.Pos()).Line, sanitizeInline(joinNodes(fset, s.Results))))
		}
		return true
	})
	return dedupeKeepOrder(items)
}

func collectLiterals(fset *token.FileSet, body *ast.BlockStmt) []LiteralInfo {
	var literals []LiteralInfo
	seen := map[string]struct{}{}
	ast.Inspect(body, func(n ast.Node) bool {
		switch lit := n.(type) {
		case *ast.BasicLit:
			item := LiteralInfo{
				Line:  fset.Position(lit.Pos()).Line,
				Value: lit.Value,
				Kind:  lit.Kind.String(),
			}
			key := fmt.Sprintf("%d|%s|%s", item.Line, item.Kind, item.Value)
			if _, ok := seen[key]; !ok {
				literals = append(literals, item)
				seen[key] = struct{}{}
			}
		case *ast.Ident:
			if lit.Name != "true" && lit.Name != "false" && lit.Name != "nil" {
				return true
			}
			item := LiteralInfo{
				Line:  fset.Position(lit.Pos()).Line,
				Value: lit.Name,
				Kind:  "keyword",
			}
			key := fmt.Sprintf("%d|%s|%s", item.Line, item.Kind, item.Value)
			if _, ok := seen[key]; !ok {
				literals = append(literals, item)
				seen[key] = struct{}{}
			}
		}
		return true
	})
	sort.Slice(literals, func(i, j int) bool {
		if literals[i].Line == literals[j].Line {
			if literals[i].Kind == literals[j].Kind {
				return literals[i].Value < literals[j].Value
			}
			return literals[i].Kind < literals[j].Kind
		}
		return literals[i].Line < literals[j].Line
	})
	return literals
}

func collectStatements(fset *token.FileSet, body *ast.BlockStmt) []StmtInfo {
	var items []StmtInfo
	var walkBlock func([]ast.Stmt)
	walkBlock = func(stmts []ast.Stmt) {
		for _, stmt := range stmts {
			info := summarizeStmt(fset, stmt)
			if info.Kind != "" {
				items = append(items, info)
			}
			switch s := stmt.(type) {
			case *ast.BlockStmt:
				walkBlock(s.List)
			case *ast.IfStmt:
				walkBlock(s.Body.List)
				switch elseNode := s.Else.(type) {
				case *ast.BlockStmt:
					walkBlock(elseNode.List)
				case *ast.IfStmt:
					walkBlock([]ast.Stmt{elseNode})
				}
			case *ast.ForStmt:
				walkBlock(s.Body.List)
			case *ast.RangeStmt:
				walkBlock(s.Body.List)
			case *ast.SwitchStmt:
				for _, stmt := range s.Body.List {
					if clause, ok := stmt.(*ast.CaseClause); ok {
						items = append(items, summarizeStmt(fset, clause))
						walkBlock(clause.Body)
					}
				}
			case *ast.TypeSwitchStmt:
				for _, stmt := range s.Body.List {
					if clause, ok := stmt.(*ast.CaseClause); ok {
						items = append(items, summarizeStmt(fset, clause))
						walkBlock(clause.Body)
					}
				}
			case *ast.SelectStmt:
				for _, stmt := range s.Body.List {
					if clause, ok := stmt.(*ast.CommClause); ok {
						items = append(items, summarizeStmt(fset, clause))
						walkBlock(clause.Body)
					}
				}
			case *ast.LabeledStmt:
				walkBlock([]ast.Stmt{s.Stmt})
			}
		}
	}
	walkBlock(body.List)
	sort.Slice(items, func(i, j int) bool {
		if items[i].StartLine == items[j].StartLine {
			if items[i].EndLine == items[j].EndLine {
				return items[i].Summary < items[j].Summary
			}
			return items[i].EndLine < items[j].EndLine
		}
		return items[i].StartLine < items[j].StartLine
	})
	return items
}

func summarizeStmt(fset *token.FileSet, stmt ast.Stmt) StmtInfo {
	info := StmtInfo{
		StartLine: fset.Position(stmt.Pos()).Line,
		EndLine:   fset.Position(stmt.End()).Line,
	}
	switch s := stmt.(type) {
	case *ast.AssignStmt:
		info.Kind = "assign"
		lhs := sanitizeInline(joinNodes(fset, s.Lhs))
		rhs := sanitizeInline(joinNodes(fset, s.Rhs))
		info.Summary = fmt.Sprintf("assigns `%s` %s `%s`", lhs, s.Tok.String(), rhs)
	case *ast.ExprStmt:
		info.Kind = "expr"
		info.Summary = fmt.Sprintf("executes `%s`", sanitizeInline(nodeString(fset, s.X)))
	case *ast.IfStmt:
		info.Kind = "if"
		info.Summary = fmt.Sprintf("checks `%s`", sanitizeInline(nodeString(fset, s.Cond)))
	case *ast.ForStmt:
		info.Kind = "for"
		cond := "true"
		if s.Cond != nil {
			cond = sanitizeInline(nodeString(fset, s.Cond))
		}
		info.Summary = fmt.Sprintf("loops while `%s`", cond)
	case *ast.RangeStmt:
		info.Kind = "range"
		target := sanitizeInline(nodeString(fset, s.X))
		left := sanitizeInline(joinNodes(fset, []ast.Expr{exprOrBlank(s.Key), exprOrBlank(s.Value)}))
		info.Summary = fmt.Sprintf("ranges `%s` over `%s`", left, target)
	case *ast.ReturnStmt:
		info.Kind = "return"
		info.Summary = fmt.Sprintf("returns `%s`", sanitizeInline(joinNodes(fset, s.Results)))
	case *ast.DeferStmt:
		info.Kind = "defer"
		info.Summary = fmt.Sprintf("defers `%s`", sanitizeInline(nodeString(fset, s.Call)))
	case *ast.GoStmt:
		info.Kind = "go"
		info.Summary = fmt.Sprintf("launches goroutine `%s`", sanitizeInline(nodeString(fset, s.Call)))
	case *ast.SwitchStmt:
		info.Kind = "switch"
		tag := "true"
		if s.Tag != nil {
			tag = sanitizeInline(nodeString(fset, s.Tag))
		}
		info.Summary = fmt.Sprintf("switches on `%s`", tag)
	case *ast.TypeSwitchStmt:
		info.Kind = "type-switch"
		info.Summary = fmt.Sprintf("type-switches on `%s`", sanitizeInline(nodeString(fset, s.Assign)))
	case *ast.SelectStmt:
		info.Kind = "select"
		info.Summary = "selects across channel operations"
	case *ast.CaseClause:
		info.Kind = "case"
		if len(s.List) == 0 {
			info.Summary = "default case"
		} else {
			info.Summary = fmt.Sprintf("case `%s`", sanitizeInline(joinNodes(fset, s.List)))
		}
	case *ast.CommClause:
		info.Kind = "comm"
		if s.Comm == nil {
			info.Summary = "default communication case"
		} else {
			info.Summary = fmt.Sprintf("communication case `%s`", sanitizeInline(nodeString(fset, s.Comm)))
		}
	case *ast.BranchStmt:
		info.Kind = "branch"
		if s.Label != nil {
			info.Summary = fmt.Sprintf("%s to label `%s`", strings.ToLower(s.Tok.String()), s.Label.Name)
		} else {
			info.Summary = strings.ToLower(s.Tok.String())
		}
	case *ast.SendStmt:
		info.Kind = "send"
		info.Summary = fmt.Sprintf("sends `%s` to `%s`", sanitizeInline(nodeString(fset, s.Value)), sanitizeInline(nodeString(fset, s.Chan)))
	case *ast.IncDecStmt:
		info.Kind = "incdec"
		info.Summary = fmt.Sprintf("%s `%s`", strings.ToLower(s.Tok.String()), sanitizeInline(nodeString(fset, s.X)))
	case *ast.DeclStmt:
		info.Kind = "decl"
		info.Summary = fmt.Sprintf("declares `%s`", sanitizeInline(nodeString(fset, s.Decl)))
	case *ast.LabeledStmt:
		info.Kind = "label"
		info.Summary = fmt.Sprintf("label `%s`", s.Label.Name)
	default:
		info.Kind = strings.ToLower(strings.TrimSuffix(strings.TrimPrefix(fmt.Sprintf("%T", stmt), "*ast."), "Stmt"))
		info.Summary = sanitizeInline(nodeString(fset, stmt))
	}
	return info
}

func exprOrBlank(expr ast.Expr) ast.Expr {
	if expr == nil {
		return &ast.Ident{Name: "_"}
	}
	return expr
}

func deriveEffect(info *FunctionInfo, decl *ast.FuncDecl) string {
	if info.DocComment != "" {
		return info.DocComment
	}
	name := info.Name
	switch {
	case strings.HasPrefix(name, "New"):
		return fmt.Sprintf("Constructs and returns `%s`-related state.", strings.TrimPrefix(name, "New"))
	case strings.HasPrefix(name, "Get"):
		return "Retrieves or serves the requested resource and returns the outcome."
	case strings.HasPrefix(name, "Read"):
		return "Reads storage or request data and converts it into the function's return or streamed response."
	case strings.HasPrefix(name, "Write"):
		return "Writes state, file data, or response output and reports the result."
	case strings.HasPrefix(name, "Delete"):
		return "Deletes the targeted state or storage entries and returns status."
	case strings.HasPrefix(name, "Update"):
		return "Updates existing state in place, usually based on request or runtime conditions."
	case strings.HasPrefix(name, "Load"):
		return "Loads persisted state or configuration into runtime structures."
	case strings.HasPrefix(name, "Save"):
		return "Persists runtime state or derived data."
	case strings.HasPrefix(name, "parse"), strings.HasPrefix(name, "Parse"):
		return "Parses inbound text or binary input into structured values."
	case strings.HasSuffix(name, "Handler"):
		return "Handles an HTTP endpoint and writes the response side effects directly."
	case strings.Contains(name, "Heartbeat"):
		return "Maintains master/volume heartbeat state and its side effects."
	case strings.Contains(name, "Vacuum"):
		return "Runs or coordinates vacuum/compaction related work."
	case strings.Contains(name, "Copy"):
		return "Copies data between storage locations or peer volume servers."
	case strings.Contains(name, "Scrub"):
		return "Validates stored data and surfaces corruption or mismatch details."
	case strings.Contains(name, "Mount"):
		return "Attaches runtime-visible storage or shard state."
	case strings.Contains(name, "Unmount"):
		return "Detaches runtime-visible storage or shard state."
	case strings.Contains(name, "Needle"):
		return "Manipulates or transports SeaweedFS needle state."
	default:
		if info.ReceiverType != "" {
			return fmt.Sprintf("Implements `%s` behavior on receiver `%s`.", name, sanitizeInline(info.ReceiverType))
		}
		return fmt.Sprintf("Implements `%s` for package `%s`.", name, info.PackageName)
	}
}

func linkCallers(idx *funcIndex) {
	for _, fn := range idx.Defs {
		var local []string
		var external []string
		for _, display := range fn.CallDisplay {
			simple := simpleNameFromDisplay(display)
			if ids, ok := idx.ByPackage[fn.PackageName][simple]; ok && len(ids) > 0 {
				local = append(local, display)
			} else {
				external = append(external, display)
			}
		}
		fn.PotentialLocal = dedupeKeepOrder(local)
		fn.ExternalCalls = dedupeKeepOrder(external)

		var callers []string
		if ids, ok := idx.ByName[fn.Name]; ok {
			for _, candidateID := range ids {
				if candidateID == fn.ID {
					continue
				}
				candidate := idx.Defs[candidateID]
				for _, callName := range candidate.CallNames {
					if callName == fn.Name {
						callers = append(callers, candidateID)
						break
					}
				}
			}
		}
		sort.Strings(callers)
		fn.PossibleCallers = callers
	}
}

func renderFileDoc(doc *FileDoc) string {
	var b strings.Builder
	b.WriteString("# " + doc.RelPath + "\n\n")
	b.WriteString("- Source file: `" + doc.RelPath + "`\n")
	b.WriteString("- Package: `" + doc.PackageName + "`\n")
	b.WriteString(fmt.Sprintf("- Total lines: `%d`\n", doc.LineCount))
	if len(doc.RustCounterpart) > 0 {
		b.WriteString("- Rust counterpart candidates: `" + strings.Join(doc.RustCounterpart, "`, `") + "`\n")
	} else {
		b.WriteString("- Rust counterpart candidates: none mapped directly; behavior may still be folded into adjacent Rust modules.\n")
	}
	b.WriteString("\n## Imports\n\n")
	if len(doc.Imports) == 0 {
		b.WriteString("This file has no imports.\n")
	} else {
		for _, imp := range doc.Imports {
			b.WriteString("- `" + imp + "`\n")
		}
	}

	b.WriteString("\n## Top-Level Declarations\n\n")
	if len(doc.TopLevelDecls) == 0 {
		b.WriteString("No package-level const/var/type declarations in this file.\n")
	} else {
		for _, decl := range doc.TopLevelDecls {
			b.WriteString(fmt.Sprintf("### `%s` `%s`\n\n", decl.Kind, strings.Join(decl.Names, "`, `")))
			b.WriteString(fmt.Sprintf("- Lines: `%d-%d`\n", decl.StartLine, decl.EndLine))
			b.WriteString("- Role: " + decl.Summary + "\n")
			if len(decl.Details) > 0 {
				b.WriteString("- Details:\n")
				for _, detail := range decl.Details {
					b.WriteString("  - " + detail + "\n")
				}
			}
			b.WriteString("\n")
		}
	}

	b.WriteString("## Function Inventory\n\n")
	if len(doc.Functions) == 0 {
		b.WriteString("No functions or methods are declared in this file.\n")
		return b.String()
	}
	for _, fn := range doc.Functions {
		receiver := ""
		if fn.ReceiverType != "" {
			receiver = " receiver `" + sanitizeInline(fn.ReceiverType) + "`"
		}
		b.WriteString(fmt.Sprintf("- `%s`%s at lines `%d-%d`\n", fn.Name, receiver, fn.StartLine, fn.EndLine))
	}

	for _, fn := range doc.Functions {
		b.WriteString("\n## `" + fn.Name + "`\n\n")
		b.WriteString("- Signature: `" + fn.Signature + "`\n")
		b.WriteString(fmt.Sprintf("- Lines: `%d-%d`\n", fn.StartLine, fn.EndLine))
		if fn.ReceiverType != "" {
			b.WriteString("- Receiver: `" + sanitizeInline(fn.ReceiverType) + "`")
			if fn.Receiver != "" {
				b.WriteString(fmt.Sprintf(" bound as `%s`", fn.Receiver))
			}
			b.WriteString("\n")
		}
		b.WriteString("- Effect: " + fn.Effect + "\n")
		if fn.DocComment != "" {
			b.WriteString("- Native doc comment: `" + sanitizeInline(fn.DocComment) + "`\n")
		}

		b.WriteString("\n### Relations\n\n")
		if len(fn.PotentialLocal) > 0 {
			b.WriteString("- Local package calls: `" + strings.Join(fn.PotentialLocal, "`, `") + "`\n")
		} else {
			b.WriteString("- Local package calls: none detected from simple call-name matching.\n")
		}
		if len(fn.ExternalCalls) > 0 {
			b.WriteString("- External or unresolved calls: `" + strings.Join(fn.ExternalCalls, "`, `") + "`\n")
		} else {
			b.WriteString("- External or unresolved calls: none detected.\n")
		}
		if len(fn.PossibleCallers) > 0 {
			b.WriteString("- Possible name-matched callers in scanned scope: `" + strings.Join(fn.PossibleCallers, "`, `") + "`\n")
		} else {
			b.WriteString("- Possible name-matched callers in scanned scope: none detected.\n")
		}

		b.WriteString("\n### Control Flow\n\n")
		if len(fn.ControlFlow) == 0 {
			b.WriteString("No notable branch/loop/defer/return items were extracted.\n")
		} else {
			for _, item := range fn.ControlFlow {
				b.WriteString("- " + item + "\n")
			}
		}

		b.WriteString("\n### Literal And Keyword Touchpoints\n\n")
		if len(fn.Literals) == 0 {
			b.WriteString("No literals or keyword literals (`true`, `false`, `nil`) were extracted from the body.\n")
		} else {
			for _, lit := range fn.Literals {
				b.WriteString(fmt.Sprintf("- L%d `%s` = `%s`\n", lit.Line, lit.Kind, sanitizeInline(lit.Value)))
			}
		}

		b.WriteString("\n### Line-Level Operating Logic\n\n")
		lineNotes := lineNoteMap(fn.Statements)
		for offset, raw := range fn.SourceLines {
			lineNo := fn.StartLine + offset
			trimmed := strings.TrimSpace(raw)
			if trimmed == "" {
				continue
			}
			note := explainLine(trimmed, lineNo, lineNotes)
			b.WriteString(fmt.Sprintf("- L%d: `%s`", lineNo, sanitizeInline(trimmed)))
			if note != "" {
				b.WriteString(" -> " + note)
			}
			b.WriteString("\n")
		}
	}

	return b.String()
}

func renderIndexReadme(outRel string, docs []*FileDoc) string {
	var b strings.Builder
	b.WriteString("# Go Volume Server Translation Docs\n\n")
	b.WriteString("Generated reference set for translating the Go SeaweedFS volume server into the Rust `seaweed-volume` crate.\n\n")
	b.WriteString("- Generated at: `" + time.Now().Format(time.RFC3339) + "`\n")
	b.WriteString(fmt.Sprintf("- Markdown files: `%d`\n", len(docs)))
	b.WriteString("- Scope: `weed/command/volume.go`, selected `weed/server` volume-server files, and runtime files under `weed/storage`, `weed/images`, `weed/security`, and `weed/stats`.\n")
	b.WriteString("- Output root: `" + sanitizeInline(outRel) + "`\n\n")

	groups := map[string][]*FileDoc{}
	groupOrder := []string{"command", "server", "storage", "images", "security", "stats"}
	for _, doc := range docs {
		group := strings.Split(doc.RelPath, "/")[1]
		groups[group] = append(groups[group], doc)
	}

	for _, group := range groupOrder {
		items := groups[group]
		if len(items) == 0 {
			continue
		}
		sort.Slice(items, func(i, j int) bool { return items[i].RelPath < items[j].RelPath })
		b.WriteString("## " + strings.Title(group) + "\n\n")
		for _, doc := range items {
			target := filepath.ToSlash(filepath.Join(outRel, doc.RelPath+".md"))
			b.WriteString("- `" + doc.RelPath + "` -> `" + target + "`")
			if len(doc.RustCounterpart) > 0 {
				b.WriteString(" | Rust: `" + strings.Join(doc.RustCounterpart, "`, `") + "`")
			}
			b.WriteString("\n")
		}
		b.WriteString("\n")
	}
	return b.String()
}

func lineNoteMap(statements []StmtInfo) map[int]string {
	notes := map[int]string{}
	for _, stmt := range statements {
		if stmt.Summary != "" {
			notes[stmt.StartLine] = stmt.Summary
		}
	}
	return notes
}

func explainLine(line string, lineNo int, notes map[int]string) string {
	if note, ok := notes[lineNo]; ok {
		return note
	}
	switch {
	case strings.HasPrefix(line, "func "):
		return "function signature header"
	case strings.HasPrefix(line, "//"):
		return "comment line"
	case strings.HasPrefix(line, "/*") || strings.HasPrefix(line, "*/"):
		return "comment block boundary"
	case line == "{" || line == "}" || line == "})" || line == "};":
		return "block boundary"
	case strings.HasPrefix(line, "else"):
		return "alternate control-flow branch"
	case strings.HasPrefix(line, "case "):
		return "switch/select case label"
	case strings.HasPrefix(line, "default:"):
		return "default case label"
	case strings.HasPrefix(line, "return"):
		return "returns from the function"
	case strings.HasPrefix(line, "defer "):
		return "registers deferred work for function exit"
	case strings.HasPrefix(line, "go "):
		return "starts a goroutine"
	case strings.HasPrefix(line, "if "):
		return "conditional check"
	case strings.HasPrefix(line, "for "):
		return "loop header"
	case strings.HasPrefix(line, "switch "):
		return "switch header"
	case strings.HasPrefix(line, "select "):
		return "channel select header"
	case strings.Contains(line, ":="):
		return "declares and assigns local state"
	case looksLikeAssignment(line):
		return "updates existing state"
	case strings.HasSuffix(line, ")") || strings.HasSuffix(line, "},") || strings.HasSuffix(line, "})"):
		return "executes a call or composite literal line"
	default:
		return "continuation or structural line"
	}
}

func looksLikeAssignment(line string) bool {
	if strings.Contains(line, "==") || strings.Contains(line, ">=") || strings.Contains(line, "<=") || strings.Contains(line, "!=") {
		return false
	}
	if strings.Contains(line, "=") {
		return true
	}
	return false
}

func rustCounterparts(rel string) []string {
	switch {
	case rel == "weed/command/volume.go":
		return []string{"seaweed-volume/src/config.rs", "seaweed-volume/src/main.rs"}
	case strings.HasPrefix(rel, "weed/images/"):
		return []string{"seaweed-volume/src/images.rs"}
	case strings.HasPrefix(rel, "weed/security/"):
		return []string{"seaweed-volume/src/security.rs"}
	case strings.HasPrefix(rel, "weed/stats/"):
		return []string{"seaweed-volume/src/metrics.rs"}
	case rel == "weed/server/common.go":
		return []string{"seaweed-volume/src/server/handlers.rs", "seaweed-volume/src/server/volume_server.rs", "seaweed-volume/src/main.rs"}
	case rel == "weed/server/constants/volume.go":
		return []string{"seaweed-volume/src/server/mod.rs", "seaweed-volume/src/server/volume_server.rs"}
	case rel == "weed/server/volume_server.go":
		return []string{"seaweed-volume/src/server/volume_server.rs", "seaweed-volume/src/server/heartbeat.rs", "seaweed-volume/src/main.rs"}
	case strings.HasPrefix(rel, "weed/server/volume_server_handlers"):
		return []string{"seaweed-volume/src/server/handlers.rs", "seaweed-volume/src/server/volume_server.rs"}
	case strings.HasPrefix(rel, "weed/server/volume_grpc_"):
		return []string{"seaweed-volume/src/server/grpc_server.rs", "seaweed-volume/src/server/heartbeat.rs"}
	case rel == "weed/server/volume_server_ui/templates.go":
		return []string{"seaweed-volume/src/server/volume_server.rs"}
	case rel == "weed/storage/disk_location.go" || rel == "weed/storage/disk_location_ec.go":
		return []string{"seaweed-volume/src/storage/disk_location.rs"}
	case strings.HasPrefix(rel, "weed/storage/erasure_coding/"):
		name := filepath.Base(rel)
		switch name {
		case "ec_decoder.go":
			return []string{"seaweed-volume/src/storage/erasure_coding/ec_decoder.rs"}
		case "ec_encoder.go":
			return []string{"seaweed-volume/src/storage/erasure_coding/ec_encoder.rs"}
		case "ec_locate.go":
			return []string{"seaweed-volume/src/storage/erasure_coding/ec_locate.rs"}
		case "ec_shard.go", "ec_shard_info.go", "ec_shards_info.go":
			return []string{"seaweed-volume/src/storage/erasure_coding/ec_shard.rs"}
		default:
			return []string{"seaweed-volume/src/storage/erasure_coding/ec_volume.rs", "seaweed-volume/src/storage/erasure_coding/mod.rs"}
		}
	case strings.HasPrefix(rel, "weed/storage/needle/"):
		name := filepath.Base(rel)
		switch name {
		case "crc.go":
			return []string{"seaweed-volume/src/storage/needle/crc.rs"}
		case "volume_ttl.go":
			return []string{"seaweed-volume/src/storage/needle/ttl.rs"}
		default:
			return []string{"seaweed-volume/src/storage/needle/needle.rs", "seaweed-volume/src/storage/needle/mod.rs"}
		}
	case rel == "weed/storage/needle_map.go" || strings.HasPrefix(rel, "weed/storage/needle_map/") || strings.HasPrefix(rel, "weed/storage/needle_map_"):
		name := filepath.Base(rel)
		if strings.Contains(name, "compact_map") {
			return []string{"seaweed-volume/src/storage/needle_map/compact_map.rs"}
		}
		return []string{"seaweed-volume/src/storage/needle_map.rs"}
	case strings.HasPrefix(rel, "weed/storage/store"):
		return []string{"seaweed-volume/src/storage/store.rs"}
	case strings.HasPrefix(rel, "weed/storage/super_block/"):
		return []string{"seaweed-volume/src/storage/super_block.rs"}
	case strings.HasPrefix(rel, "weed/storage/types/"):
		return []string{"seaweed-volume/src/storage/types.rs"}
	case strings.HasPrefix(rel, "weed/storage/backend/s3_backend/"):
		return []string{"seaweed-volume/src/remote_storage/s3.rs", "seaweed-volume/src/remote_storage/s3_tier.rs"}
	case strings.HasPrefix(rel, "weed/storage/backend/"):
		return []string{"seaweed-volume/src/storage/volume.rs", "seaweed-volume/src/storage/mod.rs"}
	case strings.HasPrefix(rel, "weed/storage/idx/"):
		return []string{"seaweed-volume/src/storage/idx/mod.rs"}
	case strings.HasPrefix(rel, "weed/storage/volume"):
		return []string{"seaweed-volume/src/storage/volume.rs"}
	case strings.HasPrefix(rel, "weed/storage/"):
		return []string{"seaweed-volume/src/storage/mod.rs"}
	default:
		return nil
	}
}

func cleanDocComment(group *ast.CommentGroup) string {
	if group == nil {
		return ""
	}
	text := strings.TrimSpace(group.Text())
	return strings.Join(strings.Fields(text), " ")
}

func nodeString(fset *token.FileSet, node any) string {
	if node == nil {
		return ""
	}
	var buf bytes.Buffer
	if err := printer.Fprint(&buf, fset, node); err != nil {
		return ""
	}
	return buf.String()
}

func fieldListString(fset *token.FileSet, fields *ast.FieldList) string {
	if fields == nil {
		return ""
	}
	var parts []string
	for _, field := range fields.List {
		names := make([]string, 0, len(field.Names))
		for _, name := range field.Names {
			names = append(names, name.Name)
		}
		typeText := sanitizeInline(nodeString(fset, field.Type))
		if len(names) == 0 {
			parts = append(parts, typeText)
			continue
		}
		parts = append(parts, strings.Join(names, ", ")+" "+typeText)
	}
	return strings.Join(parts, ", ")
}

func joinNodes(fset *token.FileSet, nodes []ast.Expr) string {
	parts := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if node == nil {
			continue
		}
		text := nodeString(fset, node)
		if text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, ", ")
}

func splitLines(src string) []string {
	src = strings.ReplaceAll(src, "\r\n", "\n")
	src = strings.ReplaceAll(src, "\r", "\n")
	return strings.Split(src, "\n")
}

func normalizeReceiverType(receiver string) string {
	receiver = strings.TrimPrefix(receiver, "*")
	receiver = strings.ReplaceAll(receiver, " ", "")
	return receiver
}

func simpleCallName(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		return e.Sel.Name
	case *ast.IndexExpr:
		return simpleCallName(e.X)
	case *ast.IndexListExpr:
		return simpleCallName(e.X)
	case *ast.ParenExpr:
		return simpleCallName(e.X)
	default:
		return ""
	}
}

func simpleNameFromDisplay(display string) string {
	if strings.Contains(display, ".") {
		parts := strings.Split(display, ".")
		return parts[len(parts)-1]
	}
	if strings.Contains(display, "(") {
		return strings.TrimSpace(strings.Split(display, "(")[0])
	}
	return display
}

func sortedKeys(set map[string]struct{}) []string {
	items := make([]string, 0, len(set))
	for key := range set {
		items = append(items, key)
	}
	sort.Strings(items)
	return items
}

func dedupeKeepOrder(items []string) []string {
	var out []string
	seen := map[string]struct{}{}
	for _, item := range items {
		if _, ok := seen[item]; ok {
			continue
		}
		out = append(out, item)
		seen[item] = struct{}{}
	}
	return out
}

func sanitizeInline(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.Join(strings.Fields(s), " ")
	s = strings.ReplaceAll(s, "`", "'")
	return s
}
