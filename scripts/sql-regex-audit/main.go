// Command sql-regex-audit records regexp call sites in Go source.
//
// It intentionally uses only the standard library so the audit can run before
// any dependency setup. The output is a stable JSON inventory for review; it
// does not decide whether a call should be migrated.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type site struct {
	File     string `json:"file"`
	Line     int    `json:"line"`
	API      string `json:"api"`
	Function string `json:"function"`
	Category string `json:"category"`
}

type inventory struct {
	Root        string   `json:"root"`
	GoFiles     int      `json:"go_files"`
	ImportFiles []string `json:"regexp_import_files"`
	Sites       []site   `json:"sites"`
	ParseErrors []string `json:"parse_errors"`
	SkippedDirs []string `json:"skipped_dirs"`
}

func main() {
	root := flag.String("root", ".", "repository root to scan")
	output := flag.String("output", "", "JSON output path; stdout when empty")
	exclude := flag.String("exclude", "", "comma-separated directory names to exclude")
	flag.Parse()
	excluded := make(map[string]struct{})
	for _, name := range strings.Split(*exclude, ",") {
		if name = strings.TrimSpace(name); name != "" {
			excluded[name] = struct{}{}
		}
	}

	rootPath, err := filepath.Abs(*root)
	if err != nil {
		fatal(err)
	}
	result := inventory{Root: rootPath}
	fset := token.NewFileSet()

	err = filepath.WalkDir(rootPath, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if _, skip := excluded[entry.Name()]; skip {
				result.SkippedDirs = append(result.SkippedDirs, relPath(rootPath, path))
				return filepath.SkipDir
			}
			if path != rootPath && (strings.HasPrefix(entry.Name(), ".") || entry.Name() == "vendor") {
				result.SkippedDirs = append(result.SkippedDirs, relPath(rootPath, path))
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(entry.Name(), ".go") {
			return nil
		}
		result.GoFiles++
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			result.ParseErrors = append(result.ParseErrors, fmt.Sprintf("%s: %v", relPath(rootPath, path), parseErr))
			return nil
		}
		aliases := regexpAliases(file)
		if len(aliases) == 0 {
			return nil
		}
		result.ImportFiles = append(result.ImportFiles, relPath(rootPath, path))
		function := "package"
		ast.Inspect(file, func(node ast.Node) bool {
			if declaration, ok := node.(*ast.FuncDecl); ok {
				function = "func " + declaration.Name.Name
			}
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			identifier, ok := selector.X.(*ast.Ident)
			if !ok || !aliases[identifier.Name] {
				return true
			}
			result.Sites = append(result.Sites, site{
				File: relPath(rootPath, path), Line: fset.Position(call.Pos()).Line,
				API: selector.Sel.Name, Function: function, Category: classifyRegexpAPI(selector.Sel.Name),
			})
			return true
		})
		return nil
	})
	if err != nil {
		fatal(err)
	}
	sort.Strings(result.ImportFiles)
	sort.Slice(result.Sites, func(i, j int) bool {
		if result.Sites[i].File != result.Sites[j].File {
			return result.Sites[i].File < result.Sites[j].File
		}
		return result.Sites[i].Line < result.Sites[j].Line
	})

	var destination *os.File
	if *output == "" {
		destination = os.Stdout
	} else {
		destination, err = os.Create(*output)
		if err != nil {
			fatal(err)
		}
		defer destination.Close()
	}
	encoder := json.NewEncoder(destination)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		fatal(err)
	}
}

func classifyRegexpAPI(api string) string {
	switch api {
	case "QuoteMeta", "MatchString", "FindString", "FindStringSubmatch", "FindAllString", "FindAllStringSubmatch":
		if api == "QuoteMeta" {
			return "value_match_or_escaping"
		}
		return "syntax_candidate_or_value_match"
	case "ReplaceAllString", "ReplaceAllStringFunc":
		return "text_rewrite_candidate"
	case "Compile", "MustCompile":
		return "pattern_definition"
	default:
		return "other_regexp_api"
	}
}

func regexpAliases(file *ast.File) map[string]bool {
	aliases := map[string]bool{}
	for _, declaration := range file.Imports {
		path, err := strconvUnquote(declaration.Path.Value)
		if err != nil || path != "regexp" {
			continue
		}
		alias := "regexp"
		if declaration.Name != nil {
			alias = declaration.Name.Name
		}
		if alias != "_" && alias != "." {
			aliases[alias] = true
		}
	}
	return aliases
}

func strconvUnquote(value string) (string, error) {
	if len(value) < 2 || value[0] != '"' || value[len(value)-1] != '"' {
		return "", fmt.Errorf("invalid import path %q", value)
	}
	return value[1 : len(value)-1], nil
}

func relPath(root, path string) string {
	relative, err := filepath.Rel(root, path)
	if err != nil {
		return filepath.ToSlash(path)
	}
	return filepath.ToSlash(relative)
}

func fatal(err error) {
	_, _ = fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
