// Command common_constants_audit compares the original public common constants
// with their current compiler-exported values and static types.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"go/importer"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"strings"
)

type inventoryRow struct {
	ID    string `json:"id"`
	File  string `json:"file"`
	Name  string `json:"name"`
	Type  string `json:"type"`
	Value string `json:"value"`
}

type result struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	OriginalType  string `json:"original_type"`
	OriginalValue string `json:"original_value"`
	CurrentType   string `json:"current_type"`
	CurrentValue  string `json:"current_value"`
	Status        string `json:"status"`
}

func main() {
	goBinary := flag.String("go", "go", "Go toolchain executable")
	inventoryPath := flag.String("inventory", "docs/superpowers/plans/2026-10-15-common-constants-unification-inventory.json", "original inventory")
	output := flag.String("output", "", "JSON evidence output (stdout when empty)")
	flag.Parse()
	if err := run(*goBinary, *inventoryPath, *output); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(goBinary, inventoryPath, output string) error {
	data, err := os.ReadFile(inventoryPath)
	if err != nil {
		return err
	}
	var inventory struct {
		Rows []inventoryRow `json:"rows"`
	}
	if err := json.Unmarshal(data, &inventory); err != nil {
		return err
	}
	cmd := exec.Command(goBinary, "list", "-deps", "-export", "-json", "./server/common")
	cmd.Stderr = os.Stderr
	exports, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("compiler exports: %w", err)
	}
	paths := map[string]string{}
	decoder := json.NewDecoder(strings.NewReader(string(exports)))
	for {
		var pkg struct{ ImportPath, Export string }
		if err := decoder.Decode(&pkg); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		paths[pkg.ImportPath] = pkg.Export
	}
	imp := importer.ForCompiler(token.NewFileSet(), "gc", func(path string) (io.ReadCloser, error) { return os.Open(paths[path]) })
	pkg, err := imp.Import("github.com/zhukovaskychina/xmysql-server/server/common")
	if err != nil {
		return err
	}
	results := []result{}
	failures := 0
	for _, row := range inventory.Rows {
		if !strings.HasPrefix(row.File, "server/common/") || !token.IsIdentifier(row.Name) || row.Value == "" || strings.Contains(row.Type, "function/type/catalog") {
			continue
		}
		entry := result{ID: row.ID, Name: row.Name, OriginalType: row.Type, OriginalValue: row.Value, Status: "unchanged"}
		constant, ok := pkg.Scope().Lookup(row.Name).(*types.Const)
		if !ok {
			entry.Status = "missing"
			failures++
		} else {
			entry.CurrentType = types.TypeString(constant.Type(), func(p *types.Package) string { return p.Name() })
			entry.CurrentValue = constant.Val().ExactString()
			if entry.CurrentType != row.Type || entry.CurrentValue != row.Value {
				if row.Name == "TRX_STATE_NOT_STARTED" && entry.CurrentType == "uint8" && entry.CurrentValue == "0" {
					entry.Status = "intentional_transaction_encoding_correction"
				} else {
					entry.Status = "unexpected_change"
					failures++
				}
			}
		}
		results = append(results, entry)
	}
	if len(results) != 1216 {
		return fmt.Errorf("inventory selected %d original constants, expected 1216", len(results))
	}
	encoded, err := json.MarshalIndent(struct {
		Checked, Failures int
		Constants         []result
	}{len(results), failures, results}, "", "  ")
	if err != nil {
		return err
	}
	if output == "" {
		fmt.Println(string(encoded))
	} else if err := os.WriteFile(output, append(encoded, '\n'), 0644); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "checked %d original constants; unexpected changes: %d\n", len(results), failures)
	if failures > 0 {
		return fmt.Errorf("common constant compatibility audit failed")
	}
	return nil
}
