package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

func TestMetadataViewsLegacyContract(t *testing.T) {
	rows := map[string][][]interface{}{}
	for name, result := range map[string]*SelectResult{
		"isCollations": executeInformationSchemaCollationsSelect("select * from information_schema.collations"),
		"isCharsets":   executeInformationSchemaCharacterSetsSelect("select * from information_schema.character_sets"),
	} {
		for _, record := range result.Records {
			row := []interface{}{}
			for _, value := range record.GetValues() {
				row = append(row, value.Raw())
			}
			rows[name] = append(rows[name], row)
		}
	}
	e := &XMySQLExecutor{}
	for name, run := range map[string]func(*ExecutionContext){
		"showCharsets":   func(ctx *ExecutionContext) { e.executeShowCharacterSet(ctx, "show character set") },
		"showCollations": func(ctx *ExecutionContext) { e.executeShowCollation(ctx, "show collation") },
		"showPrivileges": e.executeShowPrivileges,
	} {
		ctx := &ExecutionContext{Results: make(chan *Result, 1)}
		run(ctx)
		result := <-ctx.Results
		if data, ok := result.Data.(map[string]interface{}); ok {
			rows[name] = data["rows"].([][]interface{})
		} else {
			for _, record := range result.Data.(*SelectResult).Records {
				row := []interface{}{}
				for _, value := range record.GetValues() {
					row = append(row, value.Raw())
				}
				rows[name] = append(rows[name], row)
			}
		}
	}
	typed := map[string][][]string{}
	for view, records := range rows {
		for _, record := range records {
			row := []string{}
			for _, cell := range record {
				row = append(row, fmt.Sprintf("%T:%v", cell, cell))
			}
			typed[view] = append(typed[view], row)
		}
	}
	got, err := json.MarshalIndent(typed, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile("testdata/metadata_views_legacy_contract.json")
	if err != nil {
		t.Fatal(err)
	}
	if string(want) != string(got) {
		t.Fatal("legacy metadata view contract changed")
	}
}
