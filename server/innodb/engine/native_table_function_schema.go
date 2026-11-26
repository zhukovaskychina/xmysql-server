package engine

import "github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"

// These are SQL result contracts, independent of arguments and row count.
// Return fresh slices so aliases and projection rewrites stay query-local.
func vectorSearchResultColumns() []joinOutputColumn {
	return []joinOutputColumn{
		{name: "entry_id", typ: metadata.TypeVarchar},
		{name: "distance", typ: metadata.TypeDouble},
		{name: "source_schema", typ: metadata.TypeVarchar},
		{name: "source_table", typ: metadata.TypeVarchar},
		{name: "source_column", typ: metadata.TypeVarchar},
		{name: "primary_key", typ: metadata.TypeLongBlob},
		{name: "source_revision", typ: metadata.TypeBigInt},
		{name: "source_sha256", typ: metadata.TypeVarchar},
		{name: "source_block_id", typ: metadata.TypeVarchar},
		{name: "source_modality", typ: metadata.TypeVarchar},
		{name: "model_id", typ: metadata.TypeVarchar},
		{name: "model_version", typ: metadata.TypeVarchar},
		{name: "space_id", typ: metadata.TypeVarchar},
		{name: "index_generation", typ: metadata.TypeBigInt},
		{name: "approximate", typ: metadata.TypeTinyInt},
		{name: "candidates_visited", typ: metadata.TypeBigInt},
		{name: "search_budget", typ: metadata.TypeBigInt},
		{name: "may_have_more", typ: metadata.TypeTinyInt},
	}
}

func contentTextChunksResultColumns() []joinOutputColumn {
	return []joinOutputColumn{
		{name: "ordinal", typ: metadata.TypeBigInt},
		{name: "text", typ: metadata.TypeVarchar},
		{name: "block", typ: metadata.TypeContent},
		{name: "byte_start", typ: metadata.TypeBigInt},
		{name: "byte_end", typ: metadata.TypeBigInt},
	}
}

func contentPDFPagesResultColumns() []joinOutputColumn {
	return []joinOutputColumn{
		{name: "ordinal", typ: metadata.TypeBigInt},
		{name: "page", typ: metadata.TypeBigInt},
		{name: "text", typ: metadata.TypeMediumText},
		{name: "block", typ: metadata.TypeContent},
		{name: "extraction_status", typ: metadata.TypeVarchar},
	}
}

func contentDOCXBlocksResultColumns() []joinOutputColumn {
	return []joinOutputColumn{
		{name: "ordinal", typ: metadata.TypeBigInt},
		{name: "text", typ: metadata.TypeMediumText},
		{name: "block", typ: metadata.TypeContent},
		{name: "structure_path", typ: metadata.TypeVarchar},
	}
}

func contentXLSXRowsResultColumns() []joinOutputColumn {
	return []joinOutputColumn{
		{name: "ordinal", typ: metadata.TypeBigInt},
		{name: "sheet_name", typ: metadata.TypeVarchar},
		{name: "row_number", typ: metadata.TypeBigInt},
		{name: "text", typ: metadata.TypeMediumText},
		{name: "block", typ: metadata.TypeContent},
		{name: "structure_path", typ: metadata.TypeVarchar},
	}
}

func fullTextTokensResultColumns() []joinOutputColumn {
	return []joinOutputColumn{{name: "token", typ: metadata.TypeVarchar}, {name: "position", typ: metadata.TypeBigInt}}
}

func emptyDeclaredTableFunctionSource(name string, schema []joinOutputColumn) *derivedJoinSource {
	columns := make([]string, 0, len(schema))
	for _, column := range schema {
		columns = append(columns, column.name)
	}
	return &derivedJoinSource{
		rows: make([]derivedRow, 0), columns: columns,
		qualifiedCols: map[string][]string{name: append([]string(nil), columns...)},
	}
}
