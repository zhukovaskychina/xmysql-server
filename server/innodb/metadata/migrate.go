package metadata

import (
	"context"
	"fmt"
)

// MetadataManager defines the interface for managing metadata
type MetadataManager interface {
	// SaveSchema(schema *SchemaMeta) error
	SaveDatabaseSchema(schema *DatabaseSchema) error
}

// Migrator handles migration from old schema system to new metadata system
type Migrator struct {
	schemaManager InfoSchemaManager
	metaManager   MetadataManager
}

// NewMigrator creates a new Migrator instance
func NewMigrator(schemaManager InfoSchemaManager, metaManager MetadataManager) *Migrator {
	return &Migrator{
		schemaManager: schemaManager,
		metaManager:   metaManager,
	}
}

// MigrateAll migrates all schemas from the old system to the new one
func (m *Migrator) MigrateAll(ctx context.Context) error {
	schemaNames, err := m.schemaManager.GetAllSchemaNames(ctx)
	if err != nil {
		return fmt.Errorf("failed to get schema names: %w", err)
	}

	for _, schemaName := range schemaNames {
		if err := m.MigrateSchema(ctx, schemaName); err != nil {
			return fmt.Errorf("failed to migrate schema %s: %w", schemaName, err)
		}
	}

	return nil
}

// MigrateSchema migrates a single schema
func (m *Migrator) MigrateSchema(ctx context.Context, schemaName string) error {
	// Get the old schema
	oldSchema, err := m.schemaManager.GetSchemaByName(ctx, schemaName)
	if err != nil {
		return fmt.Errorf("failed to get schema %s: %w", schemaName, err)
	}

	// Convert the schema to the new format
	databaseSchema, err := ConvertSchema(oldSchema)
	if err != nil {
		return fmt.Errorf("failed to convert schema %s: %w", schemaName, err)
	}

	// Save the migrated schema
	if err := m.metaManager.SaveDatabaseSchema(databaseSchema); err != nil {
		return fmt.Errorf("failed to save migrated schema %s: %w", schemaName, err)
	}

	return nil
}

// MigrateTable migrates a single table from the old system to the new one
func (m *Migrator) MigrateTable(ctx context.Context, schemaName, tableName string) (*TableMeta, error) {
	// Get the old table
	oldTable, err := m.schemaManager.GetTableByName(ctx, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table %s.%s: %w", schemaName, tableName, err)
	}

	// Convert the table to the new format
	tableMeta, err := ConvertTable(oldTable)
	if err != nil {
		return nil, fmt.Errorf("failed to convert table %s: %w", tableName, err)
	}

	return tableMeta, nil
}
