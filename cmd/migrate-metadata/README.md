# Metadata Migration Tool

This tool helps migrate from the old tuple-based metadata system to the new metadata system in XMySQL-Server.

## Overview

The metadata migration tool performs the following tasks:

1. Scans existing database schemas and tables
2. Converts the old tuple-based metadata to the new metadata format
3. Stores the new metadata in a structured format
4. Validates the migrated data

## Usage

```bash
# Build the migration tool
go build -o migrate-metadata ./cmd/migrate-metadata

# Migrate all schemas
./migrate-metadata --data-dir=/path/to/data

# Migrate a specific schema
./migrate-metadata --data-dir=/path/to/data --schema=schema_name
```

## Flags

- `--data-dir`: Path to the data directory (default: "./data")
- `--schema`: Optional schema name to migrate (default: migrate all schemas)

## Implementation Notes

- The migration is non-destructive and creates new metadata files without modifying existing data
- The tool includes validation to ensure data integrity during migration
- After migration, the new metadata will be stored in the `meta` subdirectory of the data directory

## Post-Migration

After running the migration, you should:

1. Verify the migrated metadata
2. Update your application to use the new metadata system
3. Test thoroughly before deploying to production
4. Consider removing the old tuple-based metadata files after successful migration
