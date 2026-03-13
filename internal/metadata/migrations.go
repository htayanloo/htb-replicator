package metadata

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// runMigrations applies any unapplied SQL migration files in order.
// Migration versions are extracted from filenames like 001_initial_schema.sql.
func runMigrations(ctx context.Context, db *sql.DB) error {
	// Ensure the schema_migrations table exists before querying it.
	const createMigTable = `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    INTEGER PRIMARY KEY,
			applied_at DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
		)`
	if _, err := db.ExecContext(ctx, createMigTable); err != nil {
		return fmt.Errorf("create schema_migrations table: %w", err)
	}

	// Read applied versions.
	applied := make(map[int]bool)
	rows, err := db.QueryContext(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("query applied migrations: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return fmt.Errorf("scan migration version: %w", err)
		}
		applied[v] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate migration rows: %w", err)
	}
	rows.Close()

	// Collect migration files.
	entries, err := fs.ReadDir(migrationFiles, "migrations")
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}


	type migration struct {
		version  int
		filename string
	}
	var migrations []migration

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		// Extract version number from filename prefix (e.g. "001_initial_schema.sql" → 1).
		parts := strings.SplitN(entry.Name(), "_", 2)
		if len(parts) == 0 {
			continue
		}
		v, err := strconv.Atoi(parts[0])
		if err != nil {
			continue
		}
		migrations = append(migrations, migration{version: v, filename: entry.Name()})
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].version < migrations[j].version
	})

	for _, m := range migrations {
		if applied[m.version] {
			continue
		}

		content, err := migrationFiles.ReadFile("migrations/" + m.filename)
		if err != nil {
			return fmt.Errorf("read migration file %s: %w", m.filename, err)
		}

		// Execute the entire script in a transaction.
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin migration tx v%d: %w", m.version, err)
		}

		if _, err := tx.ExecContext(ctx, string(content)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("exec migration v%d (%s): %w", m.version, m.filename, err)
		}

		if _, err := tx.ExecContext(ctx,
			`INSERT INTO schema_migrations (version) VALUES (?)`, m.version,
		); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("record migration v%d: %w", m.version, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration v%d: %w", m.version, err)
		}
	}

	return nil
}
