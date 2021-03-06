package libmigrate

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"io/fs"
	"os"
	"time"
)

// If your migration must run outside a transaction, the first line must match
// this constant. (Useful for migrations like PostgreSQL "CREATE INDEX
// CONCURRENTLY" statements.)
const NoTransactionPrefix = "-- migrate: no-transaction\n"

type Migrator interface {
	MigrateLatest(ctx context.Context) error
	MigrateTo(ctx context.Context, version int) error
	GetVersion(ctx context.Context) (int, error)
	HasPending(ctx context.Context) (bool, error)
	Create(ctx context.Context, name string) error

	SetTableName(name string)
	// If set, "table" becomes schema."table"
	SetTableSchema(schema string)

	// Set to nil to disable output. Default: os.Stdout
	SetOutputWriter(io.Writer)
}

// Different databases use different syntax for indicating parameter values.
// Since jmoiron/sqlx hasn't found a better way than naming each one, we
// probably won't either. Leave it to the caller.
type ParamType int

const (
	ParamTypeQuestionMark ParamType = iota
	ParamTypeDollarSign
)

// New() accepts a *database/sql.DB or equivalent.
type DB interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

func New(db DB, migrationDir string, paramType ParamType) Migrator {
	return internalNew(db, os.DirFS(migrationDir), migrationDir, paramType)
}

func NewFs(db DB, fs fs.FS, paramType ParamType) Migrator {
	return internalNew(db, fs, "", paramType)
}

// Only pass migrationDir if it's intended to be writeable
func internalNew(db DB, fs fs.FS, migrationDir string, paramType ParamType) *migrator {
	return &migrator{
		db: &dbWrapperImpl{
			db:          db,
			tableSchema: "",
			tableName:   "migration_version",
			paramType:   paramType,
		},
		filesystem: &filesystemWrapperImpl{
			migrationDir: migrationDir,
			fsys:         fs,
		},
		disableTransactions: false,
		outputWriter:        os.Stdout,
	}
}

func (m *migrator) SetTableName(name string) {
	m.db.SetTableName(name)
}

func (m *migrator) SetTableSchema(schema string) {
	m.db.SetTableSchema(schema)
}

func (m *migrator) SetOutputWriter(writer io.Writer) {
	m.outputWriter = writer
}

func (m *migrator) MigrateLatest(ctx context.Context) (err error) {
	migrations, err := m.listMigrations(ctx)
	if err != nil {
		return
	}

	toVersion := 0
	if len(migrations) > 0 {
		toVersion = migrations[len(migrations)-1].Version
	}

	return m.MigrateTo(ctx, toVersion)
}

func (m *migrator) MigrateTo(ctx context.Context, version int) (err error) {
	availableMigrations, err := m.listMigrations(ctx)
	if err != nil {
		return
	}

	currVersion, err := m.GetVersion(ctx)
	if err != nil {
		return
	}
	m.printf("Migrating from %d to %d\n", currVersion, version)
	start := time.Now()
	defer func() {
		if err == nil {
			m.printf("Finished in %v\n", time.Since(start))
		}
	}()
	if version == currVersion {
		return nil
	}

	if version < 0 {
		return &badVersionError{
			version: version,
			problem: "version must be 0 or higher",
		}
	}

	if version > len(availableMigrations) {
		return &badVersionError{
			version: version,
			problem: fmt.Sprintf("max version is %d", len(availableMigrations)),
		}
	}

	isUp := currVersion < version
	step := 1
	if !isUp {
		step = -1
	}

	for currVersion != version {
		var migration migration
		if isUp {
			migration = availableMigrations[currVersion]
		} else {
			migration = availableMigrations[currVersion-1]
		}

		err = m.internalMigrate(ctx, migration, isUp)
		if err != nil {
			return
		}

		currVersion += step
	}

	return nil
}

func (m *migrator) GetVersion(ctx context.Context) (int, error) {
	err := m.db.RequireSchema(ctx)
	if err != nil {
		return 0, err
	}

	return m.db.GetVersion(ctx)
}

func (m *migrator) HasPending(ctx context.Context) (bool, error) {
	version, err := m.GetVersion(ctx)
	if err != nil {
		return false, err
	}

	availableMigrations, err := m.listMigrations(ctx)
	if err != nil {
		return false, err
	}

	return version != len(availableMigrations), nil
}

func (m *migrator) Create(ctx context.Context, name string) (err error) {
	if name == "" {
		return &badMigrationFilenameError{
			filename: name,
		}
	}

	availableMigrations, err := m.listMigrations(ctx)
	if err != nil {
		return
	}

	next := len(availableMigrations) + 1

	if next == 1 {
		if err = m.filesystem.EnsureMigrationDir(); err != nil {
			return err
		}
	}

	path, err := m.filesystem.CreateFile(next, name, "up")
	if err == nil {
		m.printf(" Created %s\n", path)
		path, err = m.filesystem.CreateFile(next, name, "down")
		if err == nil {
			m.printf(" Created %s\n", path)
		}
	}

	return
}
