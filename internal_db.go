package libmigrate

import (
	"context"
	"database/sql"
	"fmt"
)

type dbWrapper interface {
	ApplyMigration(ctx context.Context, useTx, isUp bool, version int, name, query string) error
	RequireSchema(ctx context.Context) error
	ListMigrations(ctx context.Context) ([]dbMigration, error)
	GetVersion(ctx context.Context) (int, error)

	SetTableName(name string)
	SetTableSchema(schema string)
}

type dbWrapperImpl struct {
	db          DB
	paramType   ParamType
	tableSchema string
	tableName   string
}

type dbOrTx interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

func (db *dbWrapperImpl) SetTableName(name string) {
	db.tableName = name
}

func (db *dbWrapperImpl) SetTableSchema(schema string) {
	db.tableSchema = schema
}

func (w *dbWrapperImpl) ApplyMigration(ctx context.Context, useTx, isUp bool, version int, name, query string) (err error) {
	var db dbOrTx = w.db
	if useTx {
		var tx *sql.Tx
		tx, err = w.db.BeginTx(ctx, nil)
		if err != nil {
			return
		}

		db = tx
		defer func() {
			if err == nil {
				err = tx.Commit()
			} else {
				tx.Rollback()
			}
		}()
	}

	_, err = db.ExecContext(ctx, query)
	if err != nil {
		return &migrateError{cause: err}
	}

	paramFunc, err := w.paramType.getFunc()
	if err != nil {
		return
	}
	if isUp {
		_, err = db.ExecContext(ctx, fmt.Sprintf(`
			INSERT INTO %s
						(version, name)
				 VALUES (%s, %s)
		`, w.fullTableName(), paramFunc(), paramFunc()),
			version, name)
	} else {
		_, err = db.ExecContext(ctx, fmt.Sprintf(`
			DELETE FROM %s
				  WHERE version = %s
						AND name = %s
		`, w.fullTableName(), paramFunc(), paramFunc()),
			version, name)
	}
	return err
}

func (w *dbWrapperImpl) RequireSchema(ctx context.Context) error {
	_, err := w.db.ExecContext(ctx, fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		version integer PRIMARY KEY NOT NULL,
		name text NOT NULL
	);`, w.fullTableName()))
	return err
}

func (w *dbWrapperImpl) ListMigrations(ctx context.Context) (result []dbMigration, err error) {
	rows, err := w.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT version, name
		  FROM %s
	  ORDER BY version ASC
	`, w.fullTableName()))
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var m dbMigration
		err = rows.Scan(&m.Version, &m.Name)
		if err != nil {
			return
		}

		result = append(result, m)
	}

	return
}

func (w *dbWrapperImpl) GetVersion(ctx context.Context) (version int, err error) {
	err = w.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT coalesce(max(version), 0)
		  FROM %s
		  `, w.fullTableName())).Scan(&version)
	return
}

func (w *dbWrapperImpl) fullTableName() string {
	if w.tableSchema != "" {
		return fmt.Sprintf("%s.\"%s\"", w.tableSchema, w.tableName)
	}

	return fmt.Sprintf("\"%s\"", w.tableName)
}
