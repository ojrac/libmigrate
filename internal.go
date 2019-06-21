package libmigrate

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const filenameFmt = "%04d_%s.%s.sql"

type migrator struct {
	db                  dbWrapper
	filesystem          filesystemWrapper
	disableTransactions bool
	outputWriter        io.Writer
}

func (m *migrator) printf(format string, a ...interface{}) {
	if m.outputWriter == nil {
		return
	}

	fmt.Fprintf(m.outputWriter, format, a...)
}

type paramFunc func() string

func (t ParamType) getFunc() (paramFunc, error) {
	switch t {
	case ParamTypeQuestionMark:
		return func() string { return "?" }, nil
	case ParamTypeDollarSign:
		var i = 0
		return func() string {
			i++
			return fmt.Sprintf("$%d", i)
		}, nil
	}

	return nil, &unknownParamTypeError{paramType: t}
}

type migration struct {
	Version int
	Name    string
	HasUp   bool
	HasDown bool
}

type dbMigration struct {
	Version int
	Name    string
}

func (m migration) Filename(isUp bool) string {
	direction := "up"
	if !isUp {
		direction = "down"
	}
	return fmt.Sprintf(filenameFmt, m.Version, m.Name, direction)
}

func (m *migrator) listMigrations(ctx context.Context) (result []migration, err error) {
	err = m.db.RequireSchema(ctx)
	if err != nil {
		return
	}

	names, err := m.filesystem.ListMigrationDir()
	return m.filenamesToMigrations(ctx, names)
}

func (m *migrator) filenamesToMigrations(ctx context.Context, names []string) (result []migration, err error) {
	migrationsByVersion := make(map[int]migration, len(names)/2)

	for _, s := range names {
		up := strings.HasSuffix(s, ".up.sql")
		down := strings.HasSuffix(s, ".down.sql")
		if !up && !down || up == down {
			continue
		}

		parts := strings.SplitN(s, "_", 2)

		var version int
		version, err = strconv.Atoi(parts[0])
		if err != nil {
			return nil, &badMigrationFilenameError{
				filename: s,
				cause:    err,
			}
		}

		name := parts[1]
		if up {
			name = strings.TrimSuffix(name, ".up.sql")
		} else {
			name = strings.TrimSuffix(name, ".down.sql")
		}

		if m, ok := migrationsByVersion[version]; !ok {
			migrationsByVersion[version] = migration{
				Version: version,
				Name:    name,
				HasUp:   up,
				HasDown: down,
			}
		} else if m.Name != name {
			var upName, downName string
			if up {
				upName = name
				downName = m.Name
			} else {
				upName = m.Name
				downName = name
			}

			err = &migrationNameMismatchError{
				version:  version,
				upName:   upName,
				downName: downName,
			}
			return
		} else {
			if up {
				m.HasUp = true
			} else {
				m.HasDown = true
			}
			migrationsByVersion[version] = m
		}
	}

	err = validateMigrations(true, migrationsByVersion)
	if err != nil {
		return nil, err
	}

	err = m.testForUnknownMigrations(ctx, migrationsByVersion)
	if err != nil {
		return nil, err
	}

	// At this point, we've checked that migrationsByVersion has migrations
	// from 1 to N, so we can directly write to result[i].
	result = make([]migration, len(migrationsByVersion))
	for version, m := range migrationsByVersion {
		result[version-1] = m
	}
	return
}

func (m *migrator) testForUnknownMigrations(ctx context.Context, migrations map[int]migration) (err error) {
	dbMigrations, err := m.db.ListMigrations(ctx)

	for _, dbMigration := range dbMigrations {
		fsMigration, ok := migrations[dbMigration.Version]
		if !ok {
			return &filesystemMissingDbMigrationError{
				version: dbMigration.Version,
			}
		}

		if fsMigration.Name != dbMigration.Name {
			return &filesystemMigrationMismatchError{
				version:        dbMigration.Version,
				dbName:         dbMigration.Name,
				filesystemName: fsMigration.Name,
			}
		}
	}

	return nil
}

func validateMigrations(isUp bool, migrations map[int]migration) error {
	for i := 0; i < len(migrations); i++ {
		version := i + 1

		// Missing "up" migrations is a fatal error; missing down migrations
		// are only an error if you need to run them.
		migration, ok := migrations[version]
		if !ok || !migration.HasUp {
			return &missingMigrationError{
				version: version,
				isUp:    true,
			}
		}

	}

	return nil
}

func (m *migrator) useTx(sql string) bool {
	if m.disableTransactions {
		return false
	}
	if strings.HasPrefix(sql, NoTransactionPrefix) {
		return false
	}
	return true
}

func (m *migrator) internalMigrate(ctx context.Context, migration migration, isUp bool) (err error) {
	if (isUp && !migration.HasUp) || (!isUp && !migration.HasDown) {
		return &missingMigrationError{
			version: migration.Version,
			isUp:    isUp,
		}
	}

	note := "+"
	if !isUp {
		note = "-"
	}
	m.printf(" %s %s\n", note, migration.Filename(isUp))

	sqlString, err := m.filesystem.ReadMigration(migration.Filename(isUp))
	if err != nil {
		return
	}

	useTx := m.useTx(sqlString)
	return m.db.ApplyMigration(ctx, useTx, isUp, migration.Version, migration.Name, sqlString)
}
