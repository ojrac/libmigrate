package libmigrate

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func testMigrator(t *testing.T) *migrator {
	require.NotNil(t, t)

	db := dbMock{
		listMigrations: func(ctx context.Context) ([]dbMigration, error) {
			return []dbMigration{}, nil
		},
	}
	return &migrator{db: db}
}

func TestFilenamesToMigrations(t *testing.T) {
	migrator := testMigrator(t)
	result, err := migrator.filenamesToMigrations(context.Background(), []string{
		"0002_second.up.sql",
		"ignored", // ignored
		"0001_first.down.sql",
		"0001_ignored.sql", // ignored
		"0002_second.down.sql",
		"9999_asfjkgsdhsl.up.txt", // ignored
		"0001_first.up.sql",
	})
	require.NoError(t, err)

	require.Equal(t, []migration{
		migration{
			Version: 1,
			Name:    "first",
			HasUp:   true,
			HasDown: true,
		},
		migration{
			Version: 2,
			Name:    "second",
			HasUp:   true,
			HasDown: true,
		},
	}, result)
}

func TestFilenamesToMigrationsMissingDown(t *testing.T) {
	// Missing a down migration doesn't return an error, but does note that
	// it's missing in the corresponding migration
	migrator := testMigrator(t)
	result, err := migrator.filenamesToMigrations(context.Background(), []string{
		"0001_v1.up.sql",
		"0001_v1.down.sql",
		"0002_v2.up.sql",
		// No 0002_v2.down.dql
	})
	require.NoError(t, err)
	require.Equal(t, []migration{
		migration{
			Version: 1,
			Name:    "v1",
			HasUp:   true,
			HasDown: true,
		},
		migration{
			Version: 2,
			Name:    "v2",
			HasUp:   true,
			HasDown: false,
		},
	}, result)
}

func TestFilenamesToMigrationsMismatchedNames(t *testing.T) {
	migrator := testMigrator(t)
	_, err := migrator.filenamesToMigrations(context.Background(), []string{
		"0001_name_b.down.sql",
		"0001_name_a.up.sql",
	})
	require.Equal(t, err, &migrationNameMismatchError{
		version:  1,
		upName:   "name_a",
		downName: "name_b",
	})
}

func TestFilenamesToMigrationsMissingMiddleMigration(t *testing.T) {
	migrator := testMigrator(t)
	_, err := migrator.filenamesToMigrations(context.Background(), []string{
		"0001_one.up.sql",
		"0001_one.down.sql",
		"0003_three.up.sql",
		"0003_three.down.sql",
	})
	require.Equal(t, &missingMigrationError{
		version: 2,
		isUp:    true,
	}, err)
}

func TestMissingLastMigration(t *testing.T) {
	migrator := testMigrator(t)
	_, err := migrator.filenamesToMigrations(context.Background(), []string{
		"0001_one.up.sql",
		"0001_one.down.sql",
		"0002_two.down.sql",
	})
	require.Equal(t, &missingMigrationError{
		version: 2,
		isUp:    true,
	}, err)
}
