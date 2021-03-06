package libmigrate

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParamTypeFuncQuestionMark(t *testing.T) {
	fn, err := ParamTypeQuestionMark.getFunc()
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		require.Equal(t, fn(), "?")
	}
}

func TestParamTypeFuncDollarSign(t *testing.T) {
	fn, err := ParamTypeDollarSign.getFunc()
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		require.Equal(t, fn(), fmt.Sprintf("$%d", i+1))
	}
}

func TestGetFunc(t *testing.T) {
	fn, err := ParamType(9999).getFunc()
	require.Equal(t, &unknownParamTypeError{
		paramType: ParamType(9999),
	}, err)
	require.Nil(t, fn)
}

func TestFilename(t *testing.T) {
	cases := []struct {
		isUp     bool
		version  int
		name     string
		expected string
	}{
		{isUp: true, version: 1, name: "asdf", expected: "0001_asdf.up.sql"},
		{isUp: false, version: 1, name: "asdf", expected: "0001_asdf.down.sql"},
		{isUp: true, version: 3210, name: "zz", expected: "3210_zz.up.sql"},
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("up=%v v=%v name=%v", c.isUp, c.version, c.name), func(t *testing.T) {
			require.Equal(t, c.expected, migration{
				Version: c.version,
				Name:    c.name,
			}.Filename(c.isUp))
		})
	}
}

func TestFilenameMatchesFormat(t *testing.T) {
	name := "01_notEnoughZeroes.up.sql"

	m := &migrator{}
	_, err := m.filenamesToMigrations(context.Background(), []string{name})
	require.Error(t, err)
	require.Equal(t, &badMigrationFilenameError{
		filename: name,
		expected: "0001_notEnoughZeroes.up.sql",
	}, err)
}

func TestDirectoriesToMigrationsDbDisagrees(t *testing.T) {
	db := dbMock{
		listMigrations: func(ctx context.Context) ([]dbMigration, error) {
			return []dbMigration{
				{Version: 1, Name: "first"},
			}, nil
		},
	}
	migrator := &migrator{db: db}
	_, err := migrator.filenamesToMigrations(context.Background(), []string{
		"0001_first_newname.up.sql",
		"0001_first_newname.down.sql",
	})

	require.Equal(t, &filesystemMigrationMismatchError{
		version:        1,
		dbName:         "first",
		filesystemName: "first_newname",
	}, err)
}

func TestDirectoriesToMigrationsTooManyInDb(t *testing.T) {
	db := dbMock{
		listMigrations: func(ctx context.Context) ([]dbMigration, error) {
			return []dbMigration{
				{Version: 1, Name: "first"},
				{Version: 2, Name: "second"},
			}, nil
		},
	}
	migrator := &migrator{db: db}
	_, err := migrator.filenamesToMigrations(context.Background(), []string{
		"0001_first.up.sql",
		"0001_first.down.sql",
	})

	require.Equal(t, err, &filesystemMissingDbMigrationError{
		version: 2,
	})
}

func TestMigrateLatest(t *testing.T) {
	calledGetVersion := false
	dbVersion := 1
	m, db, fs := Fixture(t)
	db.listMigrations = func(ctx context.Context) ([]dbMigration, error) {
		return []dbMigration{
			{Version: 1, Name: "v1"},
		}, nil
	}
	db.getVersion = func(ctx context.Context) (version int, err error) {
		require.False(t, calledGetVersion)
		calledGetVersion = true
		return dbVersion, nil
	}
	db.applyMigration = func(ctx context.Context, useTx, isUp bool, version int, name, query string) error {
		require.True(t, isUp)
		require.Equal(t, dbVersion+1, version)
		require.Equal(t, fmt.Sprintf("v%d", version), name)

		dbVersion++
		return nil
	}

	fs.listMigrationDir = func() ([]string, error) {
		return []string{
			"0001_v1.up.sql",
			"0001_v1.down.sql",
			"0002_v2.up.sql",
			"0002_v2.down.sql",
			"0003_v3.up.sql",
			"0003_v3.down.sql",
		}, nil
	}

	err := m.MigrateLatest(context.Background())
	require.NoError(t, err)
	require.Equal(t, 3, dbVersion)
}

func TestMigrateToUp(t *testing.T) {
	calledGetVersion := false
	m, db, _ := Fixture(t)
	db.listMigrations = func(ctx context.Context) ([]dbMigration, error) {
		return []dbMigration{
			{Version: 1, Name: "v1"},
		}, nil
	}
	db.getVersion = func(ctx context.Context) (version int, err error) {
		require.False(t, calledGetVersion)
		calledGetVersion = true
		return 1, nil
	}
	db.applyMigration = func(ctx context.Context, useTx, isUp bool, version int, name, query string) error {
		require.True(t, isUp)
		require.Equal(t, 2, version)
		require.Equal(t, fmt.Sprintf("v%d", version), name)
		return nil
	}

	err := m.MigrateTo(context.Background(), 2)
	require.NoError(t, err)
}

func TestMigrateToDown(t *testing.T) {
	calledGetVersion := false
	m, db, _ := Fixture(t)
	db.getVersion = func(ctx context.Context) (version int, err error) {
		require.False(t, calledGetVersion)
		calledGetVersion = true
		return 1, nil
	}
	db.applyMigration = func(ctx context.Context, useTx, isUp bool, version int, name, query string) error {
		require.False(t, isUp)
		require.Equal(t, 1, version)
		require.Equal(t, fmt.Sprintf("v%d", version), name)
		return nil
	}

	err := m.MigrateTo(context.Background(), 0)
	require.NoError(t, err)
}

func TestMigrateToDownMissing(t *testing.T) {
	calledGetVersion := false
	m, db, fs := Fixture(t)
	fs.listMigrationDir = func() ([]string, error) {
		return []string{
			"0001_v1.up.sql",
			"0002_v2.up.sql",
			"0002_v2.down.sql",
		}, nil
	}

	db.getVersion = func(ctx context.Context) (version int, err error) {
		require.False(t, calledGetVersion)
		calledGetVersion = true
		return 2, nil
	}

	// Should call apply once (at version 2), then error
	applyCalled := false
	db.applyMigration = func(ctx context.Context, useTx, isUp bool, version int, name, query string) error {
		applyCalled = true
		require.False(t, isUp)
		require.Equal(t, 2, version)
		require.Equal(t, fmt.Sprintf("v%d", version), name)
		return nil
	}

	err := m.MigrateTo(context.Background(), 0)
	require.Equal(t, &missingMigrationError{
		isUp:    false,
		version: 1,
	}, err)
	require.True(t, applyCalled)
}

func TestHasPendingTrue(t *testing.T) {
	m, db, _ := Fixture(t)
	db.getVersion = func(ctx context.Context) (version int, err error) {
		return 1, nil
	}

	hasPending, err := m.HasPending(context.Background())
	require.NoError(t, err)
	require.True(t, hasPending)
}

func TestHasPendingFalse(t *testing.T) {
	m, db, _ := Fixture(t)
	db.getVersion = func(ctx context.Context) (version int, err error) {
		return 3, nil
	}

	hasPending, err := m.HasPending(context.Background())
	require.NoError(t, err)
	require.False(t, hasPending)
}

func TestCreate(t *testing.T) {
	calledCreateFile := make(map[string]bool)
	m, _, fs := Fixture(t)
	fs.createFile = func(version int, name, direction string) (string, error) {
		require.Equal(t, 4, version)
		require.Equal(t, "asdff", name)

		calledCreateFile[direction] = true
		return "testpath", nil
	}

	err := m.Create(context.Background(), "asdff")
	require.NoError(t, err)
	require.Equal(t, map[string]bool{
		"up":   true,
		"down": true,
	}, calledCreateFile)
}
