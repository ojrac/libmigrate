package libmigrate

import "fmt"

type filesystemMissingDbMigrationError struct {
	version int
}

func (e *filesystemMissingDbMigrationError) Error() string {
	return fmt.Sprintf(
		"DB has more migrations than filesystem")
}

func (e *filesystemMissingDbMigrationError) Version() int { return e.version }

type filesystemMigrationMismatchError struct {
	version        int
	dbName         string
	filesystemName string
}

func (e *filesystemMigrationMismatchError) Error() string {
	return fmt.Sprintf(
		"DB migration %d (%s) doesn't match filesystem (%s)",
		e.version, e.dbName, e.filesystemName)
}

func (e *filesystemMigrationMismatchError) Version() int           { return e.version }
func (e *filesystemMigrationMismatchError) DbName() string         { return e.dbName }
func (e *filesystemMigrationMismatchError) FilesystemName() string { return e.filesystemName }

type migrationNameMismatchError struct {
	version          int
	upName, downName string
}

func (e *migrationNameMismatchError) Error() string {
	return fmt.Sprintf(
		"DB migration %d up and down migration names don't match (\"%s\" != \"%s\")",
		e.version, e.upName, e.downName)
}

func (e *migrationNameMismatchError) Version() int     { return e.version }
func (e *migrationNameMismatchError) UpName() string   { return e.upName }
func (e *migrationNameMismatchError) DownName() string { return e.downName }

type badMigrationFilenameError struct {
	filename string
	expected string
	cause    error
}

func (e *badMigrationFilenameError) Error() string {
	expected := e.expected
	if expected == "" {
		expected = "1234_name.up.sql"
	}
	return fmt.Sprintf(
		"Bad migration filename: %s (should be %s)", e.filename, expected)
}

func (e *badMigrationFilenameError) BadFilename() string { return e.filename }
func (e *badMigrationFilenameError) Cause() error        { return e.cause }

type missingMigrationError struct {
	version int
	isUp    bool
}

func (e *missingMigrationError) Error() string {
	direction := "down"
	if e.isUp {
		direction = "up"
	}
	return fmt.Sprintf(
		"Missing %s migration %d", direction, e.version)
}

func (e *missingMigrationError) MigrationVersion() int { return e.version }
func (e *missingMigrationError) IsUp() bool            { return e.isUp }

type badVersionError struct {
	version int
	problem string
}

func (e *badVersionError) Error() string {
	return fmt.Sprintf("Bad migration (%d): %s", e.version, e.problem)
}

func (e *badVersionError) Version() int    { return e.version }
func (e *badVersionError) Problem() string { return e.problem }

type badMigrationPathError struct {
	isNotDir bool
}

func (e *badMigrationPathError) Error() string {
	if e.isNotDir {
		return "Migration path exists, but is not a directory"
	}

	return "Bad migration path"
}

func (e *badMigrationPathError) IsNotDir() bool { return e.isNotDir }

type migrateError struct {
	cause error
}

func (e *migrateError) Error() string {
	return fmt.Sprintf("running migration: %+v", e.cause)
}

func (e *migrateError) Cause() error { return e.cause }

type unknownParamTypeError struct {
	paramType ParamType
}

func (e *unknownParamTypeError) Error() string {
	return fmt.Sprintf("unknown ParamType: %d", e.paramType)
}

func (e *unknownParamTypeError) ParamType() ParamType { return e.paramType }
