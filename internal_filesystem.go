package libmigrate

import (
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"path"
)

var (
	ErrFsNotWriteable = fmt.Errorf("Migration filesystem not writeable")
)

type filesystemWrapper interface {
	CreateFile(version int, name, direction string) (filePath string, err error)
	EnsureMigrationDir() error
	ListMigrationDir() ([]string, error)
	ReadMigration(filename string) (string, error)
}

type filesystemWrapperImpl struct {
	migrationDir string // Optional. If not set, the filesystem is not writeable.
	fsys         fs.FS
}

func (w *filesystemWrapperImpl) ListMigrationDir() (names []string, err error) {
	dirEntries, err := fs.ReadDir(w.fsys, ".")
	if err != nil {
		return
	}

	for _, entry := range dirEntries {
		if !entry.IsDir() {
			names = append(names, entry.Name())
		}
	}

	return
}

func (w *filesystemWrapperImpl) requireWriteable() error {
	// If only w.fsys is set, this is a read-only filesystem
	if w.migrationDir == "" {
		return ErrFsNotWriteable
	}

	return nil
}

func (w *filesystemWrapperImpl) CreateFile(version int, name, direction string) (filePath string, err error) {
	if err = w.requireWriteable(); err != nil {
		return
	}

	fname := path.Join(w.migrationDir, fmt.Sprintf(filenameFmt, version, name, direction))

	f, err := os.Create(fname)
	if err != nil {
		return
	}

	err = f.Close()
	if err == nil {
		filePath = path.Clean(fname)
	}
	return
}

func (w *filesystemWrapperImpl) EnsureMigrationDir() error {
	if stat, err := fs.Stat(w.fsys, "."); errors.Is(err, fs.ErrNotExist) {
		if err = w.requireWriteable(); err != nil {
			return err
		}

		return os.Mkdir(w.migrationDir, os.ModeDir|0775)
	} else if err != nil {
		return err
	} else if !stat.IsDir() {
		return &badMigrationPathError{
			isNotDir: true,
		}
	}

	return nil
}

func (w *filesystemWrapperImpl) ReadMigration(filename string) (sql string, err error) {
	f, err := w.fsys.Open(filename)
	if err != nil {
		return
	}

	migrationSql, err := ioutil.ReadAll(f)
	f.Close()
	if err != nil {
		return
	}

	sql = string(migrationSql)
	return
}
