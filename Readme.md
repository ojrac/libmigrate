libmigrate
============

[![Go Reference](https://pkg.go.dev/badge/github.com/ojrac/libmigrate.svg)](https://pkg.go.dev/github.com/ojrac/libmigrate)

A library for managing and running database migrations. For a command-line tool, see:

* [psql-migrate](https://github.com/ojrac/psql-migrate) for PostgreSQL
* [sqlite-migrate](https://github.com/ojrac/sqlite-migrate) for SQLite3

This library has a few goals:
* Production-friendly: Transactional DDL by default, with per-migration overrides
* Safe: Strict validation of migration files, for gaps or conflicts with the database
* Minimal: Bring your own database driver

Usage
-----

If you want to use libmigrate directly as a library, look at `psql-migrate` or
`sqlite-migrate`.

Migration files can be marked to run without a transaction with a prefix comment:

    -- migrate: no-transaction
