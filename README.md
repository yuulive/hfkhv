

# Environment variables

- `PGFINE_CONNECTION_STRING` credentials for altering target db
- `PGFINE_ADMIN_CONNECTION_STRING` credentials for creating a new database (usually postgres db with user postgres) refereced above.
- `PGFINE_DIR` defaults to `./pgfine`

Connection strings: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING

# Workflow

## Install `pgfine`

`pgfine` is not yet published at crates.io so you should install it from git repository:
```bash
git clone https://gitlab.com/mrsk/pgfine
cargo install --path ./pgfine
```

## Create new project

- Choose version controlled directory.
- Create git-ignored `env-local-db-0.sh` (as an example) file like this:

```bash
export PGFINE_CONNECTION_STRING="..."
export PGFINE_ADMIN_CONNECTION_STRING="..."
# export PGFINE_DIR="./pgfine"
```
- Run `pgfine init`
- Modify newly created `./pgfine/create.sql` if needed.


## Create a database

- Modify `./pgfine/create/*` scripts if needed.
- Setup environment and run:

```bash
source env-local-db-0.sh
pgfine migrate
```


## Making changes to database

- Apply any changes to database schema objects in `./pgfine/**/*.sql`.
- All the chagnes related with tables should be implemented via `./pgfine/migrations/*` scripts.
- For all other objects (not tables) it is enough to modify a related create script. (ex. `./pgfine/views/public.view0.sql`)
- Filenames for database objects must be of format `<schema>.<name>.sql`.
- Setup environment and run
```bash
source env-local-db-0.sh
pgfine migrate
```

- Test your fresh db maybe.
- Commit all files to version control.


Table constraints should be stored along with tables. You will have a problem if constraints form circular dependencies.

# Commands

## `pgfine init [./pgfine]`

- Initializes empty project with empty directories and `./pgfine/create.sql` example script.


## `pgfine create`


- Everything else is done using `PGFINE_CONNECTION_STRING` credentials.
- All database schema objects are created using script files.
- `pgfine_objects` and `pgfine_migrations` tabls are created in default schema with the last migration_id and object hashes.


## `pgfine migrate`

- Uses `PGFINE_ADMIN_CONNECTION_STRING` to create a new database and role referenced in `PGFINE_CONNECTION_STRING` using `/pgfine/create/*.sql` scripts (if they do not exist).
- Uses `PGFINE_CONNECTION_STRING` credentials to connect to a working database.
- Applies new scripts in `./pgfine/migrations/` and updates version in `pgfine` table. (This is skipped if the database is newly created).
- Scans all objects in `./pgfine/` and builds the dependency tree.
- Calculates hash of each object script file.
- Attempts to update each object whose script hash does not match the one in the `pgfine` table (or drop the object if it was deleted).
- Updates `pgfine_objects` table with newest information.
- Inserts into `pgfine_migrations` executed migration scripts.


## `pgfine drop --no-joke`

- Uses `PGFINE_ADMIN_CONNECTION_STRING` credentials to connect to database.
- Uses executes `/pgfine/drop/*.sql` scripts to drop database and role.


# Structure

## Files
- `./pgfine/create/`
- `./pgfine/drop/`
- `./pgfine/tables/`
- `./pgfine/views/`
- `./pgfine/functions/`
- `./pgfine/roles/`
- `./pgfine/migrations/`

## `pgfine_objects` table

Contains a list of managed pgfine objects and their hashes.

## `pgfine_migrations` table

Contains a list of executed migrations. Selecting the max value should reveal the current state of database.


## Assumptions

- Each script filename in `tables`, `views` and `functions` correspond to database object. This information is used to track dependencies between objects (using simple text match)
- Each file in `./pgfine/migrations/` has format `<comparable_version_string>.*`
- During each migration all views and functions which hashes do not match with `object_md5` will be updated. (online if possible)
- Scripts are modified during `pgfine migrate` execution
  - First we attempt `CREATE OR REPLACE`. If it fails and `--offline` flag is provided we do `DROP` and `CREATE` including all the dependencies.


- does not support functions with same name different args.
- empty string is the name of the first migration

# Plan

- [x] implement `pgfine init`
- [x] implement `pgfine create`
- [x] implement `pgfine drop`
- [ ] implement `pgfine migrate`
- [ ] support for circular constraints (by adding `./pgfine/constraints`)
- [ ] support for initial data
- [ ] support tls
- [ ] default drop script to disconnect users
- [ ] publish to crates.io
- [ ] example projects at `./example/`
- [x] case insensitive
- [ ] configurable search schemas
- [x] search dependencies by matching whole word
- [ ] make execute order deterministic
- [ ] operations in single transaction if possible
- [ ] make README.md readable

