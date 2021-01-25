

# Environment variables

- `PGFINE_CONNECTION_STRING` credentials for altering target db
- `PGFINE_SUPER_CONNECTION_STRING` credentials for creating a new database (usually postgres db with user postgres) refereced above.
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
export PGFINE_SUPER_CONNECTION_STRING="..."
# export PGFINE_DIR="./pgfine"
```
- Run `pgfine init`
- Modify newly created `./pgfine/create.sql` if needed.


## Create new database

- Modify `./pgfine/create.sql` if needed.
- Setup environment and run:

```bash
source env-local-db-0.sh
pgfine create
```


## Making changes to database

- Apply any changes to database schema objects in `./pgfine/**/*.sql`.
- Create new file in `./pgfine/migrations` if tables were created or modified.
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

- Uses `PGFINE_SUPER_CONNECTION_STRING` to create a new database and role referenced in `PGFINE_CONNECTION_STRING` using `/pgfine/create/*.sql` scripts.
- Everything else is done using `PGFINE_CONNECTION_STRING` credentials.
- All database schema objects are created using script files.
- `pgfine` table is created in default schema with latest version number and object hashes.


## `pgfine migrate`

- Uses `PGFINE_CONNECTION_STRING` credentials to connect to database.
- Applies new scripts in `./pgfine/migrations/` and updates version in `pgfine` table.
- Scans all objects in `./pgfine/` and builds the dependency tree.
- Calculates hash of each object script file.
- Attempts to update each object whose script hash does not match the one in the `pgfine` table (or drop the object if it was deleted).
- Updates `pgfine` table with newest hashes.


## `pgfine drop --no-joke`

- Uses `PGFINE_SUPER_CONNECTION_STRING` credentials to connect to database.
- Uses executes `/pgfine/drop/*.sql` scripts to drop database and role.


# Structure

## Files
- `./pgfine/create.sql`
- `./pgfine/tables/`
- `./pgfine/views/`
- `./pgfine/functions/`
- `./pgfine/roles/`
- `./pgfine/migrations/`

## `pgfine` table

```sql
select * from pgfine;
```

should return single json object:

```json
{
    "version": "000123",
    "object_md5": {
        "public.todo_item": "123_md5",
        "public.user": "234_md5"
    },
}
```

## Assumptions

- Each script filename in `tables`, `views` and `functions` correspond to database object. This information is used to track dependencies between objects (using simple text match)
- Each file in `./pgfine/migrations/` has format `<comparable_version_string>.*`
- During each migration all views and functions which hashes do not match with `object_md5` will be updated. (online if possible)
- Scripts are modified during `pgfine migrate` execution
  - First we attempt `CREATE OR REPLACE`. If it fails and `--offline` flag is provided we do `DROP` and `CREATE` including all the dependencies.



# Plan

- [x] implement `pgfine init`
- [ ] implement `pgfine create`
- [x] implement `pgfine drop`
- [ ] implement `pgfine migrate`
- [ ] support for circular constraints (by adding `./pgfine/constraints`)
- [ ] support for initial data
- [ ] support tls
- [ ] default drop script to disconnect users
- [ ] publish to crates.io

