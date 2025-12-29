# Lockplane

Easy postgres schema management.

Status: Alpha software! this is still a playground for learning golang, postgres & ai tools

[![Test](https://github.com/lockplane/lockplane/actions/workflows/test.yml/badge.svg)](https://github.com/lockplane/lockplane/actions/workflows/test.yml)
[![codecov](https://codecov.io/github/lockplane/lockplane/graph/badge.svg?token=JP0QINP1G1)](https://codecov.io/github/lockplane/lockplane)

## 1. Install

```bash
go install github.com/lockplane/lockplane
```

## 2. Create a config file

The config file is a TOML file named `lockplane.toml` in the root of the project.  It should look like this:

```toml
[environments.local]
postgres_url = "postgresql://postgres:postgres@localhost:5432/postgres"
```

At this time, only a single environment called `local` is supported.

## 3. Create a schema file

Add to `schema/users.lp.sql`:

```sql
CREATE TABLE users (
  id BIGINT PRIMARY KEY,
  email TEXT NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

For now, schema files must be in the root of the `schema/` directory, and must
end in `.lp.sql`.

Lockplane supports PostgreSQL schemas. Tables with the same name can exist in different schemas:

```sql
CREATE TABLE public.users (id BIGINT PRIMARY KEY);
CREATE TABLE auth.users (id BIGINT PRIMARY KEY, email TEXT);
```

If no schema is specified in `CREATE TABLE`, Lockplane assumes the `public` schema. If your database uses a different default schema, you should explicitly qualify table names in your `.lp.sql` files.

## 4. Check the schema for issues

```bash
lockplane check schema/
```

## 4. Apply changes

```bash
# TODO
npx lockplane apply
```

## PostgreSQL Version Support

Lockplane is tested against **PostgreSQL 17**.

## Postgres Feature Support

### DDL Operations

Feature | SQL Parsing | DB Introspection | SQL Generation
-- | -- | -- | --
CREATE TABLE | ✅ | ✅ | ✅
DROP TABLE | ✅ | ✅ | ✅
ALTER TABLE | ❌ | N/A | ❌
ENABLE/DISABLE ROW LEVEL SECURITY | ✅ | ✅ | ✅

### Constraints

Feature | SQL Parsing | DB Introspection | SQL Generation
-- | -- | -- | --
NOT NULL | ✅ | ✅ | ✅
PRIMARY KEY | ✅ | ✅ | ✅
UNIQUE | ❌ | ❌ | ❌
FOREIGN KEY | ❌ | ❌ | ❌
CHECK | ❌ | ❌ | ❌
DEFAULT | ✅ | ✅ | ✅

### Data Types

Feature | SQL Parsing | DB Introspection | SQL Generation
-- | -- | -- | --
**Numeric** |
Integer types (SMALLINT, INTEGER, BIGINT) | ✅ | ✅ | ✅
Serial types (SMALLSERIAL, SERIAL, BIGSERIAL) | ✅ | ✅ | ✅
Floating point (REAL, DOUBLE PRECISION) | ✅ | ✅ | ✅
Numeric/Decimal (NUMERIC, DECIMAL) | ✅ | ✅ | ✅
Money (MONEY) | ❌ | ❌ | ❌
**Character** |
Text types (TEXT, VARCHAR, CHAR) | ✅ | ✅ | ✅
**Date/Time** |
Date (DATE) | ✅ | ✅ | ✅
Time (TIME, TIMETZ) | ✅ | ✅ | ✅
Timestamp (TIMESTAMP, TIMESTAMPTZ) | ✅ | ✅ | ✅
Interval (INTERVAL) | ❌ | ❌ | ❌
**Boolean** |
Boolean (BOOLEAN) | ✅ | ✅ | ✅
**UUID** |
UUID (UUID) | ✅ | ✅ | ✅
**JSON** |
JSON (JSON, JSONB) | ✅ | ✅ | ✅
JSONPath (JSONPATH) | ❌ | ❌ | ❌
**Binary** |
Binary Data (BYTEA) | ✅ | ✅ | ✅
**XML** |
XML (XML) | ❌ | ❌ | ❌
**Arrays** |
Array types (INT[], TEXT[], etc.) | ✅ | ❌ | ✅
**Geometric** |
Point, Line, Box, Path, Polygon, Circle | ❌ | ❌ | ❌
**Network** |
INET, CIDR, MACADDR, MACADDR8 | ❌ | ❌ | ❌
**Bit Strings** |
BIT, VARBIT | ❌ | ❌ | ❌
**Text Search** |
TSVECTOR, TSQUERY | ❌ | ❌ | ❌
**Other** |
PG_LSN, PG_SNAPSHOT | ❌ | ❌ | ❌

## Lockplane with Supabase

1. Initialize the project with lockplane

```
lockplane init
```

2. Find you connection string

```
npx supabase status
# look for the DATABASE_URL
```

3. Edit lockplane.toml to add the connection string

```
[environments.local]
postgres_url = "your connection string"
```
