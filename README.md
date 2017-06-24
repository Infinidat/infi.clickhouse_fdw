Overview
========
This project provides a PostgreSQL Foreign Data Wrapper (FDW) for the [ClickHouse database](https://clickhouse.yandex/). It allows ClickHouse tables to be queried as if they were stored in PostgreSQL.

The project is based on [Multicorn](http://multicorn.org/) and [infi.clickhouse_orm](http://github.com/Infinidat/infi.clickhouse_orm/).

Features
--------
- Automatic generation of foreign table definitions
- Sorting and filtering is done inside ClickHouse 
- Hints about each column are provided to the query planner
- EXPLAIN is supported (shows the query sent to ClickHouse)

Limitations
-----------
- Supports only SELECT, no updates
- Does not support complex ClickHouse datatypes (arrays, enums, nested tables)

Installation
============
First, [install Multicorn](http://multicorn.org/#idinstallation). Then run:

    easy_install infi.clickhouse_fdw

Usage
=====
The simplest way to use this project is the included `generate_clickhouse_fdw` script. It generates all the required SQL statements, which you can then pipe to `psql`:
```
$ generate_clickhouse_fdw --help

Usage: generate_clickhouse_fdw [OPTIONS] [TABLE]...

  Generates SQL statements for defining Foreign Data Wrappers for ClickHouse
  tables.

  If no table names are specified, wrappers are generated for all tables in
  the ClickHouse database. If table names are given, wrappers will be
  generated only for those tables unless --exclude is present, in which case
  all tables EXCEPT those listed will be processed.

  If --pg-ver=9.4 is specified, explicit CREATE FOREIGN TABLE is generated
  for each table. Otherwise a single IMPORT FOREIGN SCHEMA statement is
  used.

Options:
  --db-url TEXT           ClickHouse URL [http://localhost:8123/]
  --db-name TEXT          ClickHouse database name [default]
  --server-name TEXT      FDW server name [clickhouse_server]
  --schema-name TEXT      Schema to define the tables in [public]
  --pg-ver [9.4|9.5|9.6]  PostgreSQL version [9.6]
  --exclude               Generate all tables except those named
  --help                  Show this message and exit.
```

For example:
```
$ generate_clickhouse_fdw --db-name=events table1 table2 table3

CREATE EXTENSION IF NOT EXISTS multicorn;

CREATE SERVER clickhouse_server 
FOREIGN DATA WRAPPER multicorn
OPTIONS (
  wrapper 'infi.clickhouse_fdw.main.ClickHouseDataWrapper'
);

IMPORT FOREIGN SCHEMA "events"
FROM SERVER "clickhouse_server" 
LIMIT TO ('table1', 'table2', 'table3')
INTO "public"
OPTIONS ( 
    db_url 'http://localhost:8123/', 
    db_name 'events'
);
```

Any warnings that are detected by the script are printed to `stderr`.

Contributing
============
To set up a development version, clone the project and run the following commands:
    
    easy_install -U infi.projector
    cd infi.clickhouse_fdw
    projector devenv build
    
A `setup.py` file will be generated, which you can use to install the development version of the package:

    python setup.py develop
    
Any changes you make to the code will take effect only after you restart PostgreSQL.
