DAS Foreign Data Wrapper for PostgreSQL
=========================================

This is a foreign data wrapper (FDW) to connect [PostgreSQL](https://www.postgresql.org/)
to [DAS][1].

It is derived from the [MySQL FDW available here](http://github.com/enterprisedb/mysql_fdw/).

Contents
--------

1. [Features](#features)
2. [Supported platforms](#supported-platforms)
3. [Installation](#installation)
4. [Usage](#usage)

Features
--------
### Common features & enhancements

The following enhancements are added to the latest version of
``das_fdw``:

#### Write-able FDW
The user can now an insert, update, and delete
statements for the foreign tables using the `das_fdw`. It uses the PG
type casting mechanism to provide opposite type casting between DAS
and PG data types.

## Pushdowning

#### WHERE clause push-down
The FDW will push-down the foreign table where clause to
the foreign server. The where condition on the foreign table will be
executed on the foreign server hence there will be fewer rows to bring
across to PostgreSQL. This is a performance feature.

#### Column push-down
The FDW does the column push-down and only
brings back the columns that are part of the select target list. This is
a performance feature.

#### JOIN push-down
The FDW supports join push-down. The joins between two
foreign tables from the same remote DAS server are pushed to a remote
server, instead of fetching all the rows for both the tables and
performing a join locally, thereby enhancing the performance. Currently,
joins involving only relational and arithmetic operators in join-clauses
are pushed down to avoid any potential join failure. Also, only the
INNER and LEFT/RIGHT OUTER joins are supported, and not the FULL OUTER,
SEMI, and ANTI join. This is a performance feature.

#### AGGREGATE push-down
The FDW supports aggregate push-down. Push aggregates to the
remote DAS server instead of fetching all of the rows and aggregating
them locally. This gives a very good performance boost for the cases
where aggregates can be pushed down. The push-down is currently limited
to aggregate functions min, max, sum, avg, and count, to avoid pushing
down the functions that are not present on the DAS server. Also,
aggregate filters and orders are not pushed down.

#### ORDER BY push-down
The FDW supports order by push-down. If possible, push order by
clause to the remote server so that we get the ordered result set from the
foreign server itself. It might help us to have an efficient merge join.

#### LIMIT OFFSET push-down
The FDW supports limit offset push-down. Wherever possible,
perform LIMIT and OFFSET operations on the remote server. This reduces
network traffic between local PostgreSQL and remote DAS servers.

Supported platforms
-------------------

`das_fdw` was developed on Linux, and should run on any
reasonably POSIX-compliant system.

Please refer to [das_fdw_documentation][6].

Installation
------------

TODO

Usage
-----

## CREATE SERVER options

`das_fdw` accepts the following options via the `CREATE SERVER` command:

- **das_url** as *string*, mandatory

  Address of the DAS server, e.g. 'localhost:50051'

- **das_type** as *string*, optional

  The of the DAS server, e.g. 'salesforce'