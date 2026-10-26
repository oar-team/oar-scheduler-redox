# Crate oar-scheduler-db

## Overview

This crate provide the integration with the OAR3 database using `sqlx` and `sea-query`.
Queries are first built using sea-query to the target database (either PostgreSQL or SQLite), and are then executed using sqlx.

### Notes about dynamic database schema

The original OAR3 database schema is dynamic, meaning that tables and columns can be added or removed at any time.
Only the `resources` table is concerned by this. Indeed, sysadmins can define custom rows in the `resources` table to act as hierarchy identifiers.
For example, if the sysadmin wants to define a hierarchy `switch/node/cpu/core`, they can add the columns `switch`, `node`, `cpu` and `core` to the
`resources` table, set the configuration value `HIERARCHY_LABELS=resource_id,network_address,core,cpu,host,mem`, and then make requests using these
hierarchy levels.

To be compatible with the original OAR3 database schema, this crate support dynamic schema. Then no ORM can be used, and we made the choice to use
`sqlx` with `sea-query` to build queries programmatically for any of the two supported databases (PostgreSQL and SQLite).

### Notes about asynchronous code

`sqlx` is an asynchronous-only library. However, the scheduler is single-threaded and synchronous. Then this crate offers an entirely synchronous API
to
isolate the rest of the scheduler from asynchronous code. It uses `tokio` under the hood only building a tokio runtime at the `Session`
initialization, and then using that runtime to do `block_on` calls to run async code.

## Usage

First create an instance of a `Session` struct:

```rust
let database_url = "postgres://user:password@localhost/oar_db"; // Either PostgreSQL or SQLite
let session = oar_scheduler_db::Session::new(database_url).await?;
```

The session must then be passed to any function call involving database access (see module [`oar_scheduler_db::model`](src/model)).
