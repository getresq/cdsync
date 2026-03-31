# How CDSync Works

This is the plain-English version of the runtime lifecycle.

## Core Idea

CDSync does two jobs:

1. Copy the current state of selected PostgreSQL tables into BigQuery.
2. Keep BigQuery updated as PostgreSQL changes over time.

Those are related, but they are not the same operation.

## Terms

- **Snapshot**: a consistent copy of the rows that exist right now.
- **WAL**: PostgreSQL's write-ahead log, which is the database's transaction diary.
- **CDC**: change data capture. CDSync reads logical changes from the WAL and applies them to BigQuery.
- **LSN**: a WAL position. CDSync saves this so it knows where to resume later.

## Lifecycle

```text
               +----------------------------+
               |         config.toml        |
               | tables / columns / BQ / CDC|
               +-------------+--------------+
                             |
                             v
               +-------------+--------------+
               |       load saved state     |
               | checkpoints / last WAL LSN |
               +-------------+--------------+
                             |
                   first run?|
                 yes         | no
                  |          |
                  v          v
      +-----------+--+   +---+------------------+
      | take snapshot |   | resume from saved   |
      | of current DB |   | WAL/LSN             |
      +------+--------+   +----------+----------+
             |                      |
             v                      |
      +------+--------+             |
      | write batch   |             |
      | files to GCS  |             |
      +------+--------+             |
             |                      |
             v                      |
      +------+--------+             |
      | BigQuery load |             |
      | jobs / merge  |             |
      +------+--------+             |
             +----------+-----------+
                        |
                        v
               +--------+---------+
               | follow WAL via   |
               | CDC and apply    |
               +--------+---------+
                        |
                        v
               +--------+---------+
               | save new state   |
               | and last WAL LSN |
               +------------------+
```

## First Run From Scratch

On the first run, CDSync has no saved WAL position.

It does this:

1. Read the config file.
2. Discover the selected tables and columns.
3. Create a **consistent snapshot** of the source tables.
4. Copy those rows into BigQuery.
5. Start CDC from the matching WAL position.
6. Apply inserts, updates, and deletes that happened after the snapshot started.
7. Save the current state, including the last processed WAL position.

In plain terms:

- Snapshot gives CDSync the old data.
- CDC gives CDSync the new changes after that point.
- Together, they produce a correct BigQuery copy.

## Future Runs

On later runs, CDSync should not start from zero.

It does this:

1. Read the config file.
2. Load the saved state.
3. Resume from the last saved WAL position.
4. Apply only the new changes.
5. Save the new WAL position again.

In plain terms:

- first run = bulk copy + catch-up
- later runs = just catch-up

## What Happens In Polling Mode Versus CDC Mode

### Polling mode

CDSync repeatedly queries source tables and pages through them using an `updated_at` column and primary key.

This is simpler, but it depends on:

- a trustworthy `updated_at` column
- predictable paging
- no hidden hard deletes unless they are modeled as soft deletes

### CDC mode

CDSync reads logical replication changes from PostgreSQL's WAL.

This is stronger for:

- low-latency change capture
- real delete events
- avoiding missed updates caused by bad timestamps

It is also operationally more complex because it depends on:

- logical replication
- publications
- replication slots
- saved WAL positions

## What Happens When You Add A New Table To The Config

This depends on the sync mode.

### Polling connections

If a new polling table is added and the connection runs again:

- CDSync discovers the new table
- creates the destination table if needed
- backfills it by paging from the beginning

So polling mode is naturally tolerant of "new table added later".

### CDC connections

For CDC connections, adding a new table to the config is not enough by itself.

You also need:

- the table to be added to the PostgreSQL publication
- a snapshot/backfill for that new table

Why:

- the saved WAL position only tells CDSync how to read **future** changes
- it does not automatically give the historical rows that already existed before the table was added to the config

So the safe operational rule is:

- **new CDC table -> update publication -> run a targeted resync/full snapshot for that table**

Until CDSync grows automatic targeted table backfill on config change, that should be treated as an operator action.

## What Happens When You Remove A Table From The Config

CDSync stops syncing it.

It does **not** automatically delete the BigQuery table or clear its data.

That is intentional. Removal from config means "stop managing this table", not "destroy destination history".

## What Happens When You Add Or Change Columns

### Additive changes

If `schema_changes = auto`:

- new source columns can usually be added to BigQuery automatically
- historical rows remain null for the new column unless you explicitly resync/backfill

### Non-destructive changes

Examples:

- column removed
- column renamed

With the current raw replication policy, these are handled conservatively:

- removed columns are not automatically dropped from BigQuery
- a rename is treated as "new column added, old column left in place"
- nullability-only changes are logged but do not force a resync

That keeps the raw destination non-destructive and avoids guessing at schema intent.

### Incompatible changes

Examples:

- type changed in a way that changes the destination type
- primary key changed

These are treated as incompatible changes and usually require:

- failure with operator intervention
- or a resync, depending on policy

## What Happens On Failure

CDSync saves state as it goes.

In a durable deployment, that means:

- after a crash or restart
- it should resume from the last good checkpoint / WAL position

Important caveat:

- if state is stored on ephemeral task storage, resumability only lasts as long as the task does
- for production deployments, state must be on durable storage

## Current Important Operational Reality

Today, the most important practical rules are:

- first CDC run = snapshot + WAL catch-up
- later CDC runs = resume from saved WAL position
- new CDC tables need a deliberate backfill step
- config changes are not hot-reloaded by the running process
- durable state storage matters as much as the replication logic
