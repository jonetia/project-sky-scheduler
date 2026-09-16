# Project Sky Scheduler

Runs four high-frequency Project Sky jobs on internal timers instead of
Postgres's `pg_cron`. This service holds **no ingestion or detection
logic** -- it calls the same Supabase edge functions `pg_cron` already
called, over HTTP, on the same schedule. Moving the scheduler here avoids
a real, confirmed constraint on the underlying database instance
(`max_worker_processes=6` vs `cron.max_running_jobs=32`, both
postmaster-context settings that can't be changed without a paid compute
tier), which was causing intermittent `job startup timeout` failures even
for jobs that weren't colliding with each other.

## Jobs handled here

- `ingest-flyitalyadsb` -- every 5 min
- `compact-aircraft-flights` -- every 10 min
- `resolve-flight-routes` -- every 10 min
- `detect-go-arounds` -- every 15 min

Lower-frequency jobs (weather, NAT tracks, orbital, vantage-point rollup)
stay on `pg_cron` deliberately. They rarely collided, so moving them here
wouldn't fix anything -- it would just mean one crashed process taking
down jobs that currently fail independently of each other.

## Setup

```
npm install
cp .env.example .env   # fill in real values locally; never commit .env
npm start
```

## Deploying

Set `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` as **Railway
environment variables** in the service dashboard -- not in any file in
this repo. This repo is public; the service role key grants full,
unrestricted database access and must never be committed.
