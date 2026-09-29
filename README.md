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

Set `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY` and `SKY_JOB_SECRET` as **Railway
environment variables** in the service dashboard -- not in any file in
this repo. This repo is public; the service role key grants full,
unrestricted database access and must never be committed.

`SKY_JOB_SECRET` is the dedicated background-job secret (Supabase Vault `sky_job_secret`).
It is sent as the `x-sky-job-key` header; `resolve-flight-routes` and `ingest-flyitalyadsb`
reject calls without it. Never commit its value.

## ingest-aircraft 30 s loop (live-truth phase 2, 2026-09-29)

`ingest-aircraft` moved off pg_cron job 5 (`*/2 * * * *`) to a self-paced 30 s loop in this
service. The 2026-09-29 audit measured adsb.lol positions at `seen_pos` p50 0.3 s while Sky's
displayed observation age was ~123 s — almost all of it our own cron phase plus the client's.
Neither pg_cron nor Railway cron schedules below one minute, so it has to be an in-process loop.

As with the four jobs above, this reimplements nothing: it is a plain HTTP call to the same
`ingest-aircraft` edge function pg_cron called via `net.http_post`. The function is unchanged.

### Why duplicate execution cannot happen

Three independent mechanisms:

1. **Structural — the loop cannot overlap itself.** It is one sequential `while` loop, not a
   timer. The next cycle is only scheduled after the previous `await` resolves, so no tick can
   fire while a request is in flight. Request *starts* are therefore always ≥
   `INGEST_AIRCRAFT_MIN_GAP_MS` apart, and ≥ `INGEST_AIRCRAFT_INTERVAL_MS` apart whenever a cycle
   finishes inside the interval. Covered by the `healthy` and `slow cycle` tests, which assert
   `maxInFlight === 1` even when every cycle overruns the interval.
2. **Operational — the loop is off by default.** It only runs when
   `INGEST_AIRCRAFT_INTERVAL_MS` is set to a positive number. Deploying this code changes
   nothing. That is what allows pg_cron job 5 to be disabled *first* and this to be enabled
   *second*, with no window in which both are scheduling.
3. **Defensive — a pre-flight duplicate check.** Each tick queries the most recent
   `feed_ingestion_runs` row for `adsb.lol`/`ingest-aircraft` and skips if one started within the
   last ⅔ interval. This catches a re-enabled pg_cron job, or a second Railway replica, whoever
   the other scheduler is. It deliberately **fails open**: a transient PostgREST error must not
   silently stop ingestion, and the worst it permits is the duplicate mechanism 2 prevents.

### Upstream courtesy

adsb.lol is a free community service that has returned 429s and ~15 s timeouts. Any non-2xx or
timeout backs off exponentially (30 s → 60 → 120 → 240, capped at 300 s) and resets on the first
success; `Retry-After` is honoured when supplied. Retries never stop entirely — a feed that is
down must keep being retried slowly, or feed health starts to look like aircraft disappearance.

### Enable / rollback

```sh
# Enable (ONLY after pg_cron job 5 is confirmed inactive):
#   Supabase:  update cron.job set active = false where jobid = 5;
#   Railway:   set INGEST_AIRCRAFT_INTERVAL_MS=30000 on project-sky-scheduler

# Roll back to the 120 s pg_cron schedule:
#   Railway:   remove INGEST_AIRCRAFT_INTERVAL_MS (or set it to 0) -> loop stops on restart
#   Supabase:  update cron.job set active = true where jobid = 5;
```

Order matters in both directions: never have both active. Verify with
`select jobid, active from cron.job where jobid = 5;` and the service logs, which state on
startup whether the loop is enabled.

### Tests

`npm test` (no dependencies beyond Node's built-in runner). Runs the loop against a stub Supabase
and asserts non-overlap, start spacing, exponential backoff, `Retry-After`, timeout-as-failure,
and the duplicate-scheduler skip.
