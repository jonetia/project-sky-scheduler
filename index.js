// Project Sky -- high-frequency job scheduler.
//
// Moved off Postgres's pg_cron after a real, confirmed root cause: the
// underlying instance has max_worker_processes=6 but cron.max_running_jobs
// was set to 32 -- both are postmaster-context settings, not fixable
// without a paid compute tier. pg_cron jobs were failing with
// "job startup timeout" even when NOT colliding with each other, because
// they were competing for a background-worker slot pool shared with
// everything else Postgres does internally.
//
// This does NOT reimplement any ingestion/detection logic. Every job here
// is a plain HTTP call to the SAME Supabase edge function pg_cron used to
// call via net.http_post -- the edge functions are unchanged. This is a
// scheduler relocation, not a rewrite. Railway's own network calls don't
// touch Postgres's worker-process pool at all, which is what actually
// fixes the collision problem for these four jobs specifically.
//
// Kept to exactly the four highest-frequency jobs that were the real
// collision risk. Lower-frequency jobs (weather, NAT tracks, orbital,
// vantage-point rollup) stay on pg_cron deliberately -- moving them here
// wouldn't fix anything (they rarely collided) and would mean one crashed
// Railway process taking down jobs that currently fail independently.
//
// SECURITY: this repo is public. SUPABASE_SERVICE_ROLE_KEY grants full,
// unrestricted database access and must NEVER be committed -- it is read
// from the environment only, set as a Railway variable, not a file in
// this repo. See .env.example for what's needed without real values.

import cron from "node-cron";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.");
  process.exit(1);
}

// Same offsets already proven collision-free on pg_cron tonight -- kept
// identical so behavior doesn't change, only which process triggers it.
const JOBS = [
  { name: "ingest-flyitalyadsb", schedule: "2-59/5 * * * *" },
  { name: "compact-aircraft-flights", schedule: "3-59/10 * * * *" },
  { name: "resolve-flight-routes", schedule: "9-59/10 * * * *" },
  { name: "detect-go-arounds", schedule: "6-59/15 * * * *" },
];

async function callFunction(name) {
  const startedAt = Date.now();
  try {
    const res = await fetch(`${SUPABASE_URL}/functions/v1/${name}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${SERVICE_ROLE_KEY}`,
      },
      // 2026-09-16: was 60000. Confirmed via real DB timestamps that
      // ingest-flyitalyadsb's writes were completing successfully server-side
      // ~40s after this timeout fired client-side -- no data was lost, but
      // the script was giving up and logging false failures too early.
      signal: AbortSignal.timeout(150000),
    });
    const body = await res.text();
    console.log(`[${name}] ${res.status} in ${Date.now() - startedAt}ms -- ${body.slice(0, 200)}`);
  } catch (err) {
    console.error(`[${name}] failed after ${Date.now() - startedAt}ms:`, err.message);
  }
}

for (const job of JOBS) {
  cron.schedule(job.schedule, () => callFunction(job.name));
  console.log(`Scheduled ${job.name} -> ${job.schedule}`);
}

console.log("Project Sky scheduler running.");

// Keep the process alive -- node-cron's scheduler runs on the event loop,
// nothing else needed here. Railway restarts the service if it exits.
