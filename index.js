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
import { fileURLToPath } from "node:url";

// Only schedule anything when run as the service entrypoint. Importing this module (the
// test suite does) must not start real timers or a second ingest loop.
const isEntrypoint = process.argv[1] !== undefined
  && fileURLToPath(import.meta.url) === process.argv[1];

const SUPABASE_URL = process.env.SUPABASE_URL;
const SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
// 2026-09-28: dedicated least-privilege job secret. Background-job edge functions
// (resolve-flight-routes, ingest-flyitalyadsb) now require it as x-sky-job-key; the
// value lives in Supabase Vault (sky_job_secret) and in this Railway variable only.
// Optional here so the other jobs keep running if it is ever unset.
const JOB_SECRET = process.env.SKY_JOB_SECRET;

// 2026-09-29 (live-truth phase 2): ingest-aircraft moves off pg_cron job 5 (*/2) to a 30 s
// tick here. The 2026-09-29 audit measured adsb.lol positions at seen_pos p50 0.3 s while our
// displayed observation age was ~123 s -- almost all of it our own cron phase plus the client's.
// Neither pg_cron nor Railway's own cron can schedule below one minute, so this has to be an
// in-process loop in a long-running service, which this already is.
//
// Disabled unless INGEST_AIRCRAFT_INTERVAL_MS is set to a positive number, so deploying this
// code changes nothing until the Railway variable is set -- which is what lets pg_cron job 5 be
// disabled FIRST and this be enabled SECOND, with no window where both are scheduling.
const INGEST_AIRCRAFT_INTERVAL_MS = Number(process.env.INGEST_AIRCRAFT_INTERVAL_MS ?? 0);
// Never let two starts come closer than this even if a cycle returns instantly.
const INGEST_AIRCRAFT_MIN_GAP_MS = Number(process.env.INGEST_AIRCRAFT_MIN_GAP_MS ?? 5_000);
// The edge function gives up on adsb.lol at 15 s and then writes; observed total 6.3-25.5 s.
// 60 s is generous enough not to abandon a healthy slow cycle, short enough that a wedged
// request cannot stall the loop for minutes.
const INGEST_AIRCRAFT_TIMEOUT_MS = Number(process.env.INGEST_AIRCRAFT_TIMEOUT_MS ?? 60_000);
// adsb.lol is a free community service that has been returning 429s and ~15 s timeouts. On any
// failure, back off exponentially and reset on the first success. Capped so we never stop
// entirely -- a feed that is down must keep being retried slowly, not abandoned, or feed health
// would start to look like aircraft disappearance.
const BACKOFF_BASE_MS = Number(process.env.INGEST_AIRCRAFT_BACKOFF_BASE_MS ?? 30_000);
const BACKOFF_MAX_MS = Number(process.env.INGEST_AIRCRAFT_BACKOFF_MAX_MS ?? 300_000);

if (!SUPABASE_URL || !SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.");
  process.exit(1);
}
if (!JOB_SECRET) {
  console.warn("SKY_JOB_SECRET is not set: guarded jobs (resolve-flight-routes, ingest-flyitalyadsb) will be rejected with 401.");
}

// Same offsets already proven collision-free on pg_cron tonight -- kept
// identical so behavior doesn't change, only which process triggers it.
const JOBS = [
  { name: "compact-aircraft-flights", schedule: "8-59/10 * * * *" },
];

async function callFunction(name, { timeoutMs = 240000, quiet = false } = {}) {
  const startedAt = Date.now();
  try {
    const res = await fetch(`${SUPABASE_URL}/functions/v1/${name}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${SERVICE_ROLE_KEY}`,
        ...(JOB_SECRET ? { "x-sky-job-key": JOB_SECRET } : {}),
      },
      // 2026-09-16: was 60000. Confirmed via real DB timestamps that
      // ingest-flyitalyadsb's writes were completing successfully server-side
      // ~40s after this timeout fired client-side -- no data was lost, but
      // the script was giving up and logging false failures too early.
      signal: AbortSignal.timeout(timeoutMs),
    });
    const body = await res.text();
    const shown = quiet ? body.slice(0, 200) : body.slice(0, 2000);
    console.log(`[${name}] ${res.status} in ${Date.now() - startedAt}ms -- ${shown}`);
    return { ok: res.ok, status: res.status, retryAfterMs: parseRetryAfter(res.headers.get("retry-after")) };
  } catch (err) {
    console.error(`[${name}] failed after ${Date.now() - startedAt}ms:`, err.message);
    return { ok: false, status: 0, retryAfterMs: null };
  }
}

function parseRetryAfter(value) {
  if (!value) return null;
  const seconds = Number(value);
  if (Number.isFinite(seconds) && seconds >= 0) return Math.min(seconds * 1000, BACKOFF_MAX_MS);
  const at = Date.parse(value);
  return Number.isFinite(at) ? Math.min(Math.max(0, at - Date.now()), BACKOFF_MAX_MS) : null;
}

// Abortable so the loop can be stopped promptly (the test suite does this; production never
// aborts). unref() keeps a pending wait from holding the process open on its own.
const sleep = (ms, signal) => new Promise((resolve) => {
  if (signal?.aborted) return resolve();
  const timer = setTimeout(done, ms);
  if (typeof timer.unref === "function") timer.unref();
  function done() {
    clearTimeout(timer);
    signal?.removeEventListener?.("abort", done);
    resolve();
  }
  signal?.addEventListener?.("abort", done, { once: true });
});

// Defence in depth against two schedulers running ingest-aircraft at once. The primary guarantee
// is operational (pg_cron job 5 is disabled before this loop is enabled); this is the check that
// would catch it anyway if anything re-enabled it, or if a second replica ever appeared.
// Deliberately fails OPEN: a transient PostgREST error must not silently stop ingestion, and the
// worst case it permits is the duplicate the primary guarantee already prevents.
async function anotherRunStartedWithin(ms) {
  try {
    const url = `${SUPABASE_URL}/rest/v1/feed_ingestion_runs`
      + `?feed_name=eq.adsb.lol&function_name=eq.ingest-aircraft`
      + `&select=started_at&order=started_at.desc&limit=1`;
    const res = await fetch(url, {
      headers: { apikey: SERVICE_ROLE_KEY, Authorization: `Bearer ${SERVICE_ROLE_KEY}` },
      signal: AbortSignal.timeout(10_000),
    });
    if (!res.ok) return false;
    const [row] = await res.json();
    if (!row?.started_at) return false;
    return Date.now() - Date.parse(row.started_at) < ms;
  } catch {
    return false;
  }
}

// One sequential loop. The next cycle is only scheduled after the previous await resolves, so
// this cannot overlap itself by construction -- there is no timer that can fire while a request
// is in flight. Request STARTS are therefore always at least INGEST_AIRCRAFT_MIN_GAP_MS apart,
// and at least INGEST_AIRCRAFT_INTERVAL_MS apart whenever a cycle finishes inside the interval.
async function ingestAircraftLoop({ signal } = {}) {
  let consecutiveFailures = 0;
  console.log(`Scheduled ingest-aircraft -> every ${INGEST_AIRCRAFT_INTERVAL_MS}ms (self-paced loop)`);
  while (!signal?.aborted) {
    const startedAt = Date.now();
    let backoffMs = 0;

    if (await anotherRunStartedWithin(INGEST_AIRCRAFT_INTERVAL_MS * 0.66)) {
      console.warn("[ingest-aircraft] skipped: another run started very recently (duplicate scheduler?)");
    } else {
      const outcome = await callFunction("ingest-aircraft", {
        timeoutMs: INGEST_AIRCRAFT_TIMEOUT_MS,
        quiet: true,
      });
      if (signal?.aborted) break;
      if (outcome.ok) {
        consecutiveFailures = 0;
      } else {
        consecutiveFailures += 1;
        backoffMs = outcome.retryAfterMs
          ?? Math.min(BACKOFF_BASE_MS * 2 ** (consecutiveFailures - 1), BACKOFF_MAX_MS);
        console.warn(`[ingest-aircraft] unhealthy (status ${outcome.status}), failure #${consecutiveFailures}, backing off ${backoffMs}ms`);
      }
    }

    const elapsed = Date.now() - startedAt;
    await sleep(Math.max(INGEST_AIRCRAFT_MIN_GAP_MS, INGEST_AIRCRAFT_INTERVAL_MS - elapsed) + backoffMs, signal);
  }
}

export { ingestAircraftLoop, anotherRunStartedWithin, parseRetryAfter };

if (isEntrypoint) {
  for (const job of JOBS) {
    cron.schedule(job.schedule, () => callFunction(job.name));
    console.log(`Scheduled ${job.name} -> ${job.schedule}`);
  }

  if (INGEST_AIRCRAFT_INTERVAL_MS > 0) {
    ingestAircraftLoop();
  } else {
    console.log("ingest-aircraft loop disabled (INGEST_AIRCRAFT_INTERVAL_MS unset or 0); pg_cron remains the scheduler.");
  }

  console.log("Project Sky scheduler running.");
}

// Keep the process alive -- node-cron's scheduler runs on the event loop,
// nothing else needed here. Railway restarts the service if it exits.
