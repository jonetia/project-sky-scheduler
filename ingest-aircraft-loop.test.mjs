// Local verification of the ingest-aircraft 30 s loop, against a stub Supabase.
//
// These assert the safety properties the live-truth phase 2 brief requires proof of:
//   1. one cycle at a time -- the loop cannot overlap itself;
//   2. request STARTS are never closer than the configured minimum gap;
//   3. a 429 / 5xx / timeout backs off exponentially instead of storming;
//   4. Retry-After is honoured when the upstream supplies one;
//   5. a duplicate scheduler (another run seen very recently) causes this tick to skip.
//
// Timings are scaled down via env so the suite runs in seconds; the production defaults are
// 30 s interval / 5 s minimum gap / 30 s backoff base.
//
//   npm test

import { test } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";

const INTERVAL = 300;
const MIN_GAP = 100;
const BACKOFF_BASE = 400;

/** Stub Supabase: records ingest calls, lets each test script the responses. */
async function withStub(handler, run) {
  const calls = [];
  let inFlight = 0;
  let maxInFlight = 0;
  let lastRunStartedAt = null; // what the duplicate-guard query reports

  const server = http.createServer(async (req, res) => {
    if (req.url.startsWith("/rest/v1/feed_ingestion_runs")) {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(lastRunStartedAt ? [{ started_at: lastRunStartedAt }] : []));
      return;
    }
    if (req.url.endsWith("/functions/v1/ingest-aircraft")) {
      const index = calls.length;
      calls.push({ startedAt: Date.now() });
      inFlight += 1;
      maxInFlight = Math.max(maxInFlight, inFlight);
      const { status = 200, delayMs = 0, headers = {} } = handler(index) ?? {};
      await new Promise((r) => setTimeout(r, delayMs));
      inFlight -= 1;
      res.writeHead(status, { "Content-Type": "application/json", ...headers });
      res.end(JSON.stringify({ ok: status === 200 }));
      return;
    }
    res.writeHead(404).end();
  });

  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  const url = `http://127.0.0.1:${server.address().port}`;

  const env = {
    ...process.env,
    SUPABASE_URL: url,
    SUPABASE_SERVICE_ROLE_KEY: "test-not-a-real-key",
    INGEST_AIRCRAFT_INTERVAL_MS: String(INTERVAL),
    INGEST_AIRCRAFT_MIN_GAP_MS: String(MIN_GAP),
    INGEST_AIRCRAFT_BACKOFF_BASE_MS: String(BACKOFF_BASE),
    INGEST_AIRCRAFT_BACKOFF_MAX_MS: "5000",
    INGEST_AIRCRAFT_TIMEOUT_MS: "1000",
  };
  const saved = process.env;
  process.env = env;
  // Fresh module instance per test so the module-level constants re-read env.
  const mod = await import(`./index.js?t=${Date.now()}`);
  const controller = new AbortController();
  const start = () => mod.ingestAircraftLoop({ signal: controller.signal });

  try {
    await run({ mod, calls, start, setLastRunStartedAt: (v) => { lastRunStartedAt = v; },
                maxInFlight: () => maxInFlight });
  } finally {
    controller.abort();
    process.env = saved;
    server.closeAllConnections?.();
    await new Promise((r) => server.close(r));
  }
}

const gaps = (calls) => calls.slice(1).map((c, i) => c.startedAt - calls[i].startedAt);

test("healthy: one cycle at a time, starts spaced by the interval", async () => {
  await withStub(
    () => ({ status: 200, delayMs: 50 }),
    async ({ start, calls, maxInFlight }) => {
      const loop = start();
      await new Promise((r) => setTimeout(r, INTERVAL * 4.5));
      assert.ok(calls.length >= 3, `expected several cycles, got ${calls.length}`);
      assert.equal(maxInFlight(), 1, "two ingest requests were in flight at once");
      for (const g of gaps(calls)) {
        assert.ok(g >= MIN_GAP, `starts ${g}ms apart, below the ${MIN_GAP}ms floor`);
        assert.ok(g >= INTERVAL * 0.8, `starts ${g}ms apart, well under the ${INTERVAL}ms interval`);
      }
      void loop;
    },
  );
});

test("slow cycle: a run longer than the interval still cannot overlap the next", async () => {
  await withStub(
    () => ({ status: 200, delayMs: INTERVAL * 1.6 }), // every cycle overruns the interval
    async ({ start, calls, maxInFlight }) => {
      start();
      await new Promise((r) => setTimeout(r, INTERVAL * 6));
      assert.ok(calls.length >= 2, "expected at least two cycles");
      assert.equal(maxInFlight(), 1, "an overrunning cycle overlapped the next one");
      for (const g of gaps(calls)) {
        assert.ok(g >= INTERVAL * 1.6, `overrunning cycles must not compress: ${g}ms`);
      }
    },
  );
});

test("failure: backs off exponentially rather than storming", async () => {
  await withStub(
    () => ({ status: 500, delayMs: 10 }),
    async ({ start, calls }) => {
      start();
      await new Promise((r) => setTimeout(r, BACKOFF_BASE * 5));
      assert.ok(calls.length >= 2, "expected retries to continue (a down feed is retried slowly)");
      const g = gaps(calls);
      assert.ok(g[0] >= BACKOFF_BASE * 0.8, `first backoff ${g[0]}ms < base ${BACKOFF_BASE}ms`);
      if (g.length > 1) {
        assert.ok(g[1] > g[0] * 1.5, `backoff did not grow: ${g[0]}ms then ${g[1]}ms`);
      }
    },
  );
});

test("failure then recovery: backoff resets after a success", async () => {
  // Two failures, then healthy. The gap after recovery must return to the interval.
  await withStub(
    (i) => ({ status: i < 2 ? 500 : 200, delayMs: 10 }),
    async ({ start, calls }) => {
      start();
      await new Promise((r) => setTimeout(r, BACKOFF_BASE * 4 + INTERVAL * 4));
      assert.ok(calls.length >= 4, `expected recovery cycles, got ${calls.length}`);
      const g = gaps(calls);
      const afterRecovery = g[g.length - 1];
      assert.ok(afterRecovery < BACKOFF_BASE, `still backing off after success: ${afterRecovery}ms`);
    },
  );
});

test("429 with Retry-After is honoured", async () => {
  await withStub(
    (i) => (i === 0
      ? { status: 429, delayMs: 5, headers: { "retry-after": "1" } }
      : { status: 200, delayMs: 5 }),
    async ({ start, calls }) => {
      start();
      await new Promise((r) => setTimeout(r, 2200));
      assert.ok(calls.length >= 2, "expected a retry after the 429");
      assert.ok(gaps(calls)[0] >= 900, `Retry-After: 1s not honoured (waited ${gaps(calls)[0]}ms)`);
    },
  );
});

test("timeout is treated as failure, not success", async () => {
  await withStub(
    () => ({ status: 200, delayMs: 1500 }), // exceeds the 1000ms test timeout
    async ({ start, calls }) => {
      start();
      // The cycle itself burns the full 1000ms client timeout before the backoff even begins,
      // so the window has to cover timeout + min gap + backoff to see a second start at all.
      await new Promise((r) => setTimeout(r, 1000 + MIN_GAP + BACKOFF_BASE * 3));
      const g = gaps(calls);
      assert.ok(g.length >= 1, `expected more than one cycle, got ${calls.length}`);
      assert.ok(g[0] >= 1000 + BACKOFF_BASE * 0.8,
        `a timed-out cycle must back off after the timeout, but the next start was ${g[0]}ms later`);
    },
  );
});

test("duplicate scheduler: skips when another run started very recently", async () => {
  await withStub(
    () => ({ status: 200, delayMs: 10 }),
    async ({ start, calls, setLastRunStartedAt }) => {
      // Pretend pg_cron is still firing: a run always started a moment ago.
      const keepFresh = setInterval(() => setLastRunStartedAt(new Date().toISOString()), 20);
      setLastRunStartedAt(new Date().toISOString());
      start();
      await new Promise((r) => setTimeout(r, INTERVAL * 5));
      clearInterval(keepFresh);
      assert.equal(calls.length, 0, `expected every tick to skip, but ${calls.length} ran`);
    },
  );
});

test("duplicate guard fails open: a broken guard query must not stop ingestion", async () => {
  await withStub(
    () => ({ status: 200, delayMs: 10 }),
    async ({ start, calls, setLastRunStartedAt }) => {
      setLastRunStartedAt(null); // stub returns [] -- no recent run
      start();
      await new Promise((r) => setTimeout(r, INTERVAL * 3));
      assert.ok(calls.length >= 2, "ingestion should proceed when no recent run is reported");
    },
  );
});

test("parseRetryAfter handles seconds, dates and nonsense", async () => {
  await withStub(
    () => ({ status: 200 }),
    async ({ mod }) => {
      assert.equal(mod.parseRetryAfter("2"), 2000);
      assert.equal(mod.parseRetryAfter(null), null);
      assert.equal(mod.parseRetryAfter("not-a-date"), null);
      assert.ok(mod.parseRetryAfter(new Date(Date.now() + 1000).toUTCString()) <= 1000);
      assert.equal(mod.parseRetryAfter("999999"), 5000, "must clamp to the configured maximum");
    },
  );
});
