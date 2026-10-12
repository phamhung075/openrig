import { describe, it, expect } from "vitest";
import {
  decisionIdFor,
  dropReasonFor,
  type AckBody,
  type PulledAnswer,
} from "../src/domain/gateway/decisions/contract.js";
import {
  ackAlreadySettled,
  parseAnswersPage,
  type CloudCall,
  type CloudClient,
} from "../src/domain/gateway/decisions/cloud-client.js";
import {
  DecisionsPullDriver,
  DEFAULT_PULL_INTERVAL_MS,
  MAX_PULL_INTERVAL_MS,
} from "../src/domain/gateway/decisions/pull-driver.js";
import type { ApplyResult } from "../src/domain/gateway/decisions/answer-apply.js";

// S2 — the cloud decisions wire, PULL half. Real behaviour only: the page parser's accept/refuse
// boundary, the map onto the cloud's closed drop vocabulary, and the driver's cursor/ack/backoff
// transitions under a scripted fake client.

const OK_EMPTY: CloudCall = { ok: true, status: 200, body: { success: true, answers: [], cursor: "" } };

function pullRow(over: Partial<PulledAnswer> = {}): PulledAnswer {
  return {
    decision_id: "d-1",
    qitem_id: "qi-1",
    author_seat_key: "seat-a",
    room: "room-1",
    answers: { q1: "opt-1" },
    answered_at: "2026-10-12T00:00:00.000Z",
    ...over,
  };
}

function page(answers: PulledAnswer[], cursor: string): CloudCall {
  return { ok: true, status: 200, body: { success: true, answers, cursor } };
}

interface FakeClient {
  client: CloudClient;
  pulls: { rig: string; after: string | null }[];
  acks: { decisionId: string; body: AckBody }[];
}

function makeFakeClient(script: {
  pull: (after: string | null, callIndex: number) => CloudCall;
  ack?: (decisionId: string, body: AckBody, callIndex: number) => CloudCall;
}): FakeClient {
  const pulls: { rig: string; after: string | null }[] = [];
  const acks: { decisionId: string; body: AckBody }[] = [];
  const client: CloudClient = {
    postAsk: async () => ({ ok: true, status: 200, body: {} }),
    pullAnswers: async (rig, after) => {
      const idx = pulls.length;
      pulls.push({ rig, after });
      return script.pull(after, idx);
    },
    ack: async (decisionId, body) => {
      const idx = acks.length;
      acks.push({ decisionId, body });
      return script.ack?.(decisionId, body, idx) ?? { ok: true, status: 200, body: { success: true } };
    },
    pushState: async () => ({ ok: true, status: 200, body: {} }),
  };
  return { client, pulls, acks };
}

function makeDriver(
  client: CloudClient,
  apply: (item: PulledAnswer) => Promise<ApplyResult>,
  opts: { hostId?: string; intervalMs?: number; maxIntervalMs?: number; log?: (m: string) => void } = {},
): DecisionsPullDriver {
  return new DecisionsPullDriver({
    client,
    rigs: () => ["rig-1"],
    apply,
    hostId: () => opts.hostId ?? "host-42",
    intervalMs: opts.intervalMs ?? 1000,
    maxIntervalMs: opts.maxIntervalMs ?? 1000,
    now: () => new Date("2026-10-12T00:00:00.000Z"),
    ...(opts.log ? { log: opts.log } : {}),
  });
}

const alwaysApplied = async (): Promise<ApplyResult> => ({ settled: true, outcome: "applied", complete: true });

function deferred<T>(): { promise: Promise<T>; resolve: (v: T) => void } {
  let resolve!: (v: T) => void;
  const promise = new Promise<T>((r) => { resolve = r; });
  return { promise, resolve };
}

describe("decisions contract — identity and drop vocabulary", () => {
  it("decisionIdFor is episode-stable and names the connector kind", () => {
    const id = decisionIdFor("qitem-x");
    expect(id).toBe("agenthub:decision:qitem-x");
    // The same qitem id maps to the same key twice (one cloud row, never a duplicate).
    expect(decisionIdFor("qitem-x")).toBe(id);
    expect(decisionIdFor("qitem-y")).not.toBe(id);
  });

  it("dropReasonFor maps every local refusal onto the closed cloud vocabulary", () => {
    expect(dropReasonFor("state-done")).toBe("state-done");
    expect(dropReasonFor("state-weird")).toBe("state-done"); // any state-* prefix
    expect(dropReasonFor("not-the-asked-human")).toBe("not-the-asked-human");
    expect(dropReasonFor("resolve-not-applicable")).toBe("already-applied");
    expect(dropReasonFor("already-applied")).toBe("already-applied");
    expect(dropReasonFor("something-unmapped")).toBe("unknown-qitem");
  });

  it("pins the shipped cadence and its ceiling", () => {
    expect(DEFAULT_PULL_INTERVAL_MS).toBe(30000);
    expect(MAX_PULL_INTERVAL_MS).toBe(300000);
  });
});

describe("parseAnswersPage — frozen page shape", () => {
  it("parses a well-formed page into exact rows (answers as a questionId→optionId map) and cursor", () => {
    const r = parseAnswersPage({
      success: true,
      answers: [
        { decision_id: "d-1", qitem_id: "qi-1", author_seat_key: "seat-a", room: "room-1", answers: { q1: "opt-1", q2: "opt-2" }, answered_at: "2026-10-12T01:00:00.000Z" },
        { decision_id: "d-2", qitem_id: "qi-2", author_seat_key: "seat-b", room: "room-2", answers: { q9: "opt-9" }, answered_at: "2026-10-12T02:00:00.000Z" },
      ],
      cursor: "cur-1",
    });
    expect(r.ok).toBe(true);
    if (!r.ok) return;
    expect(r.page.cursor).toBe("cur-1");
    expect(r.unreadable).toEqual([]);
    expect(r.page.answers).toEqual([
      { decision_id: "d-1", qitem_id: "qi-1", author_seat_key: "seat-a", room: "room-1", answers: { q1: "opt-1", q2: "opt-2" }, answered_at: "2026-10-12T01:00:00.000Z" },
      { decision_id: "d-2", qitem_id: "qi-2", author_seat_key: "seat-b", room: "room-2", answers: { q9: "opt-9" }, answered_at: "2026-10-12T02:00:00.000Z" },
    ]);
  });

  it("refuses a body without success:true", () => {
    const r = parseAnswersPage({ answers: [], cursor: "" });
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.error).toMatch(/success/i);
  });

  it("refuses answers that is not an array", () => {
    const r = parseAnswersPage({ success: true, answers: "nope", cursor: "" });
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.error).toMatch(/answers/i);
  });

  // §2 no-stall clause: a defect INSIDE a structurally valid page is a ROW problem, never a page
  // problem. The reader could not use the row, so it is reported as unreadable while the page's
  // own cursor stays valid — which is what makes skipping forward safe.
  it("reports a row missing a required string field as UNREADABLE and keeps the page", () => {
    const r = parseAnswersPage({ success: true, answers: [{ decision_id: "d-1", qitem_id: "qi-1", author_seat_key: "seat-a", answers: { q1: "opt-1" }, answered_at: "t" }], cursor: "cur-1" });
    expect(r.ok).toBe(true);
    if (!r.ok) return;
    expect(r.page.answers).toEqual([]);
    expect(r.page.cursor).toBe("cur-1");
    expect(r.unreadable).toHaveLength(1);
    expect(r.unreadable[0].index).toBe(0);
    expect(r.unreadable[0].decisionId).toBe("d-1");
    expect(r.unreadable[0].reason).toMatch(/room/);
  });

  it("reports a row whose answers is an empty object as UNREADABLE (an answered row must carry at least one pick)", () => {
    const r = parseAnswersPage({ success: true, answers: [{ ...pullRow(), answers: {} }], cursor: "" });
    expect(r.ok).toBe(true);
    if (!r.ok) return;
    expect(r.page.answers).toEqual([]);
    expect(r.unreadable[0]).toMatchObject({ index: 0, decisionId: "d-1" });
    expect(r.unreadable[0].reason).toMatch(/at least one/);
  });

  it("reports a non-string option id as UNREADABLE", () => {
    const r = parseAnswersPage({ success: true, answers: [{ ...pullRow(), answers: { q1: 5 } }], cursor: "" });
    expect(r.ok).toBe(true);
    if (!r.ok) return;
    expect(r.unreadable[0].reason).toMatch(/option id/);
  });

  it("keeps the good rows of a page one of whose rows is unreadable", () => {
    const r = parseAnswersPage({
      success: true,
      answers: [pullRow({ decision_id: "d-good" }), { ...pullRow({ decision_id: "d-bad" }), answers: {} }],
      cursor: "",
    });
    expect(r.ok).toBe(true);
    if (!r.ok) return;
    expect(r.page.answers.map((a) => a.decision_id)).toEqual(["d-good"]);
    expect(r.unreadable.map((u) => u.decisionId)).toEqual(["d-bad"]);
  });

  it("refuses a non-string cursor", () => {
    const r = parseAnswersPage({ success: true, answers: [], cursor: 7 });
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.error).toMatch(/cursor/);
  });
});

describe("ackAlreadySettled", () => {
  it("treats only a 409 as settled", () => {
    expect(ackAlreadySettled({ ok: false, status: 409, error: "HTTP 409: gone" })).toBe(true);
    expect(ackAlreadySettled({ ok: false, status: 500, error: "HTTP 500: boom" })).toBe(false);
    expect(ackAlreadySettled({ ok: true, status: 200, body: { success: true } })).toBe(false);
    expect(ackAlreadySettled({ ok: false, error: "network: reset" })).toBe(false);
  });
});

describe("DecisionsPullDriver — drain, ack and cursor", () => {
  it("drains a single page ending with an empty cursor and acks with hostId + outcome applied", async () => {
    const fake = makeFakeClient({
      pull: (_after, idx) => (idx === 0 ? page([pullRow()], "") : OK_EMPTY),
    });
    const driver = makeDriver(fake.client, alwaysApplied, { hostId: "host-42" });

    await driver.tick();

    expect(fake.pulls).toEqual([{ rig: "rig-1", after: null }]);
    expect(fake.acks).toEqual([
      { decisionId: "d-1", body: { machine_id: "host-42", outcome: "applied" } },
    ]);
    expect(driver.status().applied).toBe(1);
  });

  it("drains page after page until the cursor is empty; the next request's after is the first page's cursor", async () => {
    const fake = makeFakeClient({
      pull: (_after, idx) => {
        if (idx === 0) return page([pullRow({ decision_id: "d-1", qitem_id: "qi-1" })], "cur-1");
        if (idx === 1) return page([pullRow({ decision_id: "d-2", qitem_id: "qi-2" })], "");
        return OK_EMPTY;
      },
    });
    const driver = makeDriver(fake.client, alwaysApplied);

    await driver.tick();

    expect(fake.pulls.map((p) => p.after)).toEqual([null, "cur-1"]);
    expect(fake.acks.map((a) => a.decisionId)).toEqual(["d-1", "d-2"]);
    expect(driver.status().applied).toBe(2);
  });

  it("an unsettled apply produces NO ack and leaves the cursor unmoved (same page start re-pulled)", async () => {
    const fake = makeFakeClient({
      pull: (_after, idx) => (idx === 0 ? page([pullRow()], "cur-1") : OK_EMPTY),
    });
    const apply = async (): Promise<ApplyResult> => ({ settled: false, reason: "record-failed" });
    const driver = makeDriver(fake.client, apply);

    await driver.tick();
    expect(fake.acks).toEqual([]);

    await driver.tick();
    // The failing page's cursor was never committed: both ticks ask for the same page start.
    expect(fake.pulls.map((p) => p.after)).toEqual([null, null]);
  });

  it("an ack that fails with 500 leaves the cursor at the pre-page value", async () => {
    let ackCalls = 0;
    const fake = makeFakeClient({
      pull: (_after, idx) => {
        if (idx === 0) return page([pullRow({ decision_id: "d-1", qitem_id: "qi-1" })], "cur-1");
        if (idx === 1) return page([pullRow({ decision_id: "d-2", qitem_id: "qi-2" })], "cur-2");
        return OK_EMPTY;
      },
      ack: () => (ackCalls++ === 0 ? { ok: true, status: 200, body: { success: true } } : { ok: false, status: 500, error: "HTTP 500: boom" }),
    });
    const driver = makeDriver(fake.client, alwaysApplied, { intervalMs: 1000, maxIntervalMs: 2000 });

    await driver.tick(); // page1 acked (cursor -> cur-1), page2 ack fails: cur-2 must not be committed
    expect(fake.acks.map((a) => a.decisionId)).toEqual(["d-1", "d-2"]);
    // §2 class 3: a 5xx IS transport-class, so it keeps the cursor and doubles the cadence.
    expect(driver.status().intervalMs).toBe(2000);

    await driver.tick();
    // The re-request starts from cur-1, not the failed page's cur-2.
    expect(fake.pulls.map((p) => p.after)).toEqual([null, "cur-1", "cur-1"]);
    expect(driver.status().applied).toBe(1);
  });

  it("a permanent 400 on the FIRST row's ack does not stop the drain: the later row is still acked, and the refusal is logged once (§2 class 4)", async () => {
    const fake = makeFakeClient({
      // Tick 1 serves both rows on one page. Tick 2 serves only d-1 again: it was never acked, so
      // the cloud still hands it out — which is also why no cursor was held between the ticks.
      pull: (_after, idx) =>
        idx === 0
          ? page([pullRow({ decision_id: "d-1", qitem_id: "qi-1" }), pullRow({ decision_id: "d-2", qitem_id: "qi-2" })], "")
          : idx === 1
            ? page([pullRow({ decision_id: "d-1", qitem_id: "qi-1" })], "")
            : OK_EMPTY,
      // A malformed body, a stale vocabulary token, a bad bearer, an unmounted route: no cadence
      // fixes a refused request, so one refused row must not freeze the rig behind it.
      ack: (decisionId) =>
        decisionId === "d-1"
          ? { ok: false, status: 400, error: "HTTP 400: reason is not in the closed list" }
          : { ok: true, status: 200, body: { success: true } },
    });
    const logs: string[] = [];
    const driver = makeDriver(fake.client, alwaysApplied, { log: (m) => logs.push(m) });

    await driver.tick();

    // The drain did not stop at the refused row: d-2 was applied AND acked in the same tick, so the
    // refusal cost one row, not the rig.
    expect(fake.acks.map((a) => a.decisionId)).toEqual(["d-1", "d-2"]);
    expect(driver.status().applied).toBe(2);
    // The refused row is NOT acked: the one attempt that went out is the real one (the apply's own
    // outcome), and no second or mutated ack was sent to force the cloud to accept it.
    expect(fake.acks[0]).toEqual({ decisionId: "d-1", body: { machine_id: "host-42", outcome: "applied" } });
    // A refused ack is a row-level event, not the drain's error state: the cloud still owns the row.
    expect(driver.status().lastError).toBeNull();
    // Class 4 is NOT transport-class: no backoff.
    expect(driver.status().intervalMs).toBe(1000);

    await driver.tick();
    // Re-served on the NEXT drain, not suppressed: the page end was reached and the cursor deleted,
    // so this tick asked from the first page again and the un-acked row came back.
    expect(fake.pulls.map((p) => p.after)).toEqual([null, null]);
    expect(fake.acks.map((a) => a.decisionId)).toEqual(["d-1", "d-2", "d-1"]);
    expect(driver.status().applied).toBe(3);
    // Once per episode: the cloud re-serves the un-acked row every tick, so a per-attempt line
    // would be a drumbeat for one anomaly.
    expect(logs.filter((m) => m.includes("d-1") && m.includes("refused"))).toHaveLength(1);
  });

  it("an ack that returns 409 counts as settled and advances the cursor", async () => {
    const fake = makeFakeClient({
      pull: (_after, idx) => (idx === 0 ? page([pullRow()], "cur-1") : OK_EMPTY),
      ack: () => ({ ok: false, status: 409, error: "HTTP 409: already acked" }),
    });
    const driver = makeDriver(fake.client, alwaysApplied);

    await driver.tick();

    expect(fake.acks.map((a) => a.decisionId)).toEqual(["d-1"]);
    // Advanced past the row and drained: two requests, the second using the page cursor.
    expect(fake.pulls.map((p) => p.after)).toEqual([null, "cur-1"]);
    expect(driver.status().applied).toBe(1);
    expect(driver.status().lastError).toBeNull();
  });

  it("a page whose ENVELOPE is broken stops the drain without advancing (only an envelope has no position to advance to)", async () => {
    const fake = makeFakeClient({ pull: () => ({ ok: true, status: 200, body: { success: true, answers: "nope", cursor: "" } }) });
    const driver = makeDriver(fake.client, alwaysApplied);

    await driver.tick();

    expect(fake.pulls.map((p) => p.after)).toEqual([null]);
    expect(fake.acks).toEqual([]);
    expect(driver.status().lastError).toMatch(/answers/i);

    await driver.tick();
    // Nothing was skipped past: the next tick asks for the same first page.
    expect(fake.pulls.map((p) => p.after)).toEqual([null, null]);
  });

  it("a page holding one unreadable row BETWEEN two good rows applies and acks both, logs the bad row once, and never acks it", async () => {
    const bad = {
      decision_id: "d-bad",
      qitem_id: "qi-bad",
      author_seat_key: "seat-x",
      room: "room-1",
      answers: {},
      answered_at: "2026-10-12T03:00:00.000Z",
    };
    const scripted: CloudCall = {
      ok: true,
      status: 200,
      body: {
        success: true,
        answers: [pullRow({ decision_id: "d-1", qitem_id: "qi-1" }), bad, pullRow({ decision_id: "d-2", qitem_id: "qi-2" })],
        cursor: "",
      },
    };
    // The same page every call: the un-acked row comes back next tick, exactly as the cloud serves it.
    const fake = makeFakeClient({ pull: () => scripted });
    const logs: string[] = [];
    const driver = makeDriver(fake.client, alwaysApplied, { log: (m) => logs.push(m) });

    await driver.tick();
    await driver.tick();

    // Both good rows are applied and acked on both ticks: the unreadable row blocks nothing.
    expect(fake.acks.map((a) => a.decisionId)).toEqual(["d-1", "d-2", "d-1", "d-2"]);
    // It is never acked — the cloud's row is the cloud's to judge — and it is logged ONCE, not per tick.
    expect(fake.acks.some((a) => a.decisionId === "d-bad")).toBe(false);
    expect(logs.filter((m) => m.includes("d-bad"))).toHaveLength(1);
    expect(logs.find((m) => m.includes("d-bad"))).toMatch(/not acked/i);
    // The drain ran to the empty cursor, and a row-level defect is not a driver error.
    expect(fake.pulls.map((p) => p.after)).toEqual([null, null]);
    expect(driver.status().applied).toBe(4);
    expect(driver.status().lastError).toBeNull();
  });

  it("forgets a reported anomaly once a completing drain no longer serves it, and reports it once more if it comes back", async () => {
    const bad = { decision_id: "d-bad", qitem_id: "qi-bad", author_seat_key: "seat-x", room: "room-1", answers: {}, answered_at: "2026-10-12T03:00:00.000Z" };
    const withBad: CloudCall = { ok: true, status: 200, body: { success: true, answers: [pullRow(), bad], cursor: "" } };
    // Tick 1 the anomaly is served. Tick 2 the cloud holds nothing un-acked for this rig, so the
    // drain COMPLETES — which is what tells the driver the logged row is gone. Tick 3 it is back.
    const fake = makeFakeClient({ pull: (_after, idx) => (idx === 1 || idx === 3 ? OK_EMPTY : withBad) });
    const logs: string[] = [];
    const driver = makeDriver(fake.client, alwaysApplied, { log: (m) => logs.push(m) });

    await driver.tick();
    expect(logs.filter((m) => m.includes("d-bad"))).toHaveLength(1);

    await driver.tick(); // an empty page: nothing this rig logged is live any more
    expect(logs.filter((m) => m.includes("d-bad"))).toHaveLength(1);

    await driver.tick(); // served again: a NEW episode, so it earns its own single line
    expect(logs.filter((m) => m.includes("d-bad"))).toHaveLength(2);
  });

  it("keys a nameless row by its own identity: a row that only MOVED is not re-reported, and a different one is not swallowed", async () => {
    // Same reason, no decision_id: distinguishable only by their own fields.
    const nameless = (qitem: string): unknown => ({ qitem_id: qitem, author_seat_key: "seat-a", room: "room-1", answers: {}, answered_at: "2026-10-12T03:00:00.000Z" });
    const good = pullRow({ decision_id: "d-good" });
    const serve = (rows: unknown[]): CloudCall => ({ ok: true, status: 200, body: { success: true, answers: rows, cursor: "" } });
    const fake = makeFakeClient({
      pull: (_after, idx) =>
        idx === 0 ? serve([good, nameless("qi-a")]) : idx === 1 ? serve([nameless("qi-a"), good]) : serve([good, nameless("qi-b")]),
    });
    const logs: string[] = [];
    const driver = makeDriver(fake.client, alwaysApplied, { log: (m) => logs.push(m) });

    await driver.tick();
    expect(logs.filter((m) => m.includes("unreadable"))).toHaveLength(1);

    await driver.tick(); // qi-a moved from index 1 to index 0: the same anomaly, not a new one
    expect(logs.filter((m) => m.includes("unreadable"))).toHaveLength(1);

    await driver.tick(); // a DIFFERENT nameless row at the position qi-a first occupied
    expect(logs.filter((m) => m.includes("unreadable"))).toHaveLength(2);
  });

  it("an HTTP 400 on the cursor clears it so the next tick sends an empty after", async () => {
    const fake = makeFakeClient({
      pull: (_after, idx) => {
        if (idx === 0) return page([], "cur-1"); // commit cur-1, then the next call is refused
        return { ok: false, status: 400, error: "HTTP 400: unknown cursor" };
      },
    });
    const driver = makeDriver(fake.client, alwaysApplied);

    await driver.tick();
    await driver.tick();

    // null, then cur-1 (committed, then rejected), then null again after the cursor was dropped.
    expect(fake.pulls.map((p) => p.after)).toEqual([null, "cur-1", null]);
  });

  it("a 5xx pull doubles the interval up to the ceiling; the next success resets it to base", async () => {
    const fake = makeFakeClient({
      pull: (_after, idx) => (idx < 2 ? { ok: false, status: 503, error: "HTTP 503: down" } : OK_EMPTY),
    });
    const driver = makeDriver(fake.client, alwaysApplied, { intervalMs: 1000, maxIntervalMs: 2000 });

    expect(driver.status().intervalMs).toBe(1000);
    await driver.tick();
    expect(driver.status().intervalMs).toBe(2000); // doubled
    await driver.tick();
    expect(driver.status().intervalMs).toBe(2000); // clamped at the ceiling, not 4000
    await driver.tick();
    expect(driver.status().intervalMs).toBe(1000); // first success resets to base
  });

  it("a second tick while one is in flight is a no-op (the client sees only the first tick's calls)", async () => {
    const gate = deferred<ApplyResult>();
    const fake = makeFakeClient({
      pull: (_after, idx) => (idx === 0 ? page([pullRow()], "cur-1") : OK_EMPTY),
    });
    let applyCalls = 0;
    const driver = makeDriver(fake.client, () => { applyCalls++; return gate.promise; });

    const first = driver.tick();
    const second = driver.tick(); // sees ticking === true and returns immediately
    gate.resolve({ settled: true, outcome: "applied", complete: true });
    await Promise.all([first, second]);

    expect(applyCalls).toBe(1);
    expect(fake.pulls.map((p) => p.after)).toEqual([null, "cur-1"]);
    expect(driver.status().ticks).toBe(1);
  });

  it("counts dropped outcomes separately from applied ones", async () => {
    const fake = makeFakeClient({
      pull: () =>
        page(
          [
            pullRow({ decision_id: "d-a", qitem_id: "qi-a" }),
            pullRow({ decision_id: "d-b", qitem_id: "qi-b" }),
          ],
          "",
        ),
    });
    const apply = async (item: PulledAnswer): Promise<ApplyResult> =>
      item.decision_id === "d-a"
        ? { settled: true, outcome: "applied", complete: true }
        : { settled: true, outcome: "dropped", reason: "state-done", complete: false };
    const driver = makeDriver(fake.client, apply);

    await driver.tick();

    expect(driver.status().applied).toBe(1);
    expect(driver.status().dropped).toBe(1);
    expect(fake.acks).toEqual([
      { decisionId: "d-a", body: { machine_id: "host-42", outcome: "applied" } },
      { decisionId: "d-b", body: { machine_id: "host-42", outcome: "dropped", reason: "state-done" } },
    ]);
  });
});
