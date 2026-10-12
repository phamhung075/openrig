// S2 CROSSING (task 6fbd1f95, row qitem-20261012201503-b4c5817b84c94106) — the automated ROUND
// TRIP against the REAL cloud: go-dev's local Go server on the test-Postgres harness. Not a fake.
//
// WHAT THIS DRIVES. The same composition the daemon makes at boot — the gateway subsystem around
// `buildSlackGatewayWire`, which is where `buildDecisionsHalf` is composed (startup.ts) — so the
// dispatcher, the durable buffer, the ask sweep, the pull driver and the apply step are the
// shipped ones. The crossing replaces exactly two things: the network (a real fetch against
// CROSSING_BASE_URL, where the unit suites inject a fake) and the cloud assertions (go-dev's own
// declared read-back commands, so no SQL is invented here).
//
// THE FIVE BEHAVIOURS (brief: BRIEF-fe-dev-crossing-roundtrip-2026-10-12.md)
//   1 park                 — the daemon is not running; an answer exists on the cloud and nothing
//                            applies it, and the ask's row does not move.
//   2 apply exactly once   — on return the answer applies once and a second drain does nothing.
//   3 done / no-follow-on  — the row closes that way with the owner nudged ONCE (not per drain).
//   4 the redriven ask     — a re-issued ask collapses to ONE cloud row (the identity named).
//   5 the replayed answer  — re-delivering it acks and never re-resolves.
//
// ONE COMMAND. The facts come from go-dev's CROSSING-ENV-2026-10-12.md; export them, then:
//   npx vitest run test/decisions-crossing-roundtrip.test.ts
// The variables, all required:
//   CROSSING_BASE_URL   the server's base URL (becomes AGENTHUB_URL)
//   CROSSING_TOKEN      the bearer (becomes AGENTHUB_TOKEN)
//   CROSSING_RIG        the rig name, which is the cloud room's slug for the convention
//   CROSSING_SEAT       the asker's session name, rig-suffixed (<seat>@<rig>)
//   CROSSING_HUMAN      the human's address the answer is recorded against
//   CROSSING_RESET_CMD  the clean-scratch-DB step, run once before the behaviours
//   CROSSING_ANSWER_CMD the command that POSTs an answer AS the human (re-runnable)
//   CROSSING_ASKROW_CMD the read-back that prints the cloud's decision row(s) for CROSSING_RIG
//   CROSSING_ACKROW_CMD the read-back that prints the delivery/ack state of that row
//
// SKIPS LOUDLY, like the Go half's DB-gated cases: with any fact unset this file names what is
// missing and does NOT pass. No fact is ever substituted.
import { describe, expect, it, vi } from "vitest";
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createDb } from "../src/db/connection.js";
import { migrate } from "../src/db/migrate.js";
import { ALL_MIGRATIONS } from "../src/db/all-migrations.js";
import { EventBus } from "../src/domain/event-bus.js";
import { QueueRepository } from "../src/domain/queue-repository.js";
import { makeQueuePorts } from "../src/domain/gateway/slack/queue-access.js";
import { buildSlackGatewayWire, makeHumanReplyResolver } from "../src/domain/gateway/slack/slack-subsystem.js";
import { resolveSlackHandle } from "../src/domain/gateway/human-registry.js";
import { MissionControlActionLog } from "../src/domain/mission-control/mission-control-action-log.js";
import { MissionControlWriteContract } from "../src/domain/mission-control/mission-control-write-contract.js";
import { DispatchBuffer } from "../src/domain/gateway/dispatch-buffer.js";
import { ASK_OP, decisionIdFor } from "../src/domain/gateway/decisions/contract.js";
import { toAskBody } from "../src/domain/gateway/decisions/ask-driver.js";
import type { HumanQuestion } from "../src/domain/human-questions.js";

const FACTS = [
  "CROSSING_BASE_URL",
  "CROSSING_TOKEN",
  "CROSSING_RIG",
  "CROSSING_SEAT",
  "CROSSING_HUMAN",
  "CROSSING_RESET_CMD",
  "CROSSING_ANSWER_CMD",
  "CROSSING_ASKROW_CMD",
  "CROSSING_ACKROW_CMD",
] as const;

const missing = FACTS.filter((f) => !process.env[f] || process.env[f]!.trim().length === 0);
if (missing.length > 0) {
  console.warn(
    `decisions-crossing SKIPPED, NOT PASSED: ${missing.length} fact(s) unset — ${missing.join(", ")}. ` +
    `They come from go-dev's CROSSING-ENV-2026-10-12.md; export them to run the crossing.`,
  );
}

/** One shell command whose stdout is the observable. The command comes from go-dev's file. */
function readback(cmd: string): string {
  return execFileSync("bash", ["-lc", cmd], { encoding: "utf8", timeout: 60_000 }).trim();
}

const rig = process.env.CROSSING_RIG ?? "";
const seat = process.env.CROSSING_SEAT ?? "";
const human = process.env.CROSSING_HUMAN ?? "";

/** The frozen §4 binding: a human whose connector kind is the new set member. */
const registry = {
  ok: true as const,
  entities: [{
    entityId: "crossing-human",
    class: "human" as const,
    displayName: "Crossing Human",
    address: human,
    connectorBindings: [
      { kind: "agenthub", connectorRef: "agenthub-cloud", secretsRef: "AGENTHUB_TOKEN", role: "primary" as const },
    ],
    prefs: { deliveryClass: "A" as const },
  }],
};

const questions: HumanQuestion[] = [
  { id: "db", question: "Which database?", options: [{ id: "pg", label: "Postgres", recommended: true }, { id: "sqlite", label: "SQLite" }] },
];

/** Everything one behaviour needs, built fresh per case: a scratch home, a scratch queue DB, and
 *  the production wire over them. `startServices()` is the daemon coming up; leaving it unstarted
 *  or calling `stop()` is the daemon being down. */
function harness() {
  const home = mkdtempSync(join(tmpdir(), "crossing-"));
  const db = createDb();
  migrate(db, ALL_MIGRATIONS);
  const bus = new EventBus(db);
  const repo = new QueueRepository(db, bus, { loadHumanRegistry: () => registry });
  // The inventory the pull scopes on and the seat key resolves through: without the rig row the
  // pull has nothing to loop, and without the node row `resolveSeatKey` cannot name the asker.
  db.prepare("INSERT INTO rigs (id, name) VALUES (?, ?)").run("rig-crossing", rig);
  db.prepare("INSERT INTO nodes (id, rig_id, logical_id, runtime, cwd) VALUES (?, ?, ?, ?, ?)")
    .run("node-crossing", "rig-crossing", seat.split("@")[0]?.replace(/-/g, ".") ?? "seat.crossing", "omp", "/tmp");
  const envFile = join(home, "agenthub.env");
  writeFileSync(envFile, `AGENTHUB_URL=${process.env.CROSSING_BASE_URL}\nAGENTHUB_TOKEN=${process.env.CROSSING_TOKEN}\n`, { mode: 0o600 });

  const wire = buildSlackGatewayWire({
    home,
    queueRepo: repo,
    registry: { loadHumanRegistry: () => registry, resolveSlackHandle },
    resolveHumanReply: makeHumanReplyResolver(repo, new MissionControlWriteContract({ db, eventBus: bus, queueRepo: repo, actionLog: new MissionControlActionLog(db) })),
    agenthubEnvFile: envFile,
    rigNames: () => [rig],
    // Fast ticks: the crossing waits on the drivers' own cadence, which the daemon sets by config.
    agenthubAskIntervalMs: 250,
    agenthubPullIntervalMs: 250,
    log: () => {},
  });

  const ports = makeQueuePorts(repo, { loadHumanRegistry: () => registry });
  const cleanup = () => { try { wire.stop(); } catch { /* already stopped */ } try { db.close(); } catch { /* closed */ } rmSync(home, { recursive: true, force: true }); };
  return { home, db, repo, wire, ports, cleanup };
}

/** The pending decision the ask sweeps on: a human question row, as the mission control writes it. */
async function pendingDecision(repo: QueueRepository) {
  const created = await repo.create({
    sourceSession: seat,
    destinationSession: human,
    summary: "Crossing: one quick question",
    body: "Pick one.",
    evidenceRef: null,
    nudge: false,
    humanIntent: "decision",
    humanQuestions: questions,
  });
  return created.qitemId;
}

const ownerNudges = (repo: QueueRepository, qitemId: string) =>
  repo.transitionLog.listForQitem(qitemId).filter((t) => (t as { ownerNotificationKind?: string | null }).ownerNotificationKind != null);
const doneTransitions = (repo: QueueRepository, qitemId: string) =>
  repo.transitionLog.listForQitem(qitemId).filter((t) => (t as { state?: string }).state === "done");

/** The pull driver's own counters, read off the wire's status surface. `ticks` proves a drain
 *  RAN and `applied`/`dropped` prove what it did, so the crossing shows "a later drain changed
 *  nothing" by a counter, never by a guessed duration. */
function pullCounters(wire: { status?: () => Record<string, unknown> }): { ticks: number; applied: number; dropped: number } {
  const agenthub = (wire.status?.() ?? {})["agenthub"] as { pull?: { ticks?: number; applied?: number; dropped?: number } } | undefined;
  const pull = agenthub?.pull ?? {};
  return { ticks: pull.ticks ?? 0, applied: pull.applied ?? 0, dropped: pull.dropped ?? 0 };
}

describe.skipIf(missing.length > 0)("decisions crossing — the round trip against the real server", () => {
  it("1+2+3 park, then apply exactly once, and the row ends done/no-follow-on with ONE owner nudge", async () => {
    readback(process.env.CROSSING_RESET_CMD!);
    const h = harness();
    try {
      const qitemId = await pendingDecision(h.repo);
      const decisionId = decisionIdFor(qitemId);

      // Up: the sweep posts the ask through the shipped dispatcher, over the real wire.
      h.wire.startServices?.();
      await vi.waitFor(() => expect(readback(process.env.CROSSING_ASKROW_CMD!)).toContain(decisionId), { timeout: 30_000 });
      expect(new DispatchBuffer(h.home).pending()).toHaveLength(0); // accepted, so the buffer drained
      h.wire.stop();

      // PARK: the daemon is down. The human answers on the cloud; nothing here may move.
      readback(process.env.CROSSING_ANSWER_CMD!);
      const parked = readback(process.env.CROSSING_ASKROW_CMD!);
      expect(parked).toContain(decisionId);            // the ask row is still there
      expect(parked).not.toMatch(/resolved/i);         // and the cloud state has not moved
      expect(doneTransitions(h.repo, qitemId)).toHaveLength(0); // nothing applied while down

      // RETURN: a fresh wire over the same queue DB is the daemon coming back up.
      const back = harness();
      try {
        await vi.waitFor(() => expect(doneTransitions(back.repo, qitemId)).toHaveLength(1), { timeout: 30_000 });
        // Exactly once, and deterministically: wait for a LATER drain to have actually run (the
        // driver's tick counter), never for a guessed duration, then assert the apply count held.
        // The ack, not the cursor, is the delivery record — a drained pull must not re-apply.
        await vi.waitFor(() => expect(pullCounters(back.wire).applied).toBe(1), { timeout: 30_000 });
        const ticksAtApply = pullCounters(back.wire).ticks;
        await vi.waitFor(() => expect(pullCounters(back.wire).ticks).toBeGreaterThan(ticksAtApply), { timeout: 30_000 });
        expect(pullCounters(back.wire).applied).toBe(1);
        expect(doneTransitions(back.repo, qitemId)).toHaveLength(1);
        expect(ownerNudges(back.repo, qitemId)).toHaveLength(1);
        expect(readback(process.env.CROSSING_ACKROW_CMD!)).toMatch(/applied/i);
      } finally { back.cleanup(); }

      // 5 — the replay, with the daemon live again: the same answer acks and never re-resolves.
      readback(process.env.CROSSING_ANSWER_CMD!);
      const replay = harness();
      try {
        replay.wire.startServices?.();
        await vi.waitFor(() => expect(pullCounters(replay.wire).ticks).toBeGreaterThan(0), { timeout: 30_000 });
        expect(pullCounters(replay.wire).applied).toBe(0); // the replay applied nothing
        expect(doneTransitions(replay.repo, qitemId)).toHaveLength(1);
        expect(ownerNudges(replay.repo, qitemId)).toHaveLength(1);
      } finally { replay.cleanup(); }
    } finally { h.cleanup(); }
  }, 180_000);

  it("4 a re-issued ask collapses to ONE cloud row, and the identity that collapses it is named", async () => {
    readback(process.env.CROSSING_RESET_CMD!);
    const h = harness();
    try {
      const qitemId = await pendingDecision(h.repo);
      const decisionId = decisionIdFor(qitemId);
      h.wire.startServices?.();
      const port = h.ports;
      const alerts = await port.listHumanAlerts({ minimumLevel: "NOTICE" });
      const alert = alerts.find((q) => q.qitemId === qitemId);
      if (!alert) throw new Error("fixture: the decision did not project as a human alert");
      const built = toAskBody(alert, { resolveSeatKey: () => `${rig}.${seat.split("@")[0]}` });
      if (!built.ok) throw new Error(built.error);

      await vi.waitFor(() => expect(readback(process.env.CROSSING_ASKROW_CMD!)).toContain(decisionId), { timeout: 30_000 });
      // The redrive: the SAME episode key, dispatched again through the same dispatcher. The
      // cloud's UNIQUE (user_id, idempotency_key) is what collapses it, and the key is
      // `agenthub:decision:<qitemId>` — episode-stable, so the second ingest returns the first row.
      const again = h.wire.dispatcher.dispatch(ASK_OP, human, built.body, { decisionId: built.body.idempotency_key });
      expect(again.ok).toBe(true);
      await vi.waitFor(() => expect(new DispatchBuffer(h.home).pending()).toHaveLength(0), { timeout: 30_000 });
      const rows = readback(process.env.CROSSING_ASKROW_CMD!);
      expect(rows.split(decisionId).length - 1).toBe(1);
    } finally { h.cleanup(); }
  }, 180_000);
});
