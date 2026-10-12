import { describe, it, expect, afterEach } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  applyPulledAnswer,
  replyRowIdFor,
  type AnswerApplyDeps,
  type ReplyRowPort,
} from "../src/domain/gateway/decisions/answer-apply.js";
import { DecisionsAskDriver } from "../src/domain/gateway/decisions/ask-driver.js";
import { ASK_OP, decisionIdFor, type PulledAnswer } from "../src/domain/gateway/decisions/contract.js";
import type { HumanQuestion, RecordHumanAnswerResult } from "../src/domain/human-questions.js";
import type { QueueItem } from "../src/domain/gateway/slack/queue-access.js";
import type { HumanFragment, LoadResult } from "../src/domain/gateway/human-registry.js";

// ─────────────────────────────────────────────────────────────────────────────
// Answer apply
// ─────────────────────────────────────────────────────────────────────────────

const Q1: HumanQuestion = {
  id: "q1",
  question: "Ship it?",
  options: [
    { id: "yes", label: "Yes" },
    { id: "no", label: "No" },
  ],
};
const Q2: HumanQuestion = {
  id: "q2",
  question: "Roll back?",
  options: [
    { id: "yes", label: "Yes" },
    { id: "no", label: "No" },
  ],
};

function pulled(over: Partial<PulledAnswer> = {}): PulledAnswer {
  return {
    decision_id: decisionIdFor("qitem-1"),
    qitem_id: "qitem-1",
    author_seat_key: "pod.seat-a",
    room: "rig-x",
    answers: { q1: "yes" },
    answered_at: "2026-10-12T00:00:00.000Z",
    ...over,
  };
}

type RecordedCall = { qitemId: string; actorSession: string; questionId: string; optionId: string };
type CreatedCall = Parameters<ReplyRowPort["createQitem"]>[0];
type ResolvedCall = { qitemId: string; actorSession: string; decision: string };

interface ApplyHarness {
  deps: AnswerApplyDeps;
  recorded: RecordedCall[];
  created: CreatedCall[];
  resolved: ResolvedCall[];
}

const defaultDescribe = (): { askerSession: string; humanSession: string | null } => ({
  askerSession: "seat-a@rig-x",
  humanSession: "founder@rig-x",
});
const defaultRecord = (): RecordHumanAnswerResult => ({
  status: "recorded",
  answers: { q1: "yes" },
  complete: true,
  questions: [Q1],
});

/** Wraps the injected deps so every call is observed without changing behaviour. */
function applyHarness(over: Partial<AnswerApplyDeps> = {}): ApplyHarness {
  const recorded: RecordedCall[] = [];
  const created: CreatedCall[] = [];
  const resolved: ResolvedCall[] = [];
  const raw: AnswerApplyDeps = {
    describeDecision: defaultDescribe,
    recordHumanAnswer: defaultRecord,
    replyRows: { createQitem: async (input) => input.qitemId ?? "?" },
    resolveHumanReply: async () => "resolved",
    ...over,
  };
  const deps: AnswerApplyDeps = {
    ...raw,
    describeDecision: (qitemId) => raw.describeDecision(qitemId),
    recordHumanAnswer: (input) => {
      recorded.push(input);
      return raw.recordHumanAnswer(input);
    },
    replyRows: {
      createQitem: async (input) => {
        created.push(input);
        return raw.replyRows.createQitem(input);
      },
    },
    resolveHumanReply: async (input) => {
      resolved.push(input);
      return raw.resolveHumanReply(input);
    },
  };
  return { deps, recorded, created, resolved };
}

describe("decisions answer apply — settle, never resolve on its own reading", () => {
  it("an answer for a local row that no longer exists is dropped state-done with nothing recorded", async () => {
    const h = applyHarness({ describeDecision: () => null });

    const res = await applyPulledAnswer(h.deps, pulled());

    expect(res).toEqual({ settled: true, outcome: "dropped", reason: "state-done", complete: false });
    expect(h.recorded).toEqual([]);
    expect(h.created).toEqual([]);
    expect(h.resolved).toEqual([]);
  });

  it("a row whose destination is not a registered human is dropped not-the-asked-human and no answer is recorded", async () => {
    const h = applyHarness({
      describeDecision: () => ({ askerSession: "seat-a@rig-x", humanSession: null }),
    });

    const res = await applyPulledAnswer(h.deps, pulled());

    expect(res).toEqual({ settled: true, outcome: "dropped", reason: "not-the-asked-human", complete: false });
    expect(h.recorded).toEqual([]);
    expect(h.resolved).toEqual([]);
  });

  it("the addressed human is the actor a recorded answer is attributed to", async () => {
    const h = applyHarness({
      recordHumanAnswer: () => ({ status: "recorded", answers: { q1: "yes" }, complete: false, questions: [Q1] }),
    });

    await applyPulledAnswer(h.deps, pulled({ answers: { q1: "no" } }));

    expect(h.recorded).toEqual([
      { qitemId: "qitem-1", actorSession: "founder@rig-x", questionId: "q1", optionId: "no" },
    ]);
  });

  it("a repository refusal whose local reason names a state maps onto the wire's state-done", async () => {
    const h = applyHarness({
      recordHumanAnswer: () => ({ status: "not-applicable", reason: "state-answered" }),
    });

    const res = await applyPulledAnswer(h.deps, pulled());

    expect(res).toEqual({ settled: true, outcome: "dropped", reason: "state-done", complete: false });
    expect(h.created).toEqual([]);
    expect(h.resolved).toEqual([]);
  });

  it("a repository refusal with no clean wire token maps onto unknown-qitem", async () => {
    const h = applyHarness({
      recordHumanAnswer: () => ({ status: "not-applicable", reason: "unknown-option" }),
    });

    const res = await applyPulledAnswer(h.deps, pulled());

    expect(res).toEqual({ settled: true, outcome: "dropped", reason: "unknown-qitem", complete: false });
  });

  it("a partial answer set is settled applied without a reply row or a resolve", async () => {
    const h = applyHarness({
      recordHumanAnswer: () => ({ status: "recorded", answers: { q1: "yes" }, complete: false, questions: [Q1] }),
    });

    const res = await applyPulledAnswer(h.deps, pulled());

    expect(res).toEqual({ settled: true, outcome: "applied", complete: false });
    expect(h.created).toEqual([]);
    expect(h.resolved).toEqual([]);
  });

  it("an empty answer set drops unknown-qitem and records nothing", async () => {
    const h = applyHarness();

    const res = await applyPulledAnswer(h.deps, pulled({ answers: {} }));

    expect(res).toEqual({ settled: true, outcome: "dropped", reason: "unknown-qitem", complete: false });
    expect(h.recorded).toEqual([]);
    expect(h.created).toEqual([]);
  });

  it("a complete set creates the derived reply row in reply to the decision, then resolves it", async () => {
    const h = applyHarness({
      recordHumanAnswer: () => ({
        status: "recorded",
        answers: { q1: "yes", q2: "no" },
        complete: true,
        questions: [Q1, Q2],
      }),
    });

    const res = await applyPulledAnswer(h.deps, pulled());

    expect(res).toEqual({ settled: true, outcome: "applied", complete: true });
    expect(h.created).toHaveLength(1);
    expect(h.created[0]).toMatchObject({
      qitemId: replyRowIdFor("qitem-1"),
      inReplyTo: "qitem-1",
      source: "founder@rig-x",
      destination: "seat-a@rig-x",
    });
    expect(h.resolved).toEqual([
      {
        qitemId: "qitem-1",
        actorSession: "founder@rig-x",
        decision: "Ship it?: Yes; Roll back?: No",
      },
    ]);
  });

  it("a replay whose derived reply row already exists (primary-key conflict) still resolves and reports applied", async () => {
    const conflict = Object.assign(new Error("UNIQUE constraint failed: queue_items.qitem_id"), {
      code: "SQLITE_CONSTRAINT_PRIMARYKEY",
    });
    const h = applyHarness({
      replyRows: { createQitem: async () => { throw conflict; } },
    });

    const res = await applyPulledAnswer(h.deps, pulled());

    expect(res).toEqual({ settled: true, outcome: "applied", complete: true });
    expect(h.resolved).toHaveLength(1);
  });

  it("a reply-row failure that is not the derived-id conflict stays unsettled for a retry", async () => {
    const h = applyHarness({
      replyRows: { createQitem: async () => { throw new Error("disk on fire"); } },
    });

    const res = await applyPulledAnswer(h.deps, pulled());

    expect(res).toEqual({ settled: false, reason: "reply-row-failed: disk on fire" });
    expect(h.resolved).toEqual([]);
  });

  it("a resolve that answers not-applicable is dropped already-applied past the reply row", async () => {
    const h = applyHarness({ resolveHumanReply: async () => "not-applicable" });

    const res = await applyPulledAnswer(h.deps, pulled());

    expect(res).toEqual({ settled: true, outcome: "dropped", reason: "already-applied", complete: true });
    expect(h.created).toHaveLength(1);
  });

  it("a resolve answering already-resolved is a plain applied settle", async () => {
    const h = applyHarness({ resolveHumanReply: async () => "already-resolved" });

    const res = await applyPulledAnswer(h.deps, pulled());

    expect(res).toEqual({ settled: true, outcome: "applied", complete: true });
  });

  it("a storage failure while recording the answer is NOT settled", async () => {
    const h = applyHarness({
      recordHumanAnswer: () => { throw new Error("sqlite busy"); },
    });

    const res = await applyPulledAnswer(h.deps, pulled());

    expect(res).toEqual({ settled: false, reason: "record-failed: sqlite busy" });
    expect(h.created).toEqual([]);
  });

  it("a throwing resolve is NOT settled", async () => {
    const h = applyHarness({
      resolveHumanReply: async () => { throw new Error("mission-control down"); },
    });

    const res = await applyPulledAnswer(h.deps, pulled());

    expect(res).toEqual({ settled: false, reason: "resolve-failed: mission-control down" });
    expect(h.created).toHaveLength(1);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// Ask sweep
// ─────────────────────────────────────────────────────────────────────────────

const homes: string[] = [];
afterEach(() => {
  while (homes.length > 0) rmSync(homes.pop() as string, { recursive: true, force: true });
});

function human(entityId: string, kinds: string[]): HumanFragment {
  return {
    entityId,
    class: "human",
    displayName: entityId,
    address: `${entityId}@external`,
    connectorBindings: kinds.map((kind) => ({
      kind,
      connectorRef: "c",
      secretsRef: "env:X",
      role: "primary",
    })),
    prefs: { deliveryClass: "A" },
  } as unknown as HumanFragment;
}

function registry(entities: HumanFragment[]): LoadResult {
  return { ok: true, entities };
}

const AGENTHUB_HUMAN = (): LoadResult => registry([human("founder", ["agenthub"])]);
const SLACK_ONLY_HUMAN = (): LoadResult => registry([human("founder", ["slack"])]);

function alert(over: Partial<QueueItem> = {}): QueueItem {
  return {
    qitemId: "qitem-1",
    sourceSession: "seat-a@rig-x",
    destinationSession: "founder@rig-x",
    humanIntent: "decision",
    humanQuestions: [Q1],
    summary: "Ship it?",
    humanDetail: "why",
    ...over,
  };
}

interface DispatchCall {
  op: string;
  entityBindingRef: string;
  payload: unknown;
  opts?: { decisionId?: string };
}

function askHarness(opts: {
  alerts: QueueItem[];
  registry?: LoadResult;
  seatKey?: (session: string) => string | null;
  room?: string;
  dispatchResult?: (call: DispatchCall) => { ok: boolean; error?: string };
}): { driver: DecisionsAskDriver; calls: DispatchCall[]; home: string } {
  const home = mkdtempSync(join(tmpdir(), "decisions-ask-"));
  homes.push(home);
  const calls: DispatchCall[] = [];
  const driver = new DecisionsAskDriver({
    home,
    queue: { listHumanAlerts: async () => opts.alerts },
    loadHumanRegistry: () => opts.registry ?? AGENTHUB_HUMAN(),
    dispatch: (op, entityBindingRef, payload, o) => {
      const call: DispatchCall = { op, entityBindingRef, payload, opts: o };
      calls.push(call);
      return opts.dispatchResult ? opts.dispatchResult(call) : { ok: true };
    },
    resolveSeatKey: opts.seatKey ?? (() => "pod.seat-a"),
    filter: {},
    ...(opts.room === undefined ? {} : { room: opts.room }),
  });
  return { driver, calls, home };
}

describe("decisions ask sweep — one mirrored ask per fresh decision", () => {
  it("dispatches a bound pending decision as a decision.ask keyed by the decision identity", async () => {
    const h = askHarness({ alerts: [alert()] });

    const res = await h.driver.sweepOnce();

    expect(res.dispatched).toEqual(["qitem-1"]);
    expect(res.refused).toEqual([]);
    expect(h.calls).toHaveLength(1);
    expect(h.calls[0].op).toBe(ASK_OP);
    expect(h.calls[0].entityBindingRef).toBe("founder@rig-x");
    const payload = h.calls[0].payload as Record<string, unknown>;
    expect(payload.idempotency_key).toBe(decisionIdFor("qitem-1"));
    expect(payload.rig).toBe("rig-x");
    expect(payload.qitem_id).toBe("qitem-1");
    expect(payload.author_seat_key).toBe("pod.seat-a");
    expect(payload.destination).toBe("founder@rig-x");
    expect(payload.questions).toEqual([Q1]);
    expect(payload.room).toBeUndefined();
    expect(h.calls[0].opts?.decisionId).toBe(decisionIdFor("qitem-1"));
  });

  it("carries the configured room and falls back to the session's own member label when no seat key resolves", async () => {
    const h = askHarness({ alerts: [alert()], seatKey: () => null, room: "rig-x-room" });

    await h.driver.sweepOnce();

    const payload = h.calls[0].payload as Record<string, unknown>;
    expect(payload.room).toBe("rig-x-room");
    expect(payload.author_seat_key).toBe("seat-a");
  });

  it("does not dispatch a decision whose human carries only a slack binding", async () => {
    const h = askHarness({ alerts: [alert()], registry: SLACK_ONLY_HUMAN() });

    const res = await h.driver.sweepOnce();

    expect(res.fresh).toBe(0);
    expect(res.dispatched).toEqual([]);
    expect(h.calls).toEqual([]);
  });

  it("does not dispatch an update intent, nor a decision carrying no questions", async () => {
    const h = askHarness({
      alerts: [
        alert({ qitemId: "qitem-update", humanIntent: "update" }),
        alert({ qitemId: "qitem-noquestions", humanQuestions: [] }),
      ],
    });

    const res = await h.driver.sweepOnce();

    expect(res.alerts).toBe(2);
    expect(res.fresh).toBe(0);
    expect(res.dispatched).toEqual([]);
    expect(h.calls).toEqual([]);
  });

  it("a reached ask is history: a second sweep dispatches nothing for it", async () => {
    const h = askHarness({ alerts: [alert()] });

    const first = await h.driver.sweepOnce();
    expect(first.dispatched).toEqual(["qitem-1"]);

    h.driver.markReached("qitem-1");
    expect(h.driver.status().askedEver).toBe(1);

    const second = await h.driver.sweepOnce();

    expect(second.dispatched).toEqual([]);
    expect(h.calls).toHaveLength(1);
  });

  it("refuses a source session that names no rig and never dispatches it", async () => {
    const h = askHarness({ alerts: [alert({ qitemId: "qitem-norig", sourceSession: "seat-a" })] });

    const res = await h.driver.sweepOnce();

    expect(res.dispatched).toEqual([]);
    expect(res.refused).toHaveLength(1);
    expect(res.refused[0].qitemId).toBe("qitem-norig");
    expect(res.refused[0].error).toContain("names no rig");
    expect(h.calls).toEqual([]);
  });

  it("a dispatch refused by the wire is reported and leaves the episode unseen", async () => {
    const h = askHarness({
      alerts: [alert()],
      dispatchResult: () => ({ ok: false, error: "connector offline" }),
    });

    const res = await h.driver.sweepOnce();

    expect(res.dispatched).toEqual([]);
    expect(res.refused).toEqual([{ qitemId: "qitem-1", error: "connector offline" }]);
    expect(h.driver.status().askedEver).toBe(0);

    const retry = await h.driver.sweepOnce();
    expect(retry.dispatched).toEqual([]);
    expect(h.calls).toHaveLength(2);
  });

  it("an unreadable human registry skips the sweep without dispatching", async () => {
    const h = askHarness({
      alerts: [alert()],
      registry: { ok: false, error: "projection drifted" },
    });

    const res = await h.driver.sweepOnce();

    expect(res.alerts).toBe(1);
    expect(res.fresh).toBe(0);
    expect(res.dispatched).toEqual([]);
    expect(h.calls).toEqual([]);
  });
});
