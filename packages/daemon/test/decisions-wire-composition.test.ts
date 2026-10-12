// S2 (task 6fbd1f95) — the decisions lane composed onto the REAL gateway wire, over a real queue
// database. Two things this file exists to prove that unit tests of the drivers cannot:
//
//   1. ONE wire carries BOTH transports: the ops union advertises decision.ask/decision.state
//      beside Slack's op, and the op-routed deliver sends each op to its own transport — proven by
//      watching which fake saw the request, and by a foreign op still being refused (proof-9).
//   2. The answer half closes a REAL decision end to end: a pulled answer recorded through the
//      repository, the reply row landed with the derived id, the mission-control resolve contract
//      closing the row `done`/`no-follow-on` with the owner-notification transition ONCE, and a
//      second delivery of the same answer changing nothing (at-least-once, never twice).
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
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
import { DEFAULT_CONFIG, saveConfig } from "../src/domain/gateway/slack/config.js";
import { resolveSlackHandle } from "../src/domain/gateway/human-registry.js";
import { MissionControlActionLog } from "../src/domain/mission-control/mission-control-action-log.js";
import { MissionControlWriteContract } from "../src/domain/mission-control/mission-control-write-contract.js";
import { DispatchBuffer } from "../src/domain/gateway/dispatch-buffer.js";
import { ASK_OP, DECISIONS_ASK_PATH, DECISIONS_ANSWERS_PATH, STATE_OP, decisionIdFor, decisionsStatePath } from "../src/domain/gateway/decisions/contract.js";
import { getSelfHostId } from "../src/domain/hosts/fanout-contract.js";
import { toAskBody } from "../src/domain/gateway/decisions/ask-driver.js";
import { applyPulledAnswer, replyRowIdFor } from "../src/domain/gateway/decisions/answer-apply.js";
import type { WsLike } from "../src/domain/gateway/slack/socket-inbound.js";
import type { HumanQuestion } from "../src/domain/human-questions.js";

const human = "human-founder@external";
const seat = "4genthub-min-lead@4genthub-min";
// The frozen binding of the addendum §4: an inline literal whose kind is the new set member.
const registry = {
  ok: true as const,
  entities: [{
    entityId: "human-founder",
    class: "human" as const,
    displayName: "Founder",
    address: human,
    connectorBindings: [
      { kind: "slack", connectorRef: "primary", secretsRef: "env:SLACK_BOT_TOKEN", role: "primary" as const, handle: "UFOUNDER" },
      { kind: "agenthub", connectorRef: "agenthub-cloud", secretsRef: "AGENTHUB_TOKEN", role: "primary" as const },
    ],
    prefs: { deliveryClass: "A" as const },
  }],
};
const questions: HumanQuestion[] = [
  { id: "db", question: "Which database?", options: [{ id: "pg", label: "Postgres", recommended: true }, { id: "sqlite", label: "SQLite" }] },
];
const request = {
  sourceSession: seat,
  destinationSession: human,
  summary: "One quick question",
  body: "Pick one.",
  evidenceRef: null,
  nudge: false,
};

interface CloudCall { method: string; url: string; body: Record<string, unknown> | null }

describe("decisions lane on the composed gateway wire (S2)", () => {
  let home: string;
  let db: ReturnType<typeof createDb>;
  let bus: EventBus;
  let repo: QueueRepository;
  let envFile: string;
  let cloud: CloudCall[];
  let slackPosts: Array<Record<string, unknown>>;
  const stops: Array<() => void> = [];

  beforeEach(() => {
    home = mkdtempSync(join(tmpdir(), "decisions-wire-"));
    db = createDb();
    migrate(db, ALL_MIGRATIONS);
    bus = new EventBus(db);
    repo = new QueueRepository(db, bus, { loadHumanRegistry: () => registry });
    envFile = join(home, "agenthub.env");
    writeFileSync(envFile, "AGENTHUB_URL=http://cloud.invalid\nAGENTHUB_TOKEN=tok-EXAMPLE-not-a-real-credential\n", { mode: 0o600 });
    cloud = [];
    slackPosts = [];
  });

  afterEach(() => {
    for (const stop of stops.splice(0)) stop();
    db.close();
    rmSync(home, { recursive: true, force: true });
  });

  const reply = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });

  /** Build the ONE wire with both transports, faking only the two networks. */
  function buildWire(opts: { slackEnabled: boolean; answers?: () => Response; askStatus?: number; stateStatus?: number }) {
    if (opts.slackEnabled) {
      const secrets = join(home, "slack.env");
      writeFileSync(secrets, "SLACK_BOT_TOKEN=xoxb-EXAMPLE-fake\nSLACK_APP_TOKEN=xapp-EXAMPLE-fake\n", { mode: 0o600 });
      saveConfig({ ...DEFAULT_CONFIG, enabled: true, channel: "C-TEST", secretsEnvFile: secrets, minimumLevelThatInterrupts: "NOTICE" }, home);
    }
    const sockets: WsLike[] = [];
    const wire = buildSlackGatewayWire({
      home,
      queueRepo: repo,
      registry: { loadHumanRegistry: () => registry, resolveSlackHandle },
      resolveHumanReply: makeHumanReplyResolver(repo, new MissionControlWriteContract({ db, eventBus: bus, queueRepo: repo, actionLog: new MissionControlActionLog(db) })),
      wsFactory: () => { const ws: WsLike = { send: () => {}, close: () => {}, onopen: null, onmessage: null, onclose: null, onerror: null }; sockets.push(ws); return ws; },
      inboundMaxConnects: 1,
      inboundRetryIntervalMs: 50,
      // Both halves share one fetch seam: route by URL, so a request reaching the wrong transport
      // is visible as the wrong fake having seen it.
      fetchImpl: async (url, init) => {
        if (url.includes("slack.com")) {
          if (url.endsWith("apps.connections.open")) return reply({ ok: true, url: "wss://fake-slack/ws" });
          slackPosts.push(JSON.parse(String(init?.body)) as Record<string, unknown>);
          return reply({ ok: true, ts: `${slackPosts.length}.1` });
        }
        cloud.push({ method: init?.method ?? "GET", url, body: init?.body ? (JSON.parse(String(init.body)) as Record<string, unknown>) : null });
        if (url.includes(DECISIONS_ANSWERS_PATH)) return (opts.answers ?? (() => reply({ success: true, answers: [], cursor: "" })))();
        // The state URL CONTAINS the ask path as a prefix, so it is matched first.
        if (url.endsWith("/state")) {
          return opts.stateStatus === undefined ? reply({ success: true }) : reply({ detail: "state-resolved" }, opts.stateStatus);
        }
        if (url.includes(DECISIONS_ASK_PATH)) {
          return opts.askStatus === undefined
            ? reply({ success: true, created: true, decision: { id: "dec-1" } })
            : reply({ detail: 'room "4genthub-min" not found' }, opts.askStatus);
        }
        return reply({ success: true });
      },
      agenthubEnvFile: envFile,
      rigNames: () => ["4genthub-min"],
      // 60s: the drivers' own ticks never fire inside a test; a test drives the path it asserts.
      agenthubAskIntervalMs: 60_000,
      agenthubPullIntervalMs: 60_000,
    });
    stops.push(() => wire.stop());
    wire.startServices?.();
    return wire;
  }

  /** The pending decision the tests mirror, plus the alert projection the sweep reads. */
  async function pendingDecision() {
    const created = await repo.create({ ...request, humanIntent: "decision", humanQuestions: questions });
    const port = makeQueuePorts(repo, { loadHumanRegistry: () => registry });
    const alerts = await port.listHumanAlerts({ minimumLevel: "NOTICE" });
    const alert = alerts.find((q) => q.qitemId === created.qitemId);
    if (!alert) throw new Error("fixture: the created decision did not project as a human alert");
    return { qitemId: created.qitemId, alert, port };
  }

  it("delivers the swept ask to the cloud through the one dispatcher, and drains the buffer only after the cloud accepted it", async () => {
    const wire = buildWire({ slackEnabled: true });
    const { qitemId, alert } = await pendingDecision();

    const built = toAskBody(alert, { resolveSeatKey: () => "4genthub-min.lead" });
    expect(built.ok).toBe(true);
    if (!built.ok) return;
    expect(built.body).toMatchObject({ rig: "4genthub-min", qitem_id: qitemId, destination: human, idempotency_key: decisionIdFor(qitemId) });

    const dispatched = wire.dispatcher.dispatch(ASK_OP, human, built.body, { decisionId: built.body.idempotency_key });
    expect(dispatched.ok).toBe(true);
    await vi.waitFor(() => expect(cloud.filter((c) => c.url.includes(DECISIONS_ASK_PATH))).toHaveLength(1));

    const call = cloud.find((c) => c.url.includes(DECISIONS_ASK_PATH))!;
    expect(call.method).toBe("POST");
    expect(call.body).toMatchObject({ rig: "4genthub-min", idempotency_key: decisionIdFor(qitemId), questions });
    // The delivery is ack-gated: the durable buffer is empty only because the cloud answered 2xx.
    await vi.waitFor(() => expect(new DispatchBuffer(home).pending()).toHaveLength(0));
    // Op routing: the SAME wire sent nothing to Slack for this op.
    expect(slackPosts).toHaveLength(0);
  });

  it("advertises both transports' ops on one wire and still refuses an unadvertised op", async () => {
    const wire = buildWire({ slackEnabled: true });
    const { alert } = await pendingDecision();
    const built = toAskBody(alert, { resolveSeatKey: () => null });
    if (!built.ok) throw new Error(built.error);

    expect(wire.dispatcher.dispatch(ASK_OP, human, built.body, { decisionId: built.body.idempotency_key }).ok).toBe(true);
    // The Slack op carries the row's real projection (the shape the outbound driver dispatches),
    // so the delivery engine's own consult sees the classification the queue wrote.
    expect(wire.dispatcher.dispatch("post_message", human, alert).ok).toBe(true);
    const refused = wire.dispatcher.dispatch("upload_file", human, {});
    expect(refused.ok).toBe(false);
    expect(refused.error).toContain("not advertised");

    await vi.waitFor(() => expect(slackPosts).toHaveLength(1));
    await vi.waitFor(() => expect(cloud.filter((c) => c.url.includes(DECISIONS_ASK_PATH))).toHaveLength(1));
  });

  it("keeps the decisions lane live on an otherwise inert Slack wire", async () => {
    const wire = buildWire({ slackEnabled: false });
    const { alert } = await pendingDecision();
    const built = toAskBody(alert, { resolveSeatKey: () => null });
    if (!built.ok) throw new Error(built.error);

    expect(wire.dispatcher.dispatch(ASK_OP, human, built.body, { decisionId: built.body.idempotency_key }).ok).toBe(true);
    await vi.waitFor(() => expect(cloud.filter((c) => c.url.includes(DECISIONS_ASK_PATH))).toHaveLength(1));
    // The slack op is NOT advertised when Slack is unconfigured, and says so as a refusal.
    expect(wire.dispatcher.dispatch("post_message", human, {}).ok).toBe(false);
  });

  it("keeps an ask the cloud refuses in the durable buffer and notes the refusal on the row", async () => {
    const wire = buildWire({ slackEnabled: false, askStatus: 404 });
    const { qitemId, alert } = await pendingDecision();
    const built = toAskBody(alert, { resolveSeatKey: () => null });
    if (!built.ok) throw new Error(built.error);

    expect(wire.dispatcher.dispatch(ASK_OP, human, built.body, { decisionId: built.body.idempotency_key }).ok).toBe(true);
    await vi.waitFor(() => expect(cloud.filter((c) => c.url.includes(DECISIONS_ASK_PATH))).toHaveLength(1));
    // A 4xx will not change on a retry of the same bytes, so it is REPORTED and RETAINED: the
    // buffer keeps the ask (never dropped), and the row itself carries the refusal.
    await vi.waitFor(() => expect(repo.transitionLog.listForQitem(qitemId).map((t) => t.transitionNote ?? "").some((n) => n.startsWith("agenthub-ask-refused"))).toBe(true));
    expect(new DispatchBuffer(home).pending().map((d) => d.decisionId)).toEqual([decisionIdFor(qitemId)]);
  });

  it("runs the pull half from the wire's own services", async () => {
    buildWire({ slackEnabled: false });
    // startServices() (post-bind) is what starts the lane's drivers: the pull asked the cloud for
    // answers for each of the daemon's rigs, and an empty page left no work behind.
    await vi.waitFor(() => expect(cloud.filter((c) => c.url.includes(DECISIONS_ANSWERS_PATH))).toHaveLength(1));
    expect(cloud.find((c) => c.url.includes(DECISIONS_ANSWERS_PATH))!.url).toContain("rig=4genthub-min");
    expect(new DispatchBuffer(home).pending()).toHaveLength(0);
  });

  it("applies a pulled answer end to end: the reply row lands, the decision closes once, and a redelivery changes nothing", async () => {
    buildWire({ slackEnabled: false });
    const { qitemId, port } = await pendingDecision();
    const deps = {
      recordHumanAnswer: (input: { qitemId: string; actorSession: string; questionId: string; optionId: string }) => repo.recordHumanAnswer(input),
      replyRows: port,
      resolveHumanReply: makeHumanReplyResolver(repo, new MissionControlWriteContract({ db, eventBus: bus, queueRepo: repo, actionLog: new MissionControlActionLog(db) })),
      describeDecision: () => ({ askerSession: seat, humanSession: human }),
    };
    const pulled = {
      decision_id: decisionIdFor(qitemId), qitem_id: qitemId, author_seat_key: "4genthub-min.lead",
      room: "4genthub-min", answers: { db: "pg" }, answered_at: "2026-10-12T19:40:11.123456789Z",
    };

    const first = await applyPulledAnswer(deps, pulled);
    expect(first).toMatchObject({ settled: true, outcome: "applied", complete: true });

    const closed = repo.getById(qitemId);
    expect(closed).toMatchObject({ state: "done", closureReason: "no-follow-on" });
    const replyRow = repo.list({ limit: 100 }).find((q) => q.qitemId === replyRowIdFor(qitemId));
    expect(replyRow).toMatchObject({ destinationSession: seat, replyTo: qitemId });
    const resolutions = repo.transitionLog.listForQitem(qitemId).filter((t) => t.ownerNotificationKind === "human-decision-resolved");
    expect(resolutions).toHaveLength(1);

    // At-least-once: the cloud may hand the same answer out again. The second delivery must land
    // nothing new — no second reply row, no second owner notification.
    const second = await applyPulledAnswer(deps, pulled);
    expect(second.settled).toBe(true);
    expect(repo.list({ limit: 100 }).filter((q) => q.qitemId === replyRowIdFor(qitemId))).toHaveLength(1);
    expect(repo.transitionLog.listForQitem(qitemId).filter((t) => t.ownerNotificationKind === "human-decision-resolved")).toHaveLength(1);
  });

  it("pins the state route's request shape for the hook that will produce it (this slice has none)", async () => {
    const wire = buildWire({ slackEnabled: false });
    const decisionId = decisionIdFor("qitem-state-fixture");

    // §6.7: nothing in the daemon dispatches this op yet — the hook that observes a Slack-first
    // answer (or a withdrawn local row) is a later slice, and neither transition out of `answered`
    // is exercised end to end here. This case is the TARGET that hook lands on.
    expect(wire.dispatcher.dispatch(STATE_OP, decisionId, { state: "superseded", reason: "slack-first" }, { decisionId }).ok).toBe(true);
    await vi.waitFor(() => expect(cloud.filter((c) => c.url.endsWith("/state"))).toHaveLength(1));

    const call = cloud.find((c) => c.url.endsWith("/state"))!;
    expect(call.method).toBe("POST");
    expect(call.url).toContain(decisionsStatePath(decisionId));
    expect(call.body).toMatchObject({ state: "superseded", reason: "slack-first" });
    // §4: `machine_id` carries the daemon's hostId (empty only on a host that has none, as here).
    expect(call.body.machine_id).toBe(getSelfHostId() ?? "");
    // Delivered, so the ack-gated drain took the push out of the durable buffer.
    await vi.waitFor(() => expect(new DispatchBuffer(home).pending().some((d) => d.decisionId === decisionId)).toBe(false));

    // The payload is validated on the way out: a state the route cannot carry is refused before
    // any byte leaves, and the wire RETAINS it (a delivery failure is never silently dropped).
    const badId = `${decisionId}#bad`;
    expect(wire.dispatcher.dispatch(STATE_OP, badId, { state: "pending" }, { decisionId: badId }).ok).toBe(true);
    await vi.waitFor(() => expect(new DispatchBuffer(home).pending().some((d) => d.decisionId === badId)).toBe(true));
    expect(cloud.filter((c) => c.url.endsWith("/state"))).toHaveLength(1);
  });

  it("a 409 on the state push is a settled fact for that push, not a retry", async () => {
    const wire = buildWire({ slackEnabled: false, stateStatus: 409 });
    const decisionId = decisionIdFor("qitem-state-conflict");

    expect(wire.dispatcher.dispatch(STATE_OP, decisionId, { state: "resolved" }, { decisionId }).ok).toBe(true);
    await vi.waitFor(() => expect(cloud.filter((c) => c.url.endsWith("/state"))).toHaveLength(1));
    // §1 route 4: the illegal transition means the cloud already holds a terminal state for the
    // row — treated as delivered, so the push leaves the buffer instead of replaying forever.
    await vi.waitFor(() => expect(new DispatchBuffer(home).pending()).toHaveLength(0));
  });
});
