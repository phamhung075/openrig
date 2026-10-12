// S2 (task 6fbd1f95) — the decisions half of the gateway wire: the cloud DECISIONS lane composed
// into the shipped in-process gateway. It is a SECOND TRANSPORT on ONE wire, not a second wire:
// `buildInProcessWire` owns one dispatcher and one durable buffer (gateway/dispatch-buffer.ts,
// keyed by home), so a second wire would share that file and its read-modify-write — a lost-update
// race and a cross-connector replay. Here the ops union and the op-routing `deliver` keep the two
// transports apart by op, which is the same proof-9 rail the dispatcher already enforces.
//
//   ops:     [decision.ask, decision.state] advertised when the credential resolves (§4)
//   ask:     the sweep dispatches; THIS deliver POSTs route 1 and marks the episode reached
//   state:   THIS deliver POSTs route 4 (a locally learned transition with no answer in hand)
//   pull:    the driver's own call (route 2), with the ack (route 3) after each local apply
//
// Everything network-touching is constructed here and STARTED only by the wire's startServices()
// (post-bind): composing a daemon must never dial the cloud, which is the hermeticity rule every
// monitor in the supervision tree follows.

import { getSelfHostId } from "../../hosts/fanout-contract.js";
import { parseHumanQuestions } from "../../human-questions.js";
import { loadHumanRegistry as loadHumanRegistryDefault, resolveRegisteredHumanAddress, type LoadResult } from "../human-registry.js";
import type { QueueRepository } from "../../queue-repository.js";
import type { OwnerNotificationLevel } from "../../queue-transition-log.js";
import type { FetchImpl } from "../slack/slack-api.js";
import { makeQueuePorts, type InboundQueuePort, type OutboundQueuePort } from "../slack/queue-access.js";
import type { SubsystemDeliverFn } from "../gateway-subsystem.js";
import type { OutboundDecision } from "../protocol.js";
import { resolveAgenthubTarget } from "./credentials.js";
import { makeCloudClient, type CloudCall, type CloudClient } from "./cloud-client.js";
import { ASK_OP, DECISIONS_CONNECTOR_KIND, STATE_OP, type AskBody, type StatePushBody } from "./contract.js";
import { applyPulledAnswer } from "./answer-apply.js";
import { DecisionsAskDriver, ASK_SWEEP_INTERVAL_MS } from "./ask-driver.js";
import { DecisionsPullDriver, DEFAULT_PULL_INTERVAL_MS } from "./pull-driver.js";

export interface DecisionsHalfOpts {
  home: string;
  queueRepo: QueueRepository;
  /** The shipped ports (createQitem for the reply row, listHumanAlerts for the sweep). */
  ports?: InboundQueuePort & OutboundQueuePort;
  /** The mission-control resolve contract's resolver — the SAME function the Slack wire binds.
   *  Absent, the pull does not run: without it an applied answer could never close a decision, and
   *  a pull that records answers it cannot hand back would be worse than no pull. */
  resolveHumanReply?: (input: { qitemId: string; actorSession: string; decision: string }) => Promise<"resolved" | "already-resolved" | "not-applicable">;
  /** The daemon's rigs, for the rig-scoped pull (§1). */
  rigs: () => string[];
  /** The daemon's hostId, sent in the cloud's `machine_id` field (§4). */
  hostId?: () => string | null;
  loadHumanRegistry?: (home?: string) => LoadResult;
  fetchImpl?: FetchImpl;
  /** The env file the credential is read from; defaults to ~/.config/4genthub/.env (§4). */
  envFile?: string;
  /** The cloud room, for a rig whose name is not its room's slug (§6.2). Unset = the cloud resolves. */
  room?: string | null;
  askIntervalMs?: number;
  pullIntervalMs?: number;
  /** The owner-classification floor the sweep shares with the Slack outbound half. */
  minimumLevel?: OwnerNotificationLevel;
  log?: (msg: string) => void;
  now?: () => Date;
  setTimeoutFn?: (fn: () => void, ms: number) => unknown;
  clearTimeoutFn?: (handle: unknown) => void;
}

export interface DecisionsHalf {
  /** The credential resolved: the ops may be advertised and the drivers started. */
  ready: boolean;
  /** Why the half is not ready, in the operator's words (null when ready). */
  reason: string | null;
  ops: string[];
  /** Is this op the decisions half's? The composed deliver routes by op through this, so the
   *  Slack transport never has to know the decisions constants. */
  ownsOp(op: string): boolean;
  deliver: SubsystemDeliverFn;
  /** Bind the wire's dispatcher. The deliver must exist before the wire does, so the sweep's
   *  dispatch is filled in immediately after `buildInProcessWire` returns. */
  bindDispatch(fn: (op: string, entityBindingRef: string, payload: unknown, opts?: { decisionId?: string }) => { ok: boolean; error?: string }): void;
  start(): void;
  stop(): void;
  status(): Record<string, unknown>;
}

/** The ingest body, rebuilt from validated fields only: the cloud refuses unknown JSON fields, so
 *  nothing the buffer round-tripped can ride along unvalidated. Each field is checked on the way
 *  out, which is also what keeps the dispatched payload honest against the frozen §1 shape. */
function readAskBody(payload: unknown): { ok: true; body: AskBody } | { ok: false; error: string } {
  if (typeof payload !== "object" || payload === null) return { ok: false, error: "payload is not an object" };
  const rig = "rig" in payload ? payload.rig : undefined;
  const qitemId = "qitem_id" in payload ? payload.qitem_id : undefined;
  const authorSeatKey = "author_seat_key" in payload ? payload.author_seat_key : undefined;
  const destination = "destination" in payload ? payload.destination : undefined;
  const summary = "summary" in payload ? payload.summary : undefined;
  const idempotencyKey = "idempotency_key" in payload ? payload.idempotency_key : undefined;
  for (const [key, value] of [
    ["rig", rig], ["qitem_id", qitemId], ["author_seat_key", authorSeatKey],
    ["destination", destination], ["summary", summary], ["idempotency_key", idempotencyKey],
  ] as const) {
    if (typeof value !== "string" || value.length === 0) return { ok: false, error: `payload.${key} must be a non-empty string` };
  }
  const detail = "detail" in payload ? payload.detail : undefined;
  if (detail !== null && detail !== undefined && typeof detail !== "string") return { ok: false, error: "payload.detail must be a string or null" };
  const room = "room" in payload ? payload.room : undefined;
  if (room !== undefined && room !== null && typeof room !== "string") return { ok: false, error: "payload.room must be a string" };
  const parsedQuestions = parseHumanQuestions("questions" in payload ? payload.questions : undefined);
  if (!parsedQuestions.ok) return { ok: false, error: `payload.questions: ${parsedQuestions.error}` };
  return {
    ok: true,
    body: {
      rig: rig as string,
      ...(typeof room === "string" && room.length > 0 ? { room } : {}),
      qitem_id: qitemId as string,
      author_seat_key: authorSeatKey as string,
      destination: destination as string,
      summary: summary as string,
      detail: typeof detail === "string" ? detail : null,
      questions: parsedQuestions.questions,
      idempotency_key: idempotencyKey as string,
    },
  };
}

function readStateBody(payload: unknown): { ok: true; body: Omit<StatePushBody, "machine_id"> } | { ok: false; error: string } {
  if (typeof payload !== "object" || payload === null) return { ok: false, error: "payload is not an object" };
  const state = "state" in payload ? payload.state : undefined;
  if (state !== "resolved" && state !== "superseded") return { ok: false, error: 'payload.state must be "resolved" or "superseded"' };
  const reason = "reason" in payload ? payload.reason : undefined;
  if (reason !== undefined && typeof reason !== "string") return { ok: false, error: "payload.reason must be a string" };
  return { ok: true, body: { state, ...(typeof reason === "string" ? { reason } : {}) } };
}

/** A 4xx is the cloud refusing the REQUEST: retrying the same bytes changes nothing, so the
 *  refusal is reported on the wire (and on the row) instead of being retried in the dark. 5xx and
 *  transport failures are transient and stay retained for the replay. */
function isPermanentRefusal(res: CloudCall): boolean {
  return !res.ok && res.status !== undefined && res.status >= 400 && res.status < 500;
}

export function buildDecisionsHalf(opts: DecisionsHalfOpts): DecisionsHalf {
  const log = opts.log ?? (() => {});
  const ports = opts.ports ?? makeQueuePorts(opts.queueRepo, { loadHumanRegistry: () => (opts.loadHumanRegistry ?? loadHumanRegistryDefault)() });
  const loadRegistry = opts.loadHumanRegistry ?? (() => loadHumanRegistryDefault());
  const target = resolveAgenthubTarget(opts.envFile === undefined ? {} : { envFile: opts.envFile });
  const ready = target.token !== null;
  if (target.warning) log(`agenthub: ${target.warning}`);
  if (!ready) {
    log(`agenthub decisions not configured (AGENTHUB_TOKEN unresolved in ${target.envFile}) — the wire advertises no decisions op; dispatches would be refused honestly`);
  }
  const hostId = opts.hostId ?? getSelfHostId;

  // The ask's seat key from the daemon's own inventory, by the shipped convention that composes a
  // canonical seat address as REPLACE(logical_id,'.','-')@rig (queue-repository.ts
  // `hasTerminalTransport`). Unresolvable falls back to the session's local leg: the field is
  // descriptive, and a missing inventory row must not block a decision.
  const seatKeyBySession = (sessionName: string): string | null => {
    const at = sessionName.lastIndexOf("@");
    if (at <= 0) return null;
    const local = sessionName.slice(0, at);
    const rig = sessionName.slice(at + 1);
    try {
      const row = opts.queueRepo.db.prepare(
        `SELECT COALESCE(NULLIF(p.namespace, ''), r.name) AS ns, n.logical_id AS logical_id
           FROM nodes n
           JOIN rigs r ON r.id = n.rig_id
           LEFT JOIN pods p ON p.id = n.pod_id
          WHERE r.name = ? AND REPLACE(n.logical_id, '.', '-') = ? LIMIT 1`,
      ).get(rig, local) as { ns: string; logical_id: string } | undefined;
      return row ? `${row.ns}.${row.logical_id}` : null;
    } catch {
      return null; // schema-partial fixture DB: no inventory, no invented key
    }
  };

  const client: CloudClient | null = ready
    ? makeCloudClient({ baseUrl: target.url, token: target.token!, ...(opts.fetchImpl ? { fetchImpl: opts.fetchImpl } : {}) })
    : null;

  // The wire's dispatcher does not exist yet (it is created around this half, which supplies the
  // deliver), so the sweep's dispatch is filled in by bindDispatch() right after the wire returns.
  const lateDispatch: { fn?: (op: string, entityBindingRef: string, payload: unknown, opts?: { decisionId?: string }) => { ok: boolean; error?: string } } = {};
  const askDriver = new DecisionsAskDriver({
    home: opts.home,
    queue: ports,
    loadHumanRegistry: loadRegistry,
    dispatch: (op, ref, payload, dispatchOpts) => lateDispatch.fn?.(op, ref, payload, dispatchOpts) ?? { ok: false, error: "gateway dispatcher not bound yet" },
    resolveSeatKey: seatKeyBySession,
    kind: DECISIONS_CONNECTOR_KIND,
    filter: { minimumLevel: opts.minimumLevel ?? "NOTICE" },
    ...(opts.askIntervalMs === undefined ? {} : { intervalMs: opts.askIntervalMs }),
    log,
  });

  const resolveHumanReply = opts.resolveHumanReply;
  const pullDriver = client === null || resolveHumanReply === undefined ? null : new DecisionsPullDriver({
    client,
    rigs: opts.rigs,
    hostId: () => hostId() ?? "",
    apply: (item) => applyPulledAnswer({
      recordHumanAnswer: (input) => opts.queueRepo.recordHumanAnswer(input),
      replyRows: ports,
      resolveHumanReply,
      describeDecision: (qitemId) => {
        const row = opts.queueRepo.getById(qitemId);
        if (!row) return null;
        const registry = loadRegistry();
        return {
          askerSession: row.sourceSession ?? "",
          humanSession: registry.ok ? resolveRegisteredHumanAddress(row.destinationSession, registry.entities) : null,
        };
      },
      log,
    }, item),
    ...(opts.pullIntervalMs === undefined ? {} : { intervalMs: opts.pullIntervalMs }),
    ...(opts.setTimeoutFn ? { setTimeoutFn: opts.setTimeoutFn } : {}),
    ...(opts.clearTimeoutFn ? { clearTimeoutFn: opts.clearTimeoutFn } : {}),
    ...(opts.now ? { now: opts.now } : {}),
    log,
  });

  /** One refusal note per distinct cause, on the row the operator is looking at. */
  const noteRefusal = (qitemId: string, cause: string): void => {
    const note = `agenthub-ask-refused ${cause}`;
    try {
      const existing = opts.queueRepo.transitionLog.listForQitem(qitemId).map((t) => t.transitionNote ?? "");
      if (existing.includes(note)) return;
      opts.queueRepo.update({ qitemId, actorSession: "daemon@kernel", transitionNote: note });
    } catch (e) {
      log(`could not note the ask refusal on ${qitemId}: ${(e as Error).message}`);
    }
  };

  const deliver: SubsystemDeliverFn = async (decision: OutboundDecision) => {
    if (client === null) return { ok: false as const, class: "agenthub-not-configured", detail: `AGENTHUB_TOKEN unresolved in ${target.envFile}` };
    if (decision.op === ASK_OP) {
      const parsed = readAskBody(decision.payload);
      if (!parsed.ok) return { ok: false as const, class: "ask-payload-invalid", detail: parsed.error };
      const res = await client.postAsk(parsed.body);
      if (res.ok) {
        askDriver.markReached(parsed.body.qitem_id);
        return { ok: true as const };
      }
      const klass = isPermanentRefusal(res) ? "ask-refused" : "ask-transport";
      if (isPermanentRefusal(res)) noteRefusal(parsed.body.qitem_id, res.error);
      log(`ask for ${parsed.body.qitem_id} not delivered (${res.error}) — retained in the dispatch buffer; the row carries the refusal${isPermanentRefusal(res) ? "" : " (transient, replay will retry)"}`);
      return { ok: false as const, class: klass, detail: res.error };
    }
    if (decision.op === STATE_OP) {
      const parsed = readStateBody(decision.payload);
      if (!parsed.ok) return { ok: false as const, class: "state-payload-invalid", detail: parsed.error };
      const decisionId = decision.entityBindingRef;
      const res = await client.pushState(decisionId, { machine_id: hostId() ?? "", ...parsed.body });
      if (res.ok) return { ok: true as const };
      // §1 route 4: an illegal transition is 409 "state-<state>" — the cloud already holds a
      // terminal state, which is a settled fact for this push, not a retry.
      if (res.status === 409) {
        log(`state push ${decisionId} settled by the cloud (${res.error}) — treated as delivered`);
        return { ok: true as const };
      }
      log(`state push ${decisionId} failed (${res.error}) — retained in the dispatch buffer`);
      return { ok: false as const, class: "state-transport", detail: res.error };
    }
    // The composed deliver routes by op, so this arm is unreachable in the shipped wiring: it is
    // the honest refusal shape rather than a silent success for an op this half does not own.
    return { ok: false as const, class: "unknown-op", detail: `decisions half does not deliver op "${decision.op}"` };
  };

  return {
    ready,
    reason: ready ? null : `AGENTHUB_TOKEN unresolved in ${target.envFile}`,
    ops: ready ? [ASK_OP, STATE_OP] : [],
    ownsOp: (op) => op === ASK_OP || op === STATE_OP,
    deliver,
    bindDispatch: (fn) => { lateDispatch.fn = fn; },
    start: () => {
      if (!ready) return;
      askDriver.start();
      pullDriver?.start();
    },
    stop: () => {
      askDriver.stop();
      pullDriver?.stop();
    },
    status: () => ({
      ready,
      reason: ready ? null : `AGENTHUB_TOKEN unresolved in ${target.envFile}`,
      url: target.url,
      envFile: target.envFile,
      credential: ready ? "resolved" : "unresolved",
      ops: ready ? [ASK_OP, STATE_OP] : [],
      ask: askDriver.status(),
      pull: pullDriver?.status() ?? {
        state: "unavailable",
        reason: client === null ? `AGENTHUB_TOKEN unresolved in ${target.envFile}` : "no resolve contract bound (the pull would record answers it could not hand back)",
      },
    }),
  };
}
