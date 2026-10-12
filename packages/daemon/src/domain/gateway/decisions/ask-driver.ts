// S2 (task 6fbd1f95) — the ASK sweep: pending DECISIONS whose human carries an `agenthub` binding
// are mirrored to the cloud through the durable dispatcher (addendum §4: op `decision.ask` maps to
// route 1 and rides the same ack-gated buffer as Slack's posts, so an outage loses nothing and a
// redrive collapses into one cloud row).
//
// Structure is the shipped Slack outbound driver's, deliberately (slack/outbound-driver.ts):
// interval poll, `sweeping` guard, a seen-store for "already reached the transport", an inflight
// set seeded from the durable buffer, one dispatch per fresh episode. Its SELECTION is the shipped
// human-alert set with ONE added condition — a decision, and a destination bound to this kind —
// because the classification itself belongs to the queue's own transition log, never to a second
// classifier invented here.
//
// Unlike the Slack half there is NO enable-time backlog seeding: an ask is idempotent at the cloud
// (the ingest key is the decision id) and an OPEN decision SHOULD be answerable in the panel, so
// replaying the currently-pending set once on enable is the intent, not spam.

import path from "node:path";
import { sessionRigOf, sessionMemberLabel } from "../../session-name.js";
import { DispatchBuffer } from "../dispatch-buffer.js";
import { SeenStore } from "../slack/state-store.js";
import { ASK_OP, DECISIONS_CONNECTOR_KIND, decisionIdFor, type AskBody } from "./contract.js";
import type { LoadResult, HumanFragment } from "../human-registry.js";
import type { OwnerNotificationLevel } from "../../queue-transition-log.js";
import type { QueueItem } from "../slack/queue-access.js";

/** The shipped outbound cadence (addendum §5 reuses it for the pull; the sweep shares it so one
 *  operational number explains both halves). */
export const ASK_SWEEP_INTERVAL_MS = 30000;

export interface AskDispatchResult {
  ok: boolean;
  error?: string;
}

export interface AskDriverDeps {
  home: string;
  /** The shipped human-alert read (queue-access `OutboundQueuePort`). */
  queue: { listHumanAlerts(filter: { minimumLevel?: OwnerNotificationLevel }): Promise<QueueItem[]> };
  loadHumanRegistry: () => LoadResult;
  /** The wire's dispatcher. `decisionId` is the frozen identity, so a re-dispatch is an
   *  idempotent accept and never a second record. */
  dispatch: (op: string, entityBindingRef: string, payload: unknown, opts?: { decisionId?: string }) => AskDispatchResult;
  /** The seat key the cloud row carries (`<pod namespace>.<logical id>`), resolved from the
   *  daemon's own inventory; null falls back to the session's local leg. */
  resolveSeatKey: (sessionName: string) => string | null;
  /** The cloud room for a rig whose name is not its room's slug (§6.2). Unset sends no `room`,
   *  and the cloud resolves it from `rig`. */
  room?: string | null;
  filter: { minimumLevel?: OwnerNotificationLevel };
  intervalMs?: number;
  /** The binding kind this sweep serves; the frozen member of HUMAN_CONNECTOR_KINDS. */
  kind?: string;
  seen?: SeenStore;
  log?: (msg: string) => void;
}

export interface AskSweepResult {
  alerts: number;
  fresh: number;
  dispatched: string[];
  refused: Array<{ qitemId: string; error: string }>;
}

/** The registry's own membership test for this wire: a human bound to this kind. */
function humanHasKindBinding(entities: readonly HumanFragment[], address: string, kind: string): boolean {
  const local = sessionMemberLabel(address);
  for (const e of entities) {
    if (e.entityId !== local) continue;
    if (e.connectorBindings.some((b) => b.kind === kind)) return true;
  }
  return false;
}

/** Build the ingest body for one alert. Null + a reason when the row cannot be mirrored honestly
 *  (an unparsable source session names no rig, so the cloud could not resolve a room). */
export function toAskBody(
  alert: QueueItem,
  opts: { room?: string | null; resolveSeatKey: (s: string) => string | null },
): { ok: true; body: AskBody } | { ok: false; error: string } {
  const source = alert.sourceSession ?? "";
  const rig = sessionRigOf(source);
  if (!rig) return { ok: false, error: `source session "${source}" names no rig` };
  const questions = alert.humanQuestions ?? [];
  if (questions.length === 0) return { ok: false, error: "decision carries no questions" };
  const qitemId = alert.qitemId;
  return {
    ok: true,
    body: {
      rig,
      ...(opts.room ? { room: opts.room } : {}),
      qitem_id: qitemId,
      author_seat_key: opts.resolveSeatKey(source) ?? sessionMemberLabel(source),
      destination: alert.destinationSession ?? "",
      summary: alert.summary ?? "(no summary)",
      detail: alert.humanDetail ?? null,
      questions,
      idempotency_key: decisionIdFor(qitemId),
    },
  };
}

export class DecisionsAskDriver {
  private timer: ReturnType<typeof setInterval> | undefined;
  private sweeping = false;
  private readonly seen: SeenStore;
  private readonly kind: string;
  /** Episodes dispatched this process, awaiting their transport outcome (dynamic membership). */
  private readonly inflight = new Set<string>();

  constructor(private readonly deps: AskDriverDeps) {
    this.seen = deps.seen ?? new SeenStore(path.join(deps.home, "state", "agenthub-ask-seen.jsonl"));
    this.kind = deps.kind ?? DECISIONS_CONNECTOR_KIND;
  }

  /** Seed inflight from the durable buffer (restart no-double-dispatch), then start the poll.
   *  Mirrors the Slack driver: no immediate tick, and the timer is unref'd so a daemon with the
   *  gateway composed still exits. */
  start(): void {
    try {
      for (const d of new DispatchBuffer(this.deps.home).pending()) {
        const payload = d.payload;
        if (d.op !== ASK_OP || typeof payload !== "object" || payload === null || !("qitem_id" in payload)) continue;
        const qitemId = payload.qitem_id;
        if (typeof qitemId === "string" && qitemId.length > 0) this.inflight.add(`agenthub:${qitemId}`);
      }
    } catch { /* an unreadable buffer still dedups: the seen store and the cloud's own idempotency */ }
    const interval = this.deps.intervalMs ?? ASK_SWEEP_INTERVAL_MS;
    this.timer = setInterval(() => void this.sweepOnce(), interval);
    if (typeof (this.timer as unknown as { unref?: () => void }).unref === "function") {
      (this.timer as unknown as { unref: () => void }).unref();
    }
  }

  stop(): void {
    clearInterval(this.timer);
    this.timer = undefined;
  }

  /** One sweep. The seen store is marked by the DELIVERY layer (`markReached`) once the ask
   *  actually reached the cloud, so a dispatch that fails inside the buffer stays fresh. */
  async sweepOnce(): Promise<AskSweepResult> {
    if (this.sweeping) return { alerts: 0, fresh: 0, dispatched: [], refused: [] };
    this.sweeping = true;
    const log = this.deps.log ?? (() => {});
    try {
      const alerts = await this.deps.queue.listHumanAlerts(this.deps.filter);
      const registry = this.deps.loadHumanRegistry();
      if (!registry.ok) {
        log(`agenthub ask sweep skipped: human registry unreadable (${registry.error})`);
        return { alerts: alerts.length, fresh: 0, dispatched: [], refused: [] };
      }
      const seen = this.seen.load();
      const fresh = alerts.filter((q) =>
        q.humanIntent === "decision" &&
        (q.humanQuestions?.length ?? 0) > 0 &&
        humanHasKindBinding(registry.entities, q.destinationSession ?? "", this.kind) &&
        !seen.has(`agenthub:${q.qitemId}`) &&
        !this.inflight.has(`agenthub:${q.qitemId}`));
      const dispatched: string[] = [];
      const refused: Array<{ qitemId: string; error: string }> = [];
      for (const alert of fresh) {
        const built = toAskBody(alert, { room: this.deps.room, resolveSeatKey: this.deps.resolveSeatKey });
        if (!built.ok) {
          refused.push({ qitemId: alert.qitemId, error: built.error });
          log(`ask refused for ${alert.qitemId}: ${built.error}`);
          continue;
        }
        const res = this.deps.dispatch(ASK_OP, alert.destinationSession ?? "(unknown)", built.body, { decisionId: built.body.idempotency_key });
        if (res.ok) {
          this.inflight.add(`agenthub:${alert.qitemId}`);
          dispatched.push(alert.qitemId);
        } else {
          refused.push({ qitemId: alert.qitemId, error: res.error ?? "dispatch refused" });
          log(`ask dispatch refused for ${alert.qitemId}: ${res.error ?? "unknown"}`);
        }
      }
      return { alerts: alerts.length, fresh: fresh.length, dispatched, refused };
    } catch (e) {
      log(`agenthub ask sweep failed: ${(e as Error).message}`);
      return { alerts: 0, fresh: 0, dispatched: [], refused: [{ qitemId: "(sweep)", error: (e as Error).message }] };
    } finally {
      this.sweeping = false;
    }
  }

  /** The delivery layer reports an ask that reached the cloud: history for the sweep, and this
   *  episode leaves the inflight set (a later transition mints a new episode and asks again). */
  markReached(qitemId: string): void {
    this.seen.mark(`agenthub:${qitemId}`, "ask-delivered");
    this.inflight.delete(`agenthub:${qitemId}`);
  }

  status(): { inflight: number; askedEver: number } {
    return { inflight: this.inflight.size, askedEver: this.seen.load().size };
  }
}
