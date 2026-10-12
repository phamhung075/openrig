// S2 (task 6fbd1f95) — the PULL driver: the answer comes down a client-initiated pull, so this is
// the only network loop the decisions half owns. Behaviour is the addendum's §5, verbatim:
//
//   - 30 000 ms base cadence (the shipped Slack outbound default), one IMMEDIATE first tick;
//   - each tick drains pages until the cursor is empty, so a burst costs one tick, not one page
//     per cadence;
//   - a transport failure doubles the interval up to 300 000 ms and the first success resets it;
//   - ticks never overlap.
//
// The ACK, not the cursor, is the delivery record (§2). So the cursor advances ONLY past rows that
// were settled AND acked: an apply that needs a retry, a refused page, or an ack that failed
// leaves the cursor where it was, and the row — which the cloud still counts as un-acked — comes
// back on the next tick. Nothing is remembered between ticks about what was pulled: a restart
// re-pulls from the first page, and every step of the apply is idempotent.
//
// A page shape the parser rejects stops the drain WITHOUT advancing. Skipping past an unparsed row
// would be exactly the silent loss at-least-once forbids, so a malformed page is a loud, repeated
// error the operator can see instead.

import { ackAlreadySettled, parseAnswersPage, type CloudCall, type CloudClient } from "./cloud-client.js";
import type { DropReason, PulledAnswer } from "./contract.js";
import type { ApplyResult } from "./answer-apply.js";

/** The shipped cadence and its ceiling (addendum §5; slack/outbound-driver.ts `?? 30000`). */
export const DEFAULT_PULL_INTERVAL_MS = 30000;
export const MAX_PULL_INTERVAL_MS = 300000;

export interface PullDriverDeps {
  client: CloudClient;
  /** The daemon's rigs. The machine routes are rig-scoped (§1), so each rig drains separately and
   *  a daemon hosting several rigs pulls each one's answers. */
  rigs: () => string[];
  apply: (item: PulledAnswer) => Promise<ApplyResult>;
  /** §4: the cloud's field is `machine_id`; the daemon's value is its hostId. */
  hostId: () => string;
  intervalMs?: number;
  maxIntervalMs?: number;
  setTimeoutFn?: (fn: () => void, ms: number) => unknown;
  clearTimeoutFn?: (handle: unknown) => void;
  log?: (msg: string) => void;
  now?: () => Date;
}

export interface PullStatus {
  state: "idle" | "running" | "stopped";
  intervalMs: number;
  rigs: string[];
  lastTickAt: string | null;
  lastError: string | null;
  ticks: number;
  applied: number;
  dropped: number;
}

/** Transport-class vs contract-class: only the first earns backoff. A 4xx means the request was
 *  wrong (a cursor the cloud never minted), which retrying slower would not fix. */
function isTransportFailure(res: CloudCall): boolean {
  return !res.ok && (res.status === undefined || res.status >= 500);
}

/** Drain one rig. Returns true when the driver may continue to the next rig, false when the tick
 *  must stop with this rig's cursor unmoved (a row is owed a retry). */
type DrainOutcome = { continueTicking: boolean; transportFailed: boolean };

export class DecisionsPullDriver {
  private readonly intervalMs: number;
  private readonly maxIntervalMs: number;
  private readonly setTimeoutFn: (fn: () => void, ms: number) => unknown;
  private readonly clearTimeoutFn: (handle: unknown) => void;
  private readonly cursors = new Map<string, string>();
  private interval: number;
  private timer: unknown;
  private ticking = false;
  private state: PullStatus["state"] = "idle";
  private lastTickAt: string | null = null;
  private lastError: string | null = null;
  private ticks = 0;
  private applied = 0;
  private dropped = 0;

  constructor(private readonly deps: PullDriverDeps) {
    this.intervalMs = deps.intervalMs ?? DEFAULT_PULL_INTERVAL_MS;
    this.maxIntervalMs = deps.maxIntervalMs ?? MAX_PULL_INTERVAL_MS;
    this.interval = this.intervalMs;
    this.setTimeoutFn = deps.setTimeoutFn ?? ((fn, ms) => setTimeout(fn, ms));
    this.clearTimeoutFn = deps.clearTimeoutFn ?? ((h) => clearTimeout(h as NodeJS.Timeout));
  }

  /** Idempotent: a second start is a no-op, so the post-bind call cannot double-poll. */
  start(): void {
    if (this.state === "running") return;
    this.state = "running";
    this.schedule(0); // one immediate tick at wire start, then the cadence
  }

  stop(): void {
    this.state = "stopped";
    if (this.timer !== undefined) {
      this.clearTimeoutFn(this.timer);
      this.timer = undefined;
    }
  }

  status(): PullStatus {
    return {
      state: this.state,
      intervalMs: this.interval,
      rigs: this.deps.rigs(),
      lastTickAt: this.lastTickAt,
      lastError: this.lastError,
      ticks: this.ticks,
      applied: this.applied,
      dropped: this.dropped,
    };
  }

  private schedule(ms: number): void {
    this.timer = this.setTimeoutFn(() => {
      void this.tick().finally(() => {
        if (this.state === "running") this.schedule(this.interval);
      });
    }, ms);
  }

  /** One tick: drain every rig, then set the next cadence. Never throws (a poller that dies on an
   *  unexpected error is a poller that silently stops delivering). */
  async tick(): Promise<void> {
    if (this.ticking) return; // never overlap
    this.ticking = true;
    const log = this.deps.log ?? (() => {});
    let transportFailed = false;
    try {
      this.ticks += 1;
      this.lastTickAt = (this.deps.now?.() ?? new Date()).toISOString();
      for (const rig of [...this.deps.rigs()].sort()) {
        const outcome = await this.drainRig(rig, log);
        transportFailed = transportFailed || outcome.transportFailed;
        if (!outcome.continueTicking) break;
      }
    } catch (e) {
      this.lastError = `tick-threw: ${(e as Error).message}`;
      log(`decisions pull tick failed unexpectedly: ${(e as Error).message}`);
    } finally {
      this.ticking = false;
      // §5: a transport failure doubles the cadence up to the ceiling; the first success resets it.
      this.interval = transportFailed ? Math.min(this.interval * 2, this.maxIntervalMs) : this.intervalMs;
    }
  }

  private async drainRig(rig: string, log: (m: string) => void): Promise<DrainOutcome> {
    let transportFailed = false;
    for (;;) {
      const after = this.cursors.get(rig) ?? null;
      const res = await this.deps.client.pullAnswers(rig, after);
      if (!res.ok) {
        this.lastError = res.error;
        transportFailed = isTransportFailure(res);
        if (res.status === 400) {
          // The cloud refused the cursor this daemon held: recover from the first page rather than
          // retry a value it will never accept. Rows already acked are not handed out again.
          this.cursors.delete(rig);
          log(`decisions pull rig=${rig} cursor rejected (${res.error}) — restarting from the first page`);
        } else {
          log(`decisions pull rig=${rig} failed (${res.error})${transportFailed ? " — backoff" : ""}`);
        }
        return { continueTicking: false, transportFailed };
      }
      const parsed = parseAnswersPage(res.body);
      if (!parsed.ok) {
        this.lastError = parsed.error;
        log(`decisions pull rig=${rig} page refused: ${parsed.error} — cursor left unmoved (an unparsed row is never skipped past)`);
        return { continueTicking: false, transportFailed: false };
      }
      for (const item of parsed.page.answers) {
        const outcome = await this.deps.apply(item);
        if (!outcome.settled) {
          log(`decisions pull rig=${rig} qitem=${item.qitem_id} not settled (${outcome.reason}) — cursor left unmoved, the row re-pulls`);
          return { continueTicking: false, transportFailed: false };
        }
        const acked = await this.ack(item, outcome.outcome, outcome.reason, log);
        if (!acked.ok) return { continueTicking: false, transportFailed: acked.transportFailed };
        if (outcome.outcome === "applied") this.applied += 1;
        else this.dropped += 1;
      }
      const next = parsed.page.cursor;
      if (next.length === 0) {
        // Drained. The next tick starts from the first page: the cloud hands out only un-acked
        // rows, so this costs one request and remembers nothing (§2).
        this.cursors.delete(rig);
        return { continueTicking: true, transportFailed };
      }
      this.cursors.set(rig, next);
    }
  }

  private async ack(
    item: PulledAnswer,
    outcome: "applied" | "dropped",
    reason: DropReason | undefined,
    log: (m: string) => void,
  ): Promise<{ ok: boolean; transportFailed: boolean }> {
    const res = await this.deps.client.ack(item.decision_id, {
      machine_id: this.deps.hostId(),
      outcome,
      ...(reason === undefined ? {} : { reason }),
    });
    if (res.ok) return { ok: true, transportFailed: false };
    if (ackAlreadySettled(res)) {
      // 409: the id is unknown or already acked. §1 route 3 — the state, not the id, is what a
      // caller can get wrong, and a retried ack must not read as a mistyped id. Settled.
      log(`decisions ack ${item.decision_id} already settled (409) — treated as delivered`);
      return { ok: true, transportFailed: false };
    }
    this.lastError = res.error;
    const transportFailed = isTransportFailure(res);
    log(`decisions ack ${item.decision_id} failed (${res.error}) — cursor left unmoved, the answer re-pulls`);
    return { ok: false, transportFailed };
  }
}
