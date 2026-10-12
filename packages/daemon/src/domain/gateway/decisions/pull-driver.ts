// S2 (task 6fbd1f95) — the PULL driver: the answer comes down a client-initiated pull, so this is
// the only network loop the decisions half owns. Behaviour is the addendum's §5, verbatim:
//
//   - 30 000 ms base cadence (the shipped Slack outbound default), one IMMEDIATE first tick;
//   - each tick drains pages until the cursor is empty, so a burst costs one tick, not one page
//     per cadence;
//   - a transport failure doubles the interval up to 300 000 ms and the first success resets it;
//   - ticks never overlap.
//
// The ACK, not the cursor, is the delivery record (§2), and the ack has four ruled classes: a 2xx
// is delivered; a 409 is already delivered; a transport-class failure (no answer, or a 5xx) leaves
// the cursor where it was, because the retry IS the fix; and a PERMANENT contract-class refusal (any
// other 4xx — a malformed body, an outcome or dropped reason outside the closed list, a bad bearer,
// an unmounted route) is settled for that drain only: logged once, left UN-ACKED, and skipped, since
// no cadence changes a refused request. It is stated as the COMPLEMENT of the transport test rather
// than as a status range, so nothing falls through it: a 3xx (a redirect to a login page — a config
// fault, not a retryable one) is refused-and-skipped too, not made to stop the rig. Without that
// last class one refused row would freeze every
// later answer for its rig — the same stall the no-stall clause forbids for an unreadable row — and
// nothing is lost by skipping it, because the answer was applied BEFORE the ack (§3) and a re-apply
// is idempotent. What still holds the cursor: an apply that needs a retry, a refused page, or a
// transport-class ack failure. Nothing is remembered between ticks about what was pulled: a restart
// re-pulls from the first page.
//
// Two levels of page defect, and only one of them has a cursor (§2's no-stall clause). A ROW the
// reader cannot use (an empty or unparseable answer set, a missing field) is logged once, skipped
// WITHOUT being acked, and the drain continues to the page's own cursor — the cloud still counts
// that row un-acked, so it stays the cloud's to judge. A broken ENVELOPE has no position to advance
// to, so the drain stops with the cursor unmoved and retries next tick.

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

/** What became of ONE ack attempt (§1 route 3, §2's four classes). Only `failed` may stop the drain:
 *  a transport failure is what a retry can fix, while a refused request is what no retry can, so the
 *  refusal is settled for this drain and the row stays UN-ACKED in the cloud. `refused` carries the
 *  status for the log-once key (`rig|decisionId|status`). */
type AckResult =
  | { kind: "acked" }
  | { kind: "already-settled" }
  | { kind: "refused"; status: number | undefined }
  | { kind: "failed" };

export class DecisionsPullDriver {
  private readonly intervalMs: number;
  private readonly maxIntervalMs: number;
  private readonly setTimeoutFn: (fn: () => void, ms: number) => unknown;
  private readonly clearTimeoutFn: (handle: unknown) => void;
  private readonly cursors = new Map<string, string>();
  /** Anomalies already reported, keyed rig|identity|reason (§2 wants the log ONCE, and the cloud
   *  re-serves an un-acked row on every drain, so a repeating message would be a per-cadence
   *  drumbeat for one anomaly). `liveThisDrain` is the other half of the bound: a drain that reaches
   *  the end of the cloud's page has seen that rig's whole un-acked set, so a key of that rig which
   *  the drain did NOT meet is gone from the cloud and is dropped — a return is a new episode and
   *  earns its own single line. The set therefore tracks the anomalies that are LIVE, never the
   *  history of every row ever served. */
  private readonly loggedOnce = new Set<string>();
  private liveThisDrain = new Set<string>();
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

  /** Report one anomaly at most once per episode, and mark it live for this drain. */
  private reportOnce(key: string, message: () => string, log: (m: string) => void): void {
    this.liveThisDrain.add(key);
    if (this.loggedOnce.has(key)) return;
    this.loggedOnce.add(key);
    log(message());
  }

  /** Drop this rig's keys that the drain did not meet: the cloud served its whole un-acked set for
   *  that rig, so a key it did not serve is no longer live (see `loggedOnce`). */
  private forgetUnseen(rigPrefix: string): void {
    for (const key of this.loggedOnce) {
      if (key.startsWith(rigPrefix) && !this.liveThisDrain.has(key)) this.loggedOnce.delete(key);
    }
  }

  private async drainRig(rig: string, log: (m: string) => void): Promise<DrainOutcome> {
    let transportFailed = false;
    this.liveThisDrain = new Set();
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
        log(`decisions pull rig=${rig} page refused: ${parsed.error} — cursor left unmoved (a broken envelope has no position to advance to)`);
        return { continueTicking: false, transportFailed: false };
      }
      // Row-level defects are skipped FORWARD, never acked: the row keeps its place in the cloud's
      // un-acked set, so it comes back, but it can no longer hold every later answer behind it.
      for (const row of parsed.unreadable) {
        this.reportOnce(
          `${rig}|${row.identity}|${row.reason}`,
          () =>
            `decisions pull rig=${rig} answers[${row.index}]${row.decisionId === null ? ` identity=${row.identity}` : ` decision=${row.decisionId}`} unreadable (${row.reason}) — skipped and NOT acked; the cloud still owns that row`,
          log,
        );
      }
      for (const item of parsed.page.answers) {
        const outcome = await this.deps.apply(item);
        if (!outcome.settled) {
          log(`decisions pull rig=${rig} qitem=${item.qitem_id} not settled (${outcome.reason}) — cursor left unmoved, the row re-pulls`);
          return { continueTicking: false, transportFailed: false };
        }
        const acked = await this.ack(rig, item, outcome.outcome, outcome.reason, log);
        // §2: only a TRANSPORT-class ack failure holds the cursor. A refused ack is settled for
        // this drain, so the loop continues and the page end is still reached.
        if (acked.kind === "failed") return { continueTicking: false, transportFailed: true };
        if (outcome.outcome === "applied") this.applied += 1;
        else this.dropped += 1;
      }
      const next = parsed.page.cursor;
      if (next.length === 0) {
        // Drained. The next tick starts from the first page: the cloud hands out only un-acked
        // rows, so this costs one request and remembers nothing (§2) — and that is what brings a
        // row whose ack was refused back, one drain later, with no restart.
        this.cursors.delete(rig);
        this.forgetUnseen(`${rig}|`);
        return { continueTicking: true, transportFailed };
      }
      this.cursors.set(rig, next);
    }
  }

  /** §1 route 3's four ruled classes (§2). `acked` and `already-settled` are deliveries; `failed` is
   *  transport-class and holds the cursor because a retry IS the fix; `refused` is a permanent
   *  contract-class 4xx — logged once, left UN-ACKED, and skipped. */
  private async ack(
    rig: string,
    item: PulledAnswer,
    outcome: "applied" | "dropped",
    reason: DropReason | undefined,
    log: (m: string) => void,
  ): Promise<AckResult> {
    const res = await this.deps.client.ack(item.decision_id, {
      machine_id: this.deps.hostId(),
      outcome,
      ...(reason === undefined ? {} : { reason }),
    });
    if (res.ok) return { kind: "acked" };
    if (ackAlreadySettled(res)) {
      // 409: the id is unknown or already acked. §1 route 3 — the state, not the id, is what a
      // caller can get wrong, and a retried ack must not read as a mistyped id. Settled.
      log(`decisions ack ${item.decision_id} already settled (409) — treated as delivered`);
      return { kind: "already-settled" };
    }
    if (isTransportFailure(res)) {
      this.lastError = res.error;
      log(`decisions ack ${item.decision_id} failed (${res.error}) — cursor left unmoved, the answer re-pulls`);
      return { kind: "failed" };
    }
    // §2 class 4. No cadence fixes a refused request (a malformed body, an outcome or dropped reason
    // outside the closed list, a bad bearer, an unmounted route), so this attempt is settled for
    // THIS drain and the row stays un-acked in the cloud — which is exactly what lets a repaired
    // bearer or a mounted route heal it with no restart. Reported once, and NOT as the driver's
    // error state: the drain itself did its job, and marking a delivery the cloud never got would
    // stop it from handing the row out again at all.
    this.reportOnce(
      `${rig}|${item.decision_id}|${res.status}`,
      () =>
        `decisions ack ${item.decision_id} refused (${res.error}) — not retried, NOT acked; the row stays un-acked in the cloud and the drain continues`,
      log,
    );
    return { kind: "refused", status: res.status };
  }
}
