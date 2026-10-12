// S2 (task 6fbd1f95) — APPLY one pulled answer set. This is the SAME continuation a Slack button
// click runs (slack/inbound.ts `attemptAction`), against the SAME deps trio the Slack wire binds —
// `recordHumanAnswer`, the reply-row creator, and `makeHumanReplyResolver` — so a cloud answer and
// a clicked answer cannot drift into two behaviours:
//
//   1. recordHumanAnswer writes `human_answers` and nothing else (the repository's own guard:
//      state pending + destination == actor);
//   2. when the set is COMPLETE, the connector creates the reply qitem and resolves the decision
//      through the mission-control resolve contract, whose parked-row refusal falls back to
//      `resolveDirectHumanReply` → `done` / `no-follow-on` / `ownerNotificationKind
//      human-decision-resolved` (the owner nudge, attempted once);
//   3. the connector NEVER resolves on its own reading of the answers.
//
// At-least-once is the puller's contract, so every step is idempotent by construction: an answer
// on a non-`pending` decision is `not-applicable` (therefore settled and never retried — addendum
// §2); a replayed complete set returns the same recorded answers; the reply row's id derives from
// the decision, so a replay finds the same row; and the resolver's own transition read turns a
// second resolve into `already-resolved`.

import { createHash } from "node:crypto";
import { formatHumanAnswers, type RecordHumanAnswerResult } from "../../human-questions.js";
import { isQitemPrimaryKeyConflict } from "../../queue-repository.js";
import { dropReasonFor, type DropReason, type PulledAnswer } from "./contract.js";

/** The reply-row creator the Slack inbound router also calls (queue-access `InboundQueuePort`). */
export interface ReplyRowPort {
  createQitem(input: {
    qitemId?: string;
    source: string;
    destination: string;
    summary: string;
    body: string;
    priority?: string;
    tags?: string[];
    inReplyTo?: string;
  }): Promise<string>;
}

export interface AnswerApplyDeps {
  recordHumanAnswer(input: { qitemId: string; actorSession: string; questionId: string; optionId: string }): RecordHumanAnswerResult;
  replyRows: ReplyRowPort;
  resolveHumanReply(input: { qitemId: string; actorSession: string; decision: string }): Promise<"resolved" | "already-resolved" | "not-applicable">;
  /** The local row this answer belongs to: the asking seat and the human the row was addressed to.
   *  `humanSession` null means that address is not a registered human — the one real check the
   *  daemon can make on a pulled answer (§6.9: the wire carries no answerer identity, so the
   *  addressed human is the actor, and a row addressed to nobody must be refused, never swallowed).
   *  A null RETURN means the qitem itself is gone — the local queue is the authority and the row is
   *  terminal, so there is nothing to apply. */
  describeDecision(qitemId: string): { askerSession: string; humanSession: string | null } | null;
  log?: (msg: string) => void;
}

export type ApplyResult =
  /** The cloud's row is settled for this daemon: ack it with `outcome` (+ `reason` when dropped). */
  | { settled: true; outcome: "applied" | "dropped"; reason?: DropReason; complete: boolean }
  /** Nothing may be acked: the next tick retries this row. */
  | { settled: false; reason: string };

/** The reply row's id derives from the decision, so a redelivered answer or a retry finds the same
 *  row instead of minting a second one (the Slack path's `qitem-slack-answers-<digest>`). */
export function replyRowIdFor(qitemId: string): string {
  return `qitem-agenthub-answers-${createHash("sha256").update(qitemId).digest("hex").slice(0, 20)}`;
}

export async function applyPulledAnswer(deps: AnswerApplyDeps, item: PulledAnswer): Promise<ApplyResult> {
  const log = deps.log ?? (() => {});
  const picks = Object.entries(item.answers);

  const described = deps.describeDecision(item.qitem_id);
  if (described === null) {
    log(`answer for an unknown local row qitem=${item.qitem_id} -> ack dropped (state-done)`);
    return { settled: true, outcome: "dropped", reason: "state-done", complete: false };
  }
  if (described.humanSession === null) {
    log(`answer addressed to nobody qitem=${item.qitem_id} (destination does not resolve to a registered human) -> ack dropped (not-the-asked-human)`);
    return { settled: true, outcome: "dropped", reason: "not-the-asked-human", complete: false };
  }
  const actorSession = described.humanSession;

  let last: RecordHumanAnswerResult | null = null;
  for (const [questionId, optionId] of picks) {
    let recorded: RecordHumanAnswerResult;
    try {
      recorded = deps.recordHumanAnswer({ qitemId: item.qitem_id, actorSession, questionId, optionId });
    } catch (e) {
      // A storage failure is the one class a retry CAN change: leave the cursor alone.
      return { settled: false, reason: `record-failed: ${(e as Error).message}` };
    }
    if (recorded.status !== "recorded") {
      // The repository's own guard refused it: a forged id, a stranger, or a decision already
      // resolved. Never an error the socket must retry — settled with the wire's own token, and
      // said out loud, because a refusal nobody reports is a resolve that silently never happens.
      const reason = dropReasonFor(recorded.reason);
      log(`answer refused qitem=${item.qitem_id} local=${recorded.reason} -> ack dropped (${reason})`);
      return { settled: true, outcome: "dropped", reason, complete: false };
    }
    last = recorded;
  }
  if (last === null) return { settled: true, outcome: "dropped", reason: "unknown-qitem", complete: false };

  if (!last.complete) {
    log(`answer recorded qitem=${item.qitem_id} — waiting for the rest of the set`);
    return { settled: true, outcome: "applied", complete: false };
  }

  const lines = formatHumanAnswers(last.questions, last.answers);
  try {
    await deps.replyRows.createQitem({
      qitemId: replyRowIdFor(item.qitem_id),
      source: actorSession,
      destination: described.askerSession,
      priority: "routine",
      tags: ["agenthub", "inbound", "human-answer"],
      summary: `Founder via the cloud: answered ${lines.length === 1 ? "1 question" : `${lines.length} questions`}`,
      body: `${lines.join("\n")}\n\n---\nAnswers (question id → option id): ${JSON.stringify(last.answers)}\nIn reply to: ${item.qitem_id} (its humanAnswers field holds the same)\nRouted by openrig decisions-inbound (cloud pull).`,
      inReplyTo: item.qitem_id,
    });
  } catch (e) {
    // The id is derived, so a conflicting row IS this row: the continuation proceeds.
    if (!isQitemPrimaryKeyConflict(e)) {
      log(`answer continuation failed qitem=${item.qitem_id}: ${(e as Error).message}`);
      return { settled: false, reason: `reply-row-failed: ${(e as Error).message}` };
    }
  }

  let resolution: "resolved" | "already-resolved" | "not-applicable";
  try {
    resolution = await deps.resolveHumanReply({ qitemId: item.qitem_id, actorSession, decision: lines.join("; ") });
  } catch (e) {
    log(`answer resolve failed qitem=${item.qitem_id}: ${(e as Error).message}`);
    return { settled: false, reason: `resolve-failed: ${(e as Error).message}` };
  }
  if (resolution === "not-applicable") {
    // The reply row reached the seat, the decision did not close, and a retry cannot change that.
    // Settle it and say so: reporting success here would be the lie the gate looks for.
    log(`answers complete but resolve not applicable qitem=${item.qitem_id} -> ack dropped (already-applied)`);
    return { settled: true, outcome: "dropped", reason: "already-applied", complete: true };
  }
  log(`answers complete qitem=${item.qitem_id} -> ${resolution} -> ack applied`);
  return { settled: true, outcome: "applied", complete: true };
}
