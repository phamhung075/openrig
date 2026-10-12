// S2 (task 6fbd1f95) — the CLOUD DECISIONS WIRE, daemon half. The ask and the state push go UP
// through the shipped gateway dispatcher (durable-first, `decisionId`-idempotent); the answer
// comes DOWN a client-initiated pull with an opaque cursor and an explicit ack. Nothing dials
// the daemon.
//
// FROZEN: ARCH-ADDENDUM-s2-wire-freeze-2026-10-12.md §1-§5 (architect, task c817082a), itself
// bound by ARCH-DESIGN-room-decisions-cloud-lane-1-2026-10-12.md. THIS FILE IS THE ONE PLACE the
// wire's names live, so a later addendum changes this file and no driver. Every name below was
// read in that addendum this sitting; where this file paraphrases a rule, the addendum is the
// authority.
//
// Routes (§1), all `authed`, machine side rig-scoped:
//   1 POST /api/v2/openrig/decisions              ingest one mirrored ask (op decision.ask)
//   2 GET  /api/v2/openrig/decisions/answers      pull un-acked answers (the poller's own call)
//   3 POST /api/v2/openrig/decisions/{id}/ack     record an answer's delivery outcome
//   4 POST /api/v2/openrig/decisions/{id}/state   push a state learned without an answer
//                                                 (op decision.state)
//
// Vocabulary cloned from the shipped seat_messages pull (§6.3): query param `after`, response
// field `cursor`, the `{"success":true,…}` envelope — one client parser would serve both.

import type { HumanQuestion } from "../../human-questions.js";

/** The human-connector kind a binding names to opt into this wire (§4). */
export const DECISIONS_CONNECTOR_KIND = "agenthub";

/** The ADVERTISED ops (§4). proof-9 holds in-process: an unadvertised op is REFUSED, not attempted. */
export const ASK_OP = "decision.ask";
export const STATE_OP = "decision.state";

export const DECISIONS_ASK_PATH = "/api/v2/openrig/decisions";
export const DECISIONS_ANSWERS_PATH = "/api/v2/openrig/decisions/answers";

export function decisionsAckPath(decisionId: string): string {
  return `/api/v2/openrig/decisions/${encodeURIComponent(decisionId)}/ack`;
}

export function decisionsStatePath(decisionId: string): string {
  return `/api/v2/openrig/decisions/${encodeURIComponent(decisionId)}/state`;
}

/** Episode-stable end-to-end identity (§4): the same key twice returns the first row with
 *  created:false, so a redriven ask collapses into ONE cloud row and never duplicates. */
export function decisionIdFor(qitemId: string): string {
  return `${DECISIONS_CONNECTOR_KIND}:decision:${qitemId}`;
}

/** §1 route 1 body. `room` is optional and WINS when present; absent, the cloud resolves the
 *  row's room from `rig` (a rig's name is its room's slug by convention, §6.2). */
export interface AskBody {
  rig: string;
  room?: string;
  qitem_id: string;
  author_seat_key: string;
  destination: string;
  summary: string;
  detail: string | null;
  questions: HumanQuestion[];
  idempotency_key: string;
}

/** §1 route 4 body. `resolved`/`superseded` are the daemon's transitions (only the daemon may
 *  produce every transition out of `answered`, §3). The machine identity field is literally
 *  `machine_id` and carries the daemon's hostId value (§4). */
export interface StatePushBody {
  machine_id: string;
  state: "resolved" | "superseded";
  reason?: string;
}

/** §1 route 3 body. `reason` is required when `outcome` is `dropped`; unknown JSON fields are
 *  refused by the cloud, so this type carries nothing else. */
export interface AckBody {
  machine_id: string;
  outcome: "applied" | "dropped";
  reason?: string;
}

/** The closed drop vocabulary (§1 route 3). Every one is acked once and NEVER retried. */
export const DROP_REASONS = ["state-done", "not-the-asked-human", "unknown-qitem", "already-applied", "superseded"] as const;
export type DropReason = (typeof DROP_REASONS)[number];

/** One decision whose recorded answers are not yet applied, as §1 route 2 returns it. */
export interface PulledAnswer {
  decision_id: string;
  qitem_id: string;
  author_seat_key: string;
  room: string;
  /** `questionId -> optionId`; a typed reply is not an option and never rides this wire. */
  answers: Record<string, string>;
  answered_at: string;
}

/** One page. `cursor` is OPAQUE (§2): echoed back verbatim, never parsed, never advanced on a
 *  failed apply — the ack, not the cursor, is the delivery record. Empty cursor = last page. */
export interface AnswersPage {
  answers: PulledAnswer[];
  cursor: string;
}

export const ACK_OUTCOMES = ["applied", "dropped"] as const;
export type AckOutcome = (typeof ACK_OUTCOMES)[number];

/**
 * Map a LOCAL refusal onto the cloud's closed drop vocabulary (§1 route 3). The local reasons
 * come verbatim from `recordHumanAnswer` / the apply step; the cloud accepts only its five
 * tokens, so the translation lives here rather than at each call-site. A local refusal with no
 * clean token lands on `unknown-qitem`: the cloud sent an identity this daemon cannot act on,
 * which is what that token means on the wire, and the local reason rides the log line instead.
 */
export function dropReasonFor(localReason: string): DropReason {
  if (localReason.startsWith("state-")) return "state-done";
  if (localReason === "not-the-asked-human") return "not-the-asked-human";
  if (localReason === "resolve-not-applicable") return "already-applied";
  if (localReason === "already-applied") return "already-applied";
  return "unknown-qitem";
}
