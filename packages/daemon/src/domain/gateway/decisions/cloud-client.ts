// S2 (task 6fbd1f95) — the daemon's FIRST cloud call. The Slack connector is the only other
// outbound credentialed HTTP in the daemon (`slack/slack-api.ts`), so this client copies its
// discipline rather than inventing one: ONE bounded attempt per call, a Bearer header, an
// explicit timeout, and an error string that never carries the credential.
//
// NO IN-CALL RETRY, deliberately. Everything here is driven by a poll or by the durable dispatch
// buffer: an ask that fails stays in the buffer (the dispatcher retains it; replay owns the
// redrive), a pull that fails leaves the cursor where it was, and an ack that fails is re-sent by
// the next pull because the row is still un-acked in the cloud. A retry loop would add a second,
// weaker copy of that durability (addendum §2: the ack, not the cursor, is the delivery record).
//
// The parse layer is separate from the transport so a page shape can be pinned without a socket.

import type { AckBody, AnswersPage, AskBody, StatePushBody, PulledAnswer } from "./contract.js";
import { ACK_OUTCOMES, DECISIONS_ANSWERS_PATH, DECISIONS_ASK_PATH, decisionsAckPath, decisionsStatePath } from "./contract.js";
import type { FetchImpl } from "../slack/slack-api.js";

export type CloudCall =
  | { ok: true; status: number; body: unknown }
  | { ok: false; status?: number; error: string };

export interface CloudClientOpts {
  baseUrl: string;
  /** The credential from `resolveAgenthubTarget`. NEVER logged, NEVER stored, never echoed. */
  token: string;
  fetchImpl?: FetchImpl;
  timeoutMs?: number;
}

export interface CloudClient {
  postAsk(body: AskBody): Promise<CloudCall>;
  pullAnswers(rig: string, after: string | null): Promise<CloudCall>;
  ack(decisionId: string, body: AckBody): Promise<CloudCall>;
  pushState(decisionId: string, body: StatePushBody): Promise<CloudCall>;
}

/**
 * §1 route 3: an id that is unknown OR already acked answers 409, and the daemon treats that as
 * success — the state, not the id, is what a caller can get wrong, so a retried ack must not read
 * as a mistyped id. Named here so the driver's "settled" decision is one call, not a status test
 * repeated at each site.
 */
export function ackAlreadySettled(res: CloudCall): boolean {
  return !res.ok && res.status === 409;
}

/** A row the reader cannot use, on a page whose ENVELOPE is intact. The driver logs it once, skips
 *  it and never acks it: the cloud's row is the cloud's to judge, and a row-level defect must not
 *  stop the drain (§2's no-stall clause — a guard that can livelock the stream is worse than the
 *  anomaly it guards). */
export interface UnreadableAnswerRow {
  index: number;
  /** The row's own id when it carries one, so the log can name the decision; null when it does not. */
  decisionId: string | null;
  reason: string;
}

export type AnswersPageParse =
  | { ok: true; page: AnswersPage; unreadable: UnreadableAnswerRow[] }
  | { ok: false; error: string };

/** §1 route 2's page, two levels of defect and only one has a cursor:
 *
 *   - a ROW the reader cannot use (an empty or unparseable answer set, a missing field, a
 *     non-object row) is reported as unreadable and the PAGE stays valid, so the driver can log it
 *     once, skip it without acking, and use the page's own cursor;
 *   - a broken ENVELOPE (`success` not true, `answers` not an array, `cursor` not a string) has no
 *     position to advance to, so the whole page is refused, the cursor stays put and the next tick
 *     re-requests it — bounded by the cadence, recorded in `lastError`, self-healing for a
 *     truncated body. */
export function parseAnswersPage(body: unknown): AnswersPageParse {
  if (typeof body !== "object" || body === null) return { ok: false, error: "answers body is not an object" };
  const obj = body as Record<string, unknown>;
  if (obj.success !== true) return { ok: false, error: 'answers body has no "success": true' };
  const rows = obj.answers;
  if (!Array.isArray(rows)) return { ok: false, error: 'answers body has no "answers" array' };
  const out: PulledAnswer[] = [];
  const unreadable: UnreadableAnswerRow[] = [];
  const idOf = (row: Record<string, unknown>): string | null =>
    typeof row.decision_id === "string" && row.decision_id.length > 0 ? row.decision_id : null;
  const skip = (index: number, row: Record<string, unknown> | null, reason: string): void => {
    unreadable.push({ index, decisionId: row === null ? null : idOf(row), reason });
  };
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    if (typeof r !== "object" || r === null) {
      skip(i, null, "row is not an object");
      continue;
    }
    const row = r as Record<string, unknown>;
    let missing: string | null = null;
    for (const key of ["decision_id", "qitem_id", "author_seat_key", "room", "answered_at"] as const) {
      const v = row[key];
      if (typeof v !== "string" || v.length === 0) { missing = key; break; }
    }
    if (missing !== null) {
      skip(i, row, `${missing} must be a non-empty string`);
      continue;
    }
    const raw = row.answers;
    if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
      skip(i, row, "answers must be an object");
      continue;
    }
    const picks: Record<string, string> = {};
    let badPick: string | null = null;
    for (const [questionId, optionId] of Object.entries(raw as Record<string, unknown>)) {
      if (typeof optionId !== "string" || optionId.length === 0) {
        badPick = `answers["${questionId}"] must be a non-empty option id`;
        break;
      }
      picks[questionId] = optionId;
    }
    if (badPick !== null) {
      skip(i, row, badPick);
      continue;
    }
    if (Object.keys(picks).length === 0) {
      skip(i, row, "carries no answers (a row in state 'answered' must carry at least one)");
      continue;
    }
    out.push({
      decision_id: row.decision_id as string,
      qitem_id: row.qitem_id as string,
      author_seat_key: row.author_seat_key as string,
      room: row.room as string,
      answers: picks,
      answered_at: row.answered_at as string,
    });
  }
  const cursor = obj.cursor;
  if (typeof cursor !== "string") return { ok: false, error: 'answers body\'s "cursor" must be a string (empty on the last page)' };
  return { ok: true, page: { answers: out, cursor }, unreadable };
}

export function makeCloudClient(opts: CloudClientOpts): CloudClient {
  const base = opts.baseUrl.replace(/\/+$/, "");
  const doFetch: FetchImpl = opts.fetchImpl ?? ((url, init) => fetch(url, init));
  const timeoutMs = opts.timeoutMs ?? 10000;

  const call = async (method: "GET" | "POST", path: string, body?: unknown): Promise<CloudCall> => {
    try {
      const response = await doFetch(`${base}${path}`, {
        method,
        headers: {
          authorization: `Bearer ${opts.token}`,
          accept: "application/json",
          ...(body === undefined ? {} : { "content-type": "application/json" }),
        },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
        signal: AbortSignal.timeout(timeoutMs),
      });
      const text = await response.text();
      let parsed: unknown = null;
      if (text.length > 0) {
        try { parsed = JSON.parse(text); } catch { parsed = text; }
      }
      if (!response.ok) {
        // The cloud's own words plus the status, the fact. Neither carries the token.
        const detail = typeof parsed === "object" && parsed !== null && typeof (parsed as Record<string, unknown>).detail === "string"
          ? (parsed as Record<string, string>).detail
          : typeof parsed === "string" && parsed.length > 0 ? parsed.slice(0, 200)
            : typeof parsed === "object" && parsed !== null && typeof (parsed as Record<string, unknown>).error === "string"
              ? (parsed as Record<string, string>).error
              : "(no body)";
        return { ok: false, status: response.status, error: `HTTP ${response.status}: ${detail}` };
      }
      return { ok: true, status: response.status, body: parsed };
    } catch (e) {
      const err = e as Error;
      const kind = err.name === "TimeoutError" || err.name === "AbortError" ? "timeout" : "network";
      return { ok: false, error: `${kind}: ${err.message}` };
    }
  };

  return {
    postAsk: (body) => call("POST", DECISIONS_ASK_PATH, body),
    // §1 route 2's exact query vocabulary: `after` (empty = first page), `rig` scoping the pull to
    // this daemon's rig. `limit` is deliberately NOT sent: the service clamps it
    // (ClampDecisionAnswerLimit is the one home for those numbers) and the tick drains pages until
    // the cursor is empty, so a client-chosen page size would only be a second, weaker copy.
    pullAnswers: (rig, after) => {
      const query = new URLSearchParams(after === null ? { rig } : { rig, after });
      return call("GET", `${DECISIONS_ANSWERS_PATH}?${query.toString()}`);
    },
    ack: (decisionId, body) => call("POST", decisionsAckPath(decisionId), body),
    pushState: (decisionId, body) => call("POST", decisionsStatePath(decisionId), body),
  };
}

/** Narrow, referenced here so the union is exhaustively readable at the call-site. */
export type AckOutcomeName = (typeof ACK_OUTCOMES)[number];
