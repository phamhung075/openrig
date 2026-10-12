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

/** §1 route 2's page. A body that is not the frozen shape is an honest failure, never a guess:
 *  the driver then leaves the cursor alone and tries again on the next tick. An answer set that is
 *  EMPTY is a failure too — a `state='answered'` row by construction carries at least one pick,
 *  and skipping past it would be the silent loss at-least-once forbids. */
export function parseAnswersPage(body: unknown): { ok: true; page: AnswersPage } | { ok: false; error: string } {
  if (typeof body !== "object" || body === null) return { ok: false, error: "answers body is not an object" };
  const obj = body as Record<string, unknown>;
  if (obj.success !== true) return { ok: false, error: 'answers body has no "success": true' };
  const rows = obj.answers;
  if (!Array.isArray(rows)) return { ok: false, error: 'answers body has no "answers" array' };
  const out: PulledAnswer[] = [];
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    if (typeof r !== "object" || r === null) return { ok: false, error: `answers[${i}] is not an object` };
    const row = r as Record<string, unknown>;
    for (const key of ["decision_id", "qitem_id", "author_seat_key", "room", "answered_at"] as const) {
      const v = row[key];
      if (typeof v !== "string" || v.length === 0) return { ok: false, error: `answers[${i}].${key} must be a non-empty string` };
    }
    const raw = row.answers;
    if (typeof raw !== "object" || raw === null || Array.isArray(raw)) return { ok: false, error: `answers[${i}].answers must be an object` };
    const picks: Record<string, string> = {};
    for (const [questionId, optionId] of Object.entries(raw as Record<string, unknown>)) {
      if (typeof optionId !== "string" || optionId.length === 0) return { ok: false, error: `answers[${i}].answers["${questionId}"] must be a non-empty option id` };
      picks[questionId] = optionId;
    }
    if (Object.keys(picks).length === 0) return { ok: false, error: `answers[${i}] carries no answers (a row in state 'answered' must carry at least one)` };
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
  return { ok: true, page: { answers: out, cursor } };
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
