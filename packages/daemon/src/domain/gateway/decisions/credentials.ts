// S2 (task 6fbd1f95) — the agenthub cloud target: ONE credential and ONE URL, resolved at call
// time from the environment or the shared env file. The addendum (§4) forbids a second config
// surface, so there is no JSON file here: the keys, the env file and the default URL are the ones
// the rest of the system already uses (ai_docs/operations/syncing-seats-with-the-cloud.md;
// scripts/team/4genthub-client/mission.md: "ONE credential: AGENTHUB_TOKEN with AGENTHUB_URL. ONE
// env file: ~/.config/4genthub/.env; a variable already in the environment wins").
//
// Secret posture is the Slack wire's, unchanged: the value is NEVER stored, NEVER logged, NEVER
// returned inside an error string; an unresolved token is reported "unconfigured", never faked.
// `resolveSecret` already implements the documented precedence (process env, incl. the
// OPENRIG_<NAME> alias, then the env file).

import os from "node:os";
import path from "node:path";
import { resolveSecret, checkEnvFilePermissions, type SecretFsOps, type SecretLookupOpts } from "../slack/secrets.js";

export const AGENTHUB_URL_NAME = "AGENTHUB_URL";
export const AGENTHUB_TOKEN_NAME = "AGENTHUB_TOKEN";

/** The addendum's default when AGENTHUB_URL is unset (§4): the production API. */
export const DEFAULT_AGENTHUB_URL = "https://api.4genthub.com";

/** The one env file the 4genthub client and the bridge already share. */
export function defaultAgenthubEnvFile(homedir: string = os.homedir()): string {
  return path.join(homedir, ".config", "4genthub", ".env");
}

export interface AgenthubTarget {
  /** The base URL asks are POSTed to; never null (the documented default fills it). */
  url: string;
  /** The bearer, or null when unresolved — callers report "unconfigured" and never fabricate. */
  token: string | null;
  /** The env file consulted, for the honest readiness line. */
  envFile: string;
  /** A hygiene warning (a group/world-readable env file), or null when clean or absent. */
  warning: string | null;
}

export interface AgenthubTargetOpts {
  envFile?: string;
  env?: NodeJS.ProcessEnv;
  fsops?: SecretFsOps;
}

/** Resolve the target. `envFile: null` skips the file entirely (environment only) — used by
 *  tests and by an operator who exports both variables, never by a path that needs a default. */
export function resolveAgenthubTarget(opts: AgenthubTargetOpts = {}): AgenthubTarget {
  const envFile = opts.envFile ?? defaultAgenthubEnvFile();
  const lookup: SecretLookupOpts = { envFile, ...(opts.env ? { env: opts.env } : {}), ...(opts.fsops ? { fsops: opts.fsops } : {}) };
  const url = resolveSecret(AGENTHUB_URL_NAME, lookup);
  return {
    url: url ?? DEFAULT_AGENTHUB_URL,
    token: resolveSecret(AGENTHUB_TOKEN_NAME, lookup),
    envFile,
    warning: checkEnvFilePermissions(envFile, opts.fsops),
  };
}
