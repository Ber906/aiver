// Runs Power Agent (app/ai.js) inside ONE named, persistent Vercel Sandbox.
//  - Sandbox filesystem = the "drive": workspace, chats, memory, settings and the FreeLLMAPI database all live under
//    /vercel/sandbox/home. Persistent sandboxes snapshot on stop and restore on resume.
//  - Session timeout is separate from persistence: Hobby caps one session at 45 min, Pro/Enterprise at 24 h.
import { Sandbox } from '@vercel/sandbox';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

const NAME = process.env.AGENT_SANDBOX_NAME || 'power-agent';
const PORT = 3001;                       // the one public port ai.js serves (FreeLLMAPI dashboard + /power-agent/)
const APP = '/vercel/sandbox';
const TIMEOUT_MS = Math.max(5, Number(process.env.SANDBOX_TIMEOUT_MIN) || 45) * 60_000;

const START_SH = `#!/bin/sh
cd ${APP}
command -v git >/dev/null 2>&1 || sudo dnf install -y git || true
command -v g++ >/dev/null 2>&1 || sudo dnf install -y gcc-c++ make python3 || true
exec node ai.js
`;

function agentEnv() {
  if (!process.env.AGENT_PASSWORD) throw new Error('AGENT_PASSWORD is not set on the Vercel project.');
  return {
    NODE_ENV: 'production',
    PORT: String(PORT),
    HOST: '0.0.0.0',
    DATA_DIR: `${APP}/home`,            // everything Power Agent stores lives here, inside the persistent sandbox
    AGENT_PASSWORD: process.env.AGENT_PASSWORD,
    AGENT_PORT: '18080',                // private ports, never exposed
    PREVIEW_PORT: '18081',
    ...(process.env.FREELLM_ENCRYPTION_KEY ? { FREELLM_ENCRYPTION_KEY: process.env.FREELLM_ENCRYPTION_KEY } : {}),
  };
}

// Idempotent: does nothing if the agent is already running in this session.
async function startAgent(sbx) {
  const up = await sbx.runCommand('sh', ['-c', 'pgrep -f "[n]ode ai.js" >/dev/null']);
  if (up.exitCode === 0) return;
  await sbx.writeFiles([ // always the latest ai.js, so redeploying the launcher updates the agent on its next start
    { path: `${APP}/ai.js`, content: readFileSync(join(process.cwd(), 'app', 'ai.js')) },
    { path: `${APP}/start.sh`, content: Buffer.from(START_SH), mode: 0o755 },
  ]);
  await sbx.runCommand({ cmd: 'sh', args: ['-c', './start.sh >> agent.log 2>&1'], cwd: APP, env: agentEnv(), detached: true });
}

export const ensureAgent = () => Sandbox.getOrCreate({
  name: NAME,
  runtime: 'node24',
  ports: [PORT],
  timeout: TIMEOUT_MS,
  resume: true,                          // resume right away and await onResume
  onCreate: startAgent,                  // first ever start (the heavy FreeLLMAPI clone/build happens inside ai.js)
  onResume: startAgent,                  // every later session
});

const clean = (s) => s.replace(/\x1b\[[0-9;]*m/g, '');
async function tail(sbx) {
  try {
    const buf = await sbx.readFileToBuffer({ path: `${APP}/agent.log` });
    if (!buf) return [];
    const pw = process.env.AGENT_PASSWORD || '\u0000';
    return clean(buf.subarray(-6000).toString('utf8')).split('\n')
      .filter((l) => l.trim() && !l.includes(pw)).slice(-12); // never leak the password that ai.js prints at boot
  } catch { return []; }
}
async function alive(url) {
  try { return (await fetch(url + '/health', { signal: AbortSignal.timeout(4000) })).ok; } catch { return false; }
}

// Read-only: never creates or resumes anything, so polling it costs nothing.
export async function readStatus() {
  let sbx;
  try { sbx = await Sandbox.get({ name: NAME, resume: false }); } catch { return { phase: 'idle' }; }
  if (sbx.status !== 'running') return { phase: sbx.status === 'stopped' ? 'stopped' : sbx.status };
  let url;
  try { url = sbx.domain(PORT); } catch (e) { return { phase: 'error', message: 'Port route missing: ' + e.message }; }
  const log = await tail(sbx);
  const minutesLeft = sbx.expiresAt ? Math.round((sbx.expiresAt.getTime() - Date.now()) / 60000) : null;
  if (await alive(url)) {
    if (minutesLeft !== null && minutesLeft < 10) await sbx.extendTimeout(10 * 60_000).catch(() => {}); // capped by your plan
    return { phase: 'ready', url: url + '/power-agent/', minutesLeft, log };
  }
  return { phase: 'booting', minutesLeft, log };
}
