// The launcher is protected by the same AGENT_PASSWORD as Power Agent, so strangers cannot
// start (and bill) your sandbox or read its boot log.
import { createHmac, timingSafeEqual } from 'node:crypto';

const secret = () => {
  const s = process.env.AGENT_PASSWORD;
  if (!s) throw new Error('Set AGENT_PASSWORD in the Vercel project environment variables.');
  return s;
};
const sign = (v) => createHmac('sha256', secret()).update(v).digest();
const same = (a, b) => a.length === b.length && timingSafeEqual(a, b);

export const passwordOk = (pw) => same(sign('pw:' + String(pw || '')), sign('pw:' + secret()));
export const sessionCookie = () =>
  `pl=${sign('session').toString('hex')}; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=2592000`;
export const isAuthed = (req) => {
  const m = /(?:^|;\s*)pl=([a-f0-9]+)/.exec(req.headers.cookie || '');
  if (!m) return false;
  try { return same(Buffer.from(m[1], 'hex'), sign('session')); } catch { return false; }
};

export const send = (res, code, obj) => {
  res.statusCode = code;
  res.setHeader('content-type', 'application/json');
  res.setHeader('cache-control', 'no-store');
  res.end(JSON.stringify(obj));
};
export async function readBody(req) {
  if (req.body && typeof req.body === 'object') return req.body;
  let raw = typeof req.body === 'string' ? req.body : '';
  if (!raw) { const chunks = []; for await (const c of req) chunks.push(c); raw = Buffer.concat(chunks).toString('utf8'); }
  try { return raw ? JSON.parse(raw) : {}; } catch { return {}; }
}
