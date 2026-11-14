import { passwordOk, sessionCookie, send, readBody } from '../lib/auth.js';

export default async function handler(req, res) {
  if (req.method !== 'POST') return send(res, 405, { error: 'POST only' });
  try {
    const { password } = await readBody(req);
    if (!passwordOk(password)) { await new Promise((r) => setTimeout(r, 500)); return send(res, 401, { error: 'Wrong password' }); }
    res.setHeader('set-cookie', sessionCookie());
    send(res, 200, { ok: true });
  } catch (e) { send(res, 500, { error: e.message }); }
}
