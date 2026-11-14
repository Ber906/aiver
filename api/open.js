import { isAuthed, send } from '../lib/auth.js';
import { ensureAgent } from '../lib/agent.js';

export default async function handler(req, res) {
  if (req.method !== 'POST') return send(res, 405, { error: 'POST only' });
  try {
    if (!isAuthed(req)) return send(res, 401, { error: 'Login required' });
    await ensureAgent(); // create or resume the sandbox and (re)start Power Agent inside it
    send(res, 200, { ok: true });
  } catch (e) { send(res, 500, { error: e.message }); }
}
