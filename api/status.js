import { isAuthed, send } from '../lib/auth.js';
import { readStatus } from '../lib/agent.js';

export default async function handler(req, res) {
  try {
    if (!isAuthed(req)) return send(res, 401, { error: 'Login required' });
    send(res, 200, await readStatus());
  } catch (e) { send(res, 500, { error: e.message }); }
}
