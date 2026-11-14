import { isAuthed, send } from '../lib/auth.js';
import { readLog } from '../lib/agent.js';

// Open https://YOUR-LAUNCHER.vercel.app/api/logs in the browser after logging in to the launcher.
export default async function handler(req, res) {
  try {
    if (!isAuthed(req)) return send(res, 401, { error: 'Login required. Open the launcher page and enter the agent password first.' });
    const text = await readLog();
    res.statusCode = 200;
    res.setHeader('content-type', 'text/plain; charset=utf-8');
    res.setHeader('cache-control', 'no-store');
    res.end(text);
  } catch (e) { send(res, 500, { error: e.message }); }
}
