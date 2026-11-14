# Power Agent on Vercel (runs inside a Vercel Sandbox)

`ai.js` is a long-running Node server (child processes, background runs, SSE), so it cannot run in a Vercel
Function. This project is a tiny launcher: it creates **one named, persistent Vercel Sandbox**, starts `ai.js`
inside it, and redirects you to the sandbox's public URL.

* Sandbox filesystem = your drive. Workspace, chats, memory, settings and the FreeLLMAPI database live in
  `/vercel/sandbox/home`. A persistent sandbox is snapshotted when it stops and restored when it resumes.
* Open the launcher URL any time: it resumes the sandbox, restarts the agent, and sends you to `/power-agent/`.

## Deploy

1. `npm install`
2. `vercel link` (Sandbox must be available on the project/team)
3. Set Production environment variables:
   | Name | Notes |
   |---|---|
   | `AGENT_PASSWORD` | **required**. Protects both the launcher and Power Agent. Make it long. |
   | `SANDBOX_TIMEOUT_MIN` | session length in minutes. Default `45` (Hobby max). Pro/Enterprise can use up to `1440`. |
   | `FREELLM_ENCRYPTION_KEY` | optional. Otherwise one is generated once and kept in the sandbox. |
   | `AGENT_SANDBOX_NAME` | optional, default `power-agent`. |
4. `vercel deploy --prod`
5. Open the URL, enter the password, wait. The very first start takes a few minutes (it clones and builds FreeLLMAPI).
6. First time only: open the sandbox URL root (FreeLLMAPI dashboard), add provider keys, copy the unified key into
   Power Agent -> Settings.

## Know the limits

* One session lasts up to 45 min on Hobby, 24 h on Pro/Enterprise. When it ends, the agent process stops (an answer
  in progress stops with it). Files and chats come back from the snapshot when you reopen the launcher URL.
  Snapshots expire 30 days after last use by default.
* The sandbox URL is public and `ai.js` puts no login in front of the FreeLLMAPI dashboard at `/`. Do not share that URL.
* To update the agent: replace `app/ai.js`, redeploy, then restart the sandbox session (it re-uploads on start).
* To stop paying for a running sandbox: Vercel dashboard -> Observability -> Sandboxes -> Stop.
