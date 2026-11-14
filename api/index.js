import { isAuthed } from '../lib/auth.js';

const PAGE = (authed) => `<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><meta name="color-scheme" content="dark light">
<title>Power Agent</title>
<style>
:root{--bg:#0a0e19;--elev:#131a2d;--text:#eef1ff;--mut:#7d86ab;--bd:rgba(150,165,255,.2);--acc:#5eead4;--grad:linear-gradient(135deg,#5eead4,#7aa2ff 52%,#b48cff);--on:#041a17}
@media(prefers-color-scheme:light){:root{--bg:#f4f7fc;--elev:#fff;--text:#0d1430;--mut:#6a7399;--bd:rgba(40,56,120,.2);--acc:#0f9d8a;--on:#fff}}
*{box-sizing:border-box}body{margin:0;min-height:100dvh;display:grid;place-items:center;padding:24px;background:radial-gradient(40rem 24rem at 85% -10%,rgba(139,123,255,.16),transparent 60%),radial-gradient(34rem 22rem at 0 110%,rgba(94,234,212,.1),transparent 60%),var(--bg);color:var(--text);font:16px/1.5 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif}
main{width:min(420px,100%);text-align:center;background:var(--elev);border:1px solid var(--bd);border-radius:26px;padding:32px 26px;display:flex;flex-direction:column;gap:14px;align-items:center}
.mark{width:60px;height:60px;border-radius:19px;background:var(--grad);color:var(--on);display:grid;place-items:center}
h1{margin:4px 0 0;font-size:28px;letter-spacing:-.02em}p{margin:0;color:var(--mut)}
input,button{font:inherit;width:100%;height:46px;border-radius:13px;border:1px solid var(--bd)}
input{background:transparent;color:inherit;padding:0 15px;outline:none}input:focus{border-color:var(--acc)}
button{border:0;background:var(--grad);color:var(--on);font-weight:650;cursor:pointer}
.err{color:#ff7b8e;min-height:1.2em;font-size:14px}.hidden{display:none!important}
pre{width:100%;max-height:180px;overflow:auto;margin:0;text-align:left;font:12px/1.5 ui-monospace,Menlo,Consolas,monospace;color:var(--mut);background:rgba(127,127,160,.1);border-radius:12px;padding:10px 12px;white-space:pre-wrap;word-break:break-word}
.spin{width:26px;height:26px;border-radius:50%;border:3px solid var(--bd);border-top-color:var(--acc);animation:r .9s linear infinite}@keyframes r{to{transform:rotate(360deg)}}
</style></head><body data-authed="${authed ? 1 : 0}"><main>
<div class="mark"><svg width="36" height="36" viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M13.1 5.4L8.3 12.6H11.5L10.7 18.6L15.7 11.2H12.4Z" fill="currentColor" stroke-width="1.3" stroke-linejoin="round"/><circle cx="12" cy="12" r="9.2" stroke-width="1.4" opacity=".5"/><circle cx="19.2" cy="6" r="1.5" fill="currentColor" stroke="none"/></svg></div>
<h1>Power Agent</h1>
<form id="login" class="hidden" style="width:100%;display:flex;flex-direction:column;gap:12px"><p>Enter your agent password</p>
<input id="pw" type="password" placeholder="Password" autocomplete="current-password" autofocus><button>Continue</button><div id="err" class="err"></div></form>
<section id="boot" class="hidden" style="width:100%;display:flex;flex-direction:column;gap:14px;align-items:center"><div class="spin"></div><p id="msg">Checking your sandbox...</p><pre id="log" class="hidden"></pre><p id="left" style="font-size:13px"></p></section>
</main><script>
var $=function(i){return document.getElementById(i)};
async function post(u,b){var r=await fetch(u,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(b||{})});var j=await r.json().catch(function(){return{}});if(!r.ok)throw new Error(j.error||('HTTP '+r.status));return j}
$('login').onsubmit=async function(e){e.preventDefault();$('err').textContent='';try{await post('/api/login',{password:$('pw').value});location.reload()}catch(x){$('err').textContent=x.message}};
var opening=false,openedAt=0,kicked=false;
async function tick(){
  var s;try{var r=await fetch('/api/status',{cache:'no-store'});if(r.status===401){location.reload();return}s=await r.json()}catch(e){s={phase:'error',message:e.message}}
  var say={idle:'Creating your sandbox...',stopped:'Waking your sandbox...',pending:'Starting the sandbox...',snapshotting:'Saving the sandbox...',stopping:'Stopping, one moment...',booting:'Power Agent is starting. The very first start takes a few minutes while it downloads and builds FreeLLMAPI.',ready:'Ready. Opening...',failed:'The sandbox failed to start. Retrying...',aborted:'The sandbox was aborted. Retrying...',error:s.message||'Something went wrong'}[s.phase]||s.phase;
  $('msg').textContent=say;var L=s.log||[];$('log').textContent=L.join('\\n');$('log').classList.toggle('hidden',!L.length);
  $('left').textContent=s.minutesLeft!=null?('Session time left: about '+s.minutesLeft+' min'):'';
  if(s.phase==='ready'){location.replace(s.url);return}
  if(s.phase==='booting'&&!kicked&&!opening){kicked=true;opening=true;openedAt=Date.now();post('/api/open').catch(function(){}).finally(function(){opening=false})}
  if(['idle','stopped','failed','aborted'].indexOf(s.phase)>-1&&!opening&&Date.now()-openedAt>30000){opening=true;openedAt=Date.now();post('/api/open').catch(function(x){$('msg').textContent=x.message}).finally(function(){opening=false})}
  setTimeout(tick,2500)}
if(document.body.dataset.authed==='1'){$('boot').classList.remove('hidden');tick()}else{$('login').classList.remove('hidden')}
</script></body></html>`;

export default function handler(req, res) {
  let authed = false;
  try { authed = isAuthed(req); } catch { /* AGENT_PASSWORD missing: show the form, login will explain */ }
  res.statusCode = 200;
  res.setHeader('content-type', 'text/html; charset=utf-8');
  res.setHeader('cache-control', 'no-store');
  res.end(PAGE(authed));
}
