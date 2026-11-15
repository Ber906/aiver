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
@property --ang{syntax:'<angle>';inherits:false;initial-value:0deg}
@keyframes ang{to{--ang:360deg}}@keyframes shimmer{to{background-position:-200% 0}}
@keyframes swapin{from{opacity:0;filter:blur(6px);transform:translateY(5px)}to{opacity:1;filter:none;transform:none}}
@keyframes sheen{from{transform:translateX(-100%)}to{transform:translateX(260%)}}
main{position:relative}
main.busy::before{content:'';position:absolute;inset:-1px;border-radius:inherit;padding:1.5px;pointer-events:none;background:conic-gradient(from var(--ang),transparent 0 52%,var(--acc) 70%,#b48cff 86%,transparent 100%);-webkit-mask:linear-gradient(#000 0 0) content-box,linear-gradient(#000 0 0);-webkit-mask-composite:xor;mask-composite:exclude;animation:ang 3s linear infinite}
main.busy{box-shadow:0 18px 70px rgba(94,234,212,.09)}
.orb{display:block;width:92px;height:92px;margin-top:2px;filter:drop-shadow(0 0 16px rgba(94,234,212,.35))}
.stx{max-width:100%;font-weight:560;letter-spacing:-.005em;background:linear-gradient(90deg,var(--mut) 20%,var(--text) 50%,var(--mut) 80%);background-size:200% 100%;-webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent;color:transparent;animation:shimmer 2.4s linear infinite}
.stx.swap{animation:shimmer 2.4s linear infinite,swapin .45s cubic-bezier(.2,.8,.2,1) both}
.bar{position:relative;width:100%;height:6px;border-radius:99px;background:rgba(127,127,160,.16);overflow:hidden}
.bar i{display:block;height:100%;width:8%;border-radius:inherit;background:var(--grad);transition:width 1.4s cubic-bezier(.2,.8,.2,1);position:relative;overflow:hidden}
.bar i::after{content:'';position:absolute;inset:0;width:40%;background:linear-gradient(90deg,transparent,rgba(255,255,255,.55),transparent);animation:sheen 1.6s ease-in-out infinite}
.steps{display:flex;justify-content:space-between;width:100%;font-size:12px;color:var(--mut)}
.steps span{position:relative;padding-left:14px;transition:color .3s}
.steps span::before{content:'';position:absolute;left:0;top:50%;width:8px;height:8px;margin-top:-4px;border-radius:50%;background:var(--bd);transition:background .3s,box-shadow .3s}
.steps span.on{color:var(--text)}.steps span.on::before{background:var(--acc);box-shadow:0 0 0 4px rgba(94,234,212,.18)}
.steps span.done::before{background:var(--acc)}
@media(prefers-reduced-motion:reduce){*,*::before,*::after{animation-duration:.01ms!important;animation-iteration-count:1!important}}
</style></head><body data-authed="${authed ? 1 : 0}"><main>
<div class="mark"><svg width="36" height="36" viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M13.1 5.4L8.3 12.6H11.5L10.7 18.6L15.7 11.2H12.4Z" fill="currentColor" stroke-width="1.3" stroke-linejoin="round"/><circle cx="12" cy="12" r="9.2" stroke-width="1.4" opacity=".5"/><circle cx="19.2" cy="6" r="1.5" fill="currentColor" stroke="none"/></svg></div>
<h1>Power Agent</h1>
<form id="login" class="hidden" style="width:100%;display:flex;flex-direction:column;gap:12px"><p>Enter your agent password</p>
<input id="pw" type="password" placeholder="Password" autocomplete="current-password" autofocus><button>Continue</button><div id="err" class="err"></div></form>
<section id="boot" class="hidden" style="width:100%;display:flex;flex-direction:column;gap:14px;align-items:center"><canvas id="orb" class="orb" aria-hidden="true"></canvas><p id="msg" class="stx">Checking your sandbox...</p><div class="bar"><i id="fill"></i></div><div class="steps"><span id="s1" class="on">Sandbox</span><span id="s2">Agent</span><span id="s3">Ready</span></div><pre id="log" class="hidden"></pre><p id="left" style="font-size:13px"></p></section>
</main><script>
var $=function(i){return document.getElementById(i)};
async function post(u,b){var r=await fetch(u,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(b||{})});var j=await r.json().catch(function(){return{}});if(!r.ok)throw new Error(j.error||('HTTP '+r.status));return j}
$('login').onsubmit=async function(e){e.preventDefault();$('err').textContent='';try{await post('/api/login',{password:$('pw').value});location.reload()}catch(x){$('err').textContent=x.message}};
var opening=false,openedAt=0,kicked=false;
async function tick(){
  var s;try{var r=await fetch('/api/status',{cache:'no-store'});if(r.status===401){location.reload();return}s=await r.json()}catch(e){s={phase:'error',message:e.message}}
  var say={idle:'Creating your sandbox...',stopped:'Waking your sandbox...',pending:'Starting the sandbox...',snapshotting:'Saving the sandbox...',stopping:'Stopping, one moment...',booting:'Power Agent is starting. The very first start takes a few minutes while it downloads and builds FreeLLMAPI.',ready:'Ready. Opening...',failed:'The sandbox failed to start. Retrying...',aborted:'The sandbox was aborted. Retrying...',error:s.message||'Something went wrong'}[s.phase]||s.phase;
  setMsg(say);progress(s.phase);var L=s.log||[];$('log').textContent=L.join('\\n');$('log').classList.toggle('hidden',!L.length);
  $('left').textContent=s.minutesLeft!=null?('Session time left: about '+s.minutesLeft+' min'):'';
  if(s.phase==='ready'){location.replace(s.url);return}
  if(s.phase==='booting'&&!kicked&&!opening){kicked=true;opening=true;openedAt=Date.now();post('/api/open').catch(function(){}).finally(function(){opening=false})}
  if(['idle','stopped','failed','aborted'].indexOf(s.phase)>-1&&!opening&&Date.now()-openedAt>30000){opening=true;openedAt=Date.now();post('/api/open').catch(function(x){$('msg').textContent=x.message}).finally(function(){opening=false})}
  setTimeout(tick,2500)}
function makeOrb(cv,size){var g=cv.getContext('2d');if(!g)return;var dpr=Math.min(2,window.devicePixelRatio||1);cv.width=cv.height=Math.round(size*dpr);cv.style.width=cv.style.height=size+'px';g.scale(dpr,dpr);
var rgb=function(v,d){var m=/^#?([0-9a-f]{6})$/i.exec(String(v||'').trim());if(!m)return d;var n=parseInt(m[1],16);return[n>>16,(n>>8)&255,n&255]};
var A=rgb(getComputedStyle(document.documentElement).getPropertyValue('--acc'),[94,234,212]),B=[180,140,255],C=[122,162,255];
var mix=function(p,q,k){return[p[0]+(q[0]-p[0])*k,p[1]+(q[1]-p[1])*k,p[2]+(q[2]-p[2])*k]};var ramp=function(u){return u<.5?mix(A,C,u*2):mix(C,B,(u-.5)*2)};
var N=52,TAU=Math.PI*2,HOLD=1.5,MORPH=1,CYC=HOLD+MORPH,R=size*.36;
var shapes=[function(u){return[Math.cos(u*TAU),Math.sin(u*TAU)]},function(u){var a=u*TAU,r=.3+.7*Math.abs(Math.cos(2.5*a));return[r*Math.cos(a),r*Math.sin(a)]},function(u){var a=u*TAU;return[Math.cos(a),1.15*Math.sin(a)*Math.cos(a)]},function(u){var a=u*4*Math.PI,r=.12+.88*u;return[r*Math.cos(a),r*Math.sin(a)]},function(u){var a=u*TAU;return[Math.sin(3*a+1.2),Math.sin(2*a)]}];
var ease=function(k){return k<.5?4*k*k*k:1-Math.pow(-2*k+2,3)/2};var px=function(c){return(c[0]|0)+','+(c[1]|0)+','+(c[2]|0)};
var light=function(){return matchMedia('(prefers-color-scheme: light)').matches};
function draw(t){g.globalCompositeOperation='source-over';g.clearRect(0,0,size,size);var i=Math.floor(t/CYC),ph=t-i*CYC,s0=shapes[i%shapes.length],s1=shapes[(i+1)%shapes.length],k=ph<HOLD?0:ease((ph-HOLD)/MORPH),rot=t*.55,cr=Math.cos(rot),sr=Math.sin(rot),br=1+.07*Math.sin(t*2.4),mid=size/2;
var gl=g.createRadialGradient(mid,mid,0,mid,mid,mid);gl.addColorStop(0,'rgba('+px(A)+','+(.2+.09*Math.sin(t*2.4)).toFixed(3)+')');gl.addColorStop(1,'rgba('+px(A)+',0)');g.fillStyle=gl;g.fillRect(0,0,size,size);g.globalCompositeOperation=light()?'source-over':'lighter';
for(var n=0;n<N;n++){var u=n/N,p0=s0(u),p1=s1(u),x=p0[0]+(p1[0]-p0[0])*k+.035*Math.sin(t*3+n*1.7),y=p0[1]+(p1[1]-p0[1])*k+.035*Math.cos(t*2.6+n*2.3),w=.5+.5*Math.sin(t*3.2-u*TAU*2);
g.fillStyle='rgba('+px(ramp((u+t*.08)%1))+','+(.35+.65*w).toFixed(3)+')';g.beginPath();g.arc(mid+(x*cr-y*sr)*R*br,mid+(x*sr+y*cr)*R*br,size*(.026+.03*w),0,TAU);g.fill()}}
if(matchMedia('(prefers-reduced-motion: reduce)').matches){draw(.8);return}
var t0=performance.now();(function f(now){if(!cv.isConnected)return;draw(Math.max(0,(now-t0)/1000));requestAnimationFrame(f)})(t0)}
var shown='',bootAt=0;
function setMsg(t){var x=$('msg');if(t===shown)return;shown=t;x.textContent=t;x.classList.remove('swap');void x.offsetWidth;x.classList.add('swap')}
function progress(phase){var pct={idle:10,stopped:22,pending:38,snapshotting:18,stopping:14,failed:8,aborted:8,error:8,ready:100}[phase],step=1;
 if(phase==='booting'){if(!bootAt)bootAt=Date.now();var secs=(Date.now()-bootAt)/1000;pct=55+37*(1-Math.exp(-secs/120));step=2}
 if(phase==='ready')step=3;
 if(pct!=null)$('fill').style.width=pct.toFixed(1)+'%';
 ['s1','s2','s3'].forEach(function(id,i){var el=$(id);el.className=(i+1<step?'done':i+1===step?'on':'')})}
if(document.body.dataset.authed==='1'){$('boot').classList.remove('hidden');document.querySelector('main').classList.add('busy');makeOrb($('orb'),innerWidth<420?84:92);tick()}else{$('login').classList.remove('hidden')}
</script></body></html>`;

export default function handler(req, res) {
  let authed = false;
  try { authed = isAuthed(req); } catch { /* AGENT_PASSWORD missing: show the form, login will explain */ }
  res.statusCode = 200;
  res.setHeader('content-type', 'text/html; charset=utf-8');
  res.setHeader('cache-control', 'no-store');
  res.end(PAGE(authed));
}
