const http=require('http'),fs=require('fs'),path=require('path'),{WebSocketServer}=require('ws');
const F=path.join(process.env.DATA_DIR||__dirname,'db.json');
let db={},dirty=0;try{db=JSON.parse(fs.readFileSync(F))}catch(e){}
setInterval(()=>{if(dirty){dirty=0;fs.writeFile(F,JSON.stringify(db),()=>{})}},5000);
const html=()=>fs.readFileSync(path.join(__dirname,'index.html'));
const srv=http.createServer((q,r)=>{r.writeHead(200,{'Content-Type':'text/html; charset=utf-8'});r.end(html())});
const wss=new WebSocketServer({server:srv,maxPayload:8e6});
const P=p=>String(p).split('/').filter(Boolean),get=p=>P(p).reduce((o,k)=>o==null?o:o[k],db);
function set(p,v){const k=P(p);if(!k.length)return;let o=db;k.slice(0,-1).forEach(x=>{if(typeof o[x]!='object'||o[x]===null)o[x]={};o=o[x]});const l=k[k.length-1];v==null?delete o[l]:o[l]=v;
 for(let i=k.length-1;i>0;i--){const pp=k.slice(0,i),o2=pp.reduce((a,x)=>a&&a[x],db);if(o2&&typeof o2=='object'&&!Object.keys(o2).length)delete pp.slice(0,-1).reduce((a,x)=>a[x],db)[pp[pp.length-1]];else break}dirty=1}
const rel=(a,b)=>{for(let i=0;i<Math.min(a.length,b.length);i++)if(a[i]!=b[i])return false;return true};
const kids=o=>o&&typeof o=='object'?Object.keys(o):[];
function notify(p){const k=P(p);wss.clients.forEach(ws=>ws.subs&&ws.subs.forEach((s,id)=>{if(!rel(k,s.p))return;const o=get(s.p.join('/'));
 if(s.m=='v')ws.send_({t:'val',id,v:o??null});
 else if(s.m=='ca')kids(o).filter(x=>!s.keys.has(x)).sort().forEach(x=>{s.keys.add(x);ws.send_({t:'ca',id,k:x,v:o[x]})});
 else[...s.keys].filter(x=>!kids(o).includes(x)).forEach(x=>{s.keys.delete(x);ws.send_({t:'cr',id,k:x})})}))}
wss.on('connection',ws=>{ws.subs=new Map();ws.odr=[];ws.send_=o=>ws.readyState==1&&ws.send(JSON.stringify(o));
 ws.on('message',d=>{let m;try{m=JSON.parse(d)}catch(e){return}
  if(m.t=='sub'){const o=get(m.p),s={p:P(m.p),m:m.m,keys:new Set(m.m=='v'?[]:kids(o))};ws.subs.set(m.id,s);
   if(m.m=='v')ws.send_({t:'val',id:m.id,v:o??null});else if(m.m=='ca'){const ks=kids(o).sort();ks.slice(-(m.lim||ks.length)).forEach(x=>ws.send_({t:'ca',id:m.id,k:x,v:o[x]}))}}
  else if(m.t=='unsub')ws.subs.delete(m.id);
  else if(m.t=='get')ws.send_({t:'r',q:m.q,v:get(m.p)??null});
  else if(m.t=='set'){set(m.p,m.v);notify(m.p)}
  else if(m.t=='rm'){set(m.p,null);notify(m.p)}
  else if(m.t=='push'){const k=Date.now().toString(36).padStart(9,'0')+Math.random().toString(36).slice(2,6);set(m.p+'/'+k,m.v);notify(m.p+'/'+k)}
  else if(m.t=='cas'){const ok=JSON.stringify(get(m.p)??null)==JSON.stringify(m.old??null);if(ok){set(m.p,m.v);notify(m.p)}ws.send_({t:'r',q:m.q,ok})}
  else if(m.t=='odr'&&!ws.odr.includes(m.p))ws.odr.push(m.p)});
 ws.on('close',()=>ws.odr.forEach(p=>{set(p,null);notify(p)}))});
setInterval(()=>wss.clients.forEach(c=>c.ping()),25000);
srv.listen(process.env.PORT||3000,()=>console.log('Mafya Klubu hazir'));
