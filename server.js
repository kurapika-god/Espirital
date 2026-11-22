#!/usr/bin/env node
/* ESPIRITAL — servidor local (sem dependências).
   Uso:  node server.js [porta]      (padrão 8080)
   Abre o jogo em http://localhost:PORTA e na rede local em http://SEU-IP:PORTA
   Faz: serve o espirital.html, perfis com salvamento na nuvem, salas de duelo
   (retransmissão entre jogadores) e clãs com chat. Dados em espirital-data.json */
const http=require('http'),fs=require('fs'),path=require('path'),crypto=require('crypto'),os=require('os');
const PORT=+process.argv[2]||+process.env.PORT||8080,DIR=__dirname,DBF=path.join(DIR,'espirital-data.json');
const GUID='258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

/* ---------- banco de dados (arquivo JSON) ---------- */
let db={players:{},clans:{}};
try{db=Object.assign(db,JSON.parse(fs.readFileSync(DBF,'utf8')))}catch(e){}
let dirty=false;const touch=()=>{dirty=true};
function flush(){if(!dirty)return;dirty=false;try{fs.writeFileSync(DBF+'.tmp',JSON.stringify(db));fs.renameSync(DBF+'.tmp',DBF)}catch(e){console.log('erro ao salvar dados:',e.message)}}
setInterval(flush,3000);process.on('SIGINT',()=>{flush();process.exit(0)});process.on('SIGTERM',()=>{flush();process.exit(0)});

/* ---------- HTTP ---------- */
const server=http.createServer((req,res)=>{
  const u=req.url.split('?')[0];
  if(u==='/'||u==='/index.html'||u==='/espirital.html'){
    fs.readFile(path.join(DIR,'espirital.html'),(e,d)=>{
      if(e){res.writeHead(404,{'Content-Type':'text/plain; charset=utf-8'});res.end('espirital.html não encontrado ao lado do server.js');return}
      res.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-store'});res.end(d)});return}
  if(u==='/health'){res.writeHead(200,{'Content-Type':'text/plain'});res.end('ok '+clients.size+' online');return}
  res.writeHead(404,{'Content-Type':'text/plain'});res.end('404');
});

/* ---------- WebSocket mínimo (RFC 6455) ---------- */
const clients=new Set(),online=new Map(),rooms=new Map();
function frame(str){const p=Buffer.from(str),n=p.length;let h;
  if(n<126)h=Buffer.from([0x81,n]);else if(n<65536){h=Buffer.alloc(4);h[0]=0x81;h[1]=126;h.writeUInt16BE(n,2)}
  else{h=Buffer.alloc(10);h[0]=0x81;h[1]=127;h.writeBigUInt64BE(BigInt(n),2)}
  return Buffer.concat([h,p])}
class Client{
  constructor(sock){this.sock=sock;this.buf=Buffer.alloc(0);this.frag=[];this.alive=true;this.tok=null;this.name='?';this.room=null;this.last=Date.now();this.cnt=0;this.cntT=Date.now()}
  send(o){if(!this.alive)return;try{this.sock.write(frame(JSON.stringify(o)))}catch(e){}}
  raw(op,payload){try{const n=payload.length;this.sock.write(Buffer.concat([Buffer.from([0x80|op,n]),payload]))}catch(e){}}
  data(chunk){
    this.buf=Buffer.concat([this.buf,chunk]);if(this.buf.length>2e6){this.close();return}
    for(;;){const b=this.buf;if(b.length<2)return;
      const fin=!!(b[0]&0x80),op=b[0]&15,masked=!!(b[1]&0x80);let len=b[1]&127,off=2;
      if(len===126){if(b.length<4)return;len=b.readUInt16BE(2);off=4}else if(len===127){if(b.length<10)return;len=Number(b.readBigUInt64BE(2));off=10}
      if(len>1.5e6){this.close();return}
      const need=off+(masked?4:0)+len;if(b.length<need)return;
      let pl=b.subarray(off+(masked?4:0),need);
      if(masked){const k=b.subarray(off,off+4),o=Buffer.alloc(len);for(let i=0;i<len;i++)o[i]=pl[i]^k[i&3];pl=o}
      this.buf=b.subarray(need);
      if(op===8){this.close();return}
      if(op===9){this.raw(10,pl);continue}
      if(op===10)continue;
      if(op===1||op===0){this.frag.push(pl);if(fin){const s=Buffer.concat(this.frag).toString('utf8');this.frag=[];this.text(s)}}}}
  text(s){
    const now=Date.now();if(now-this.cntT>1000){this.cntT=now;this.cnt=0}if(++this.cnt>140)return; /* limite de mensagens por segundo */
    let m;try{m=JSON.parse(s)}catch(e){return}if(!m||typeof m.t!=='string')return;
    try{handle(this,m)}catch(e){console.log('erro:',e.message)}}
  close(){if(!this.alive)return;this.alive=false;try{this.sock.destroy()}catch(e){}gone(this)}
}
server.on('upgrade',(req,sock)=>{
  if(req.url.split('?')[0]!=='/ws'||!req.headers['sec-websocket-key']){sock.destroy();return}
  const acc=crypto.createHash('sha1').update(req.headers['sec-websocket-key']+GUID).digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+acc+'\r\n\r\n');
  sock.setNoDelay(true);const c=new Client(sock);clients.add(c);
  sock.on('data',d=>c.data(d));sock.on('close',()=>c.close());sock.on('error',()=>c.close());
});
setInterval(()=>{for(const c of clients)if(Date.now()-c.last>45000)c.close();else c.raw(9,Buffer.alloc(0))},15000);

/* ---------- lógica ---------- */
const clean=(s,n)=>String(s==null?'':s).replace(/[^\p{L}\p{N}_ .\-]/gu,'').trim().slice(0,n);
const ckey=n=>n.toLowerCase();
const ROOMCH='ABCDEFGHJKLMNPQRSTUVWXYZ';
function newCode(){for(;;){let c='';for(let i=0;i<4;i++)c+=ROOMCH[crypto.randomInt(ROOMCH.length)];if(!rooms.has(c))return c}}
function clanPub(p,full){
  if(!p||!p.clan||!db.clans[p.clan])return null;const c=db.clans[p.clan];
  const o={name:c.name,tag:c.tag,wins:c.wins,members:c.members.length,leader:(db.players[c.leader]||{}).name};
  if(full){o.list=c.members.map(t=>{const q=db.players[t]||{};return{n:q.name||'?',on:online.has(t),w:(q.stats||{}).wins|0,l:t===c.leader}});o.chat=c.chat.slice(-30)}
  return o}
function sendClan(c){const p=db.players[c.tok];c.send({t:'clan.info',clan:clanPub(p,true)})}
function broadcastClan(key,msg){const c=db.clans[key];if(!c)return;for(const t of c.members){const o=online.get(t);if(o)o.send(msg)}}
function leaveRoom(c,reason){
  const r=rooms.get(c.room);c.room=null;if(!r)return;
  if(r.host===c){if(r.guest){r.guest.room=null;r.guest.send({t:'room.left',who:'host'})}rooms.delete(r.code)}
  else if(r.guest===c){r.guest=null;if(r.host)r.host.send({t:'room.left',who:'guest'})}}
function gone(c){clients.delete(c);leaveRoom(c);if(c.tok&&online.get(c.tok)===c){online.delete(c.tok);}}
function handle(c,m){
  c.last=Date.now();
  if(m.t==='hello'){
    const name=clean(m.name,14)||'Pactuário';let tok=typeof m.token==='string'?m.token:'';
    let p=db.players[tok];
    if(!p){tok=crypto.randomUUID();p=db.players[tok]={name,save:null,rev:0,clan:null,stats:{duels:0,wins:0},t:Date.now()};}
    p.name=name;c.tok=tok;c.name=name;
    const old=online.get(tok);if(old&&old!==c){old.send({t:'err',m:'Outra janela entrou com este perfil.'});old.close()}
    online.set(tok,c);touch();
    c.send({t:'welcome',name,token:tok,save:p.save,rev:p.rev|0,clan:clanPub(p,false),stats:p.stats,online:online.size});return}
  if(!c.tok)return;const p=db.players[c.tok];if(!p)return;
  switch(m.t){
    case 'save':{if(m.save&&typeof m.save==='object'&&JSON.stringify(m.save).length<200000){p.save=m.save;p.rev=m.save.rev|0;touch()}break}
    case 'ping':c.send({t:'pong',online:online.size});break;
    case 'rooms':{const list=[];for(const r of rooms.values())if(!r.guest&&r.host.alive)list.push({code:r.code,host:r.host.name,lo:r.lo&&{cid:r.lo.cid,wid:r.lo.wid}});c.send({t:'rooms',list,online:online.size});break}
    case 'room.create':{if(c.room)leaveRoom(c);const code=newCode();rooms.set(code,{code,host:c,guest:null,cfg:m.cfg||{},lo:m.lo||{}});c.room=code;c.send({t:'room.created',code});break}
    case 'room.join':{const code=clean(m.code,4).toUpperCase(),r=rooms.get(code);
      if(!r||!r.host.alive){c.send({t:'err',m:'Sala não encontrada.'});break}
      if(r.guest||r.host===c){c.send({t:'err',m:'Sala cheia.'});break}
      if(c.room)leaveRoom(c);r.guest=c;c.room=code;
      c.send({t:'room.joined',code,host:r.host.name,cfg:r.cfg,lo:r.lo});r.host.send({t:'room.guest',name:c.name,lo:m.lo||{}});break}
    case 'room.leave':leaveRoom(c);break;
    case 'rel':{const r=rooms.get(c.room);if(!r)break;const o=r.host===c?r.guest:r.host;if(o)o.send({t:'rel',d:m.d});break}
    case 'stat':{if(m.k==='duel'){p.stats.duels=(p.stats.duels|0)+1;if(m.win){p.stats.wins=(p.stats.wins|0)+1;const cl=db.clans[p.clan];if(cl)cl.wins=(cl.wins|0)+1}touch()}break}
    case 'clan.list':{const l=Object.values(db.clans).sort((a,b)=>b.wins-a.wins).slice(0,30).map(x=>({name:x.name,tag:x.tag,members:x.members.length,wins:x.wins}));c.send({t:'clan.list',list:l});break}
    case 'clan.info':sendClan(c);break;
    case 'clan.create':{
      if(p.clan&&db.clans[p.clan]){c.send({t:'err',m:'Você já está em um clã.'});break}
      const name=clean(m.name,16),tag=clean(m.tag,4).toUpperCase();
      if(name.length<3||tag.length<2){c.send({t:'err',m:'Nome (3+) e tag (2 a 4 letras) são obrigatórios.'});break}
      if(db.clans[ckey(name)]){c.send({t:'err',m:'Já existe um clã com esse nome.'});break}
      db.clans[ckey(name)]={name,tag,leader:c.tok,members:[c.tok],wins:0,chat:[]};p.clan=ckey(name);touch();sendClan(c);break}
    case 'clan.join':{
      if(p.clan&&db.clans[p.clan]){c.send({t:'err',m:'Você já está em um clã.'});break}
      const cl=db.clans[ckey(clean(m.name,16))];if(!cl){c.send({t:'err',m:'Clã não encontrado.'});break}
      if(cl.members.length>=30){c.send({t:'err',m:'Clã cheio.'});break}
      cl.members.push(c.tok);p.clan=ckey(cl.name);cl.chat.push({n:'—',t:p.name+' entrou no clã',ts:Date.now()});if(cl.chat.length>60)cl.chat.shift();touch();
      broadcastClan(p.clan,{t:'clan.msg',m:cl.chat[cl.chat.length-1]});sendClan(c);for(const t of cl.members){const o=online.get(t);if(o&&o!==c)sendClan(o)}break}
    case 'clan.leave':{
      const key=p.clan,cl=db.clans[key];if(!cl){p.clan=null;sendClan(c);break}
      cl.members=cl.members.filter(t=>t!==c.tok);p.clan=null;
      if(!cl.members.length)delete db.clans[key];
      else{if(cl.leader===c.tok)cl.leader=cl.members[0];cl.chat.push({n:'—',t:p.name+' saiu do clã',ts:Date.now()});for(const t of cl.members){const o=online.get(t);if(o)sendClan(o)}}
      touch();sendClan(c);break}
    case 'clan.chat':{
      const cl=db.clans[p.clan];if(!cl)break;const text=String(m.text||'').replace(/[\u0000-\u001f]/g,'').trim().slice(0,200);if(!text)break;
      const msg={n:p.name,t:text,ts:Date.now()};cl.chat.push(msg);if(cl.chat.length>60)cl.chat.shift();touch();broadcastClan(p.clan,{t:'clan.msg',m:msg});break}
  }
}
server.listen(PORT,'0.0.0.0',()=>{
  console.log('\n  ESPIRITAL — servidor rodando\n');
  console.log('  Neste computador:  http://localhost:'+PORT);
  for(const l of Object.values(os.networkInterfaces()))for(const a of l||[])if(a.family==='IPv4'&&!a.internal)console.log('  Na rede local:     http://'+a.address+':'+PORT+'   (passe este endereço para os amigos)');
  console.log('\n  Dados salvos em: '+DBF+'\n  Ctrl+C para parar.\n');
});
