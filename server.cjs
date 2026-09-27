const http = require('node:http');
const {randomBytes} = require('node:crypto');
const {WebSocketServer, WebSocket} = require('ws');

// All timestamps come from one monotonic clock, immune to later OS clock changes.
function createRelay({graceMs = 120000, heartbeatMs = 4000, sweepMs = 1000} = {}) {
  const epoch = Date.now() - performance.now();
  const now = () => epoch + performance.now();
  const rooms = new Map();
  const server = http.createServer((req, res) => {
    res.writeHead(req.url === '/health' ? 200 : 404, {'Content-Type':'application/json','Cache-Control':'no-store'});
    res.end(JSON.stringify(req.url === '/health' ? {service:'pulse-band-relay',version:2} : {error:'Not found'}));
  });
  const wss = new WebSocketServer({server, path:'/sync', maxPayload:4096, perMessageDeflate:false});
  const send = (socket, data) => {
    if (socket?.readyState === WebSocket.OPEN) {
      if(socket.bufferedAmount > 65536) return socket.terminate();
      socket.send(JSON.stringify(data));
    }
  };
  const snapshot = room => ({code:room.code,state:room.state,members:[...room.members.values()].map(m=>({id:m.id,name:m.name,host:m.id===room.hostId,connected:!!m.socket,ready:m.ready,rtt:m.rtt}))});
  const broadcast = room => { const message={type:'snapshot',...snapshot(room)};for(const m of room.members.values())send(m.socket,message); };
  function stop(room) {room.state={...room.state,playing:false,revision:room.state.revision+1};}
  function closeRoom(room) {
    rooms.delete(room.code);
    for(const m of room.members.values()) {
      send(m.socket,{type:'roomClosed'});
      if(m.socket){m.socket.room=null;m.socket.member=null;}
    }
  }
  function attach(socket,room,member) {
    const previous=member.socket;
    member.socket=socket;member.ready=false;member.disconnectedAt=null;member.lastSeen=now();
    socket.room=room;socket.member=member;
    if(previous&&previous!==socket){previous.room=null;previous.member=null;previous.close(4001,'Session replaced');}
  }
  wss.on('connection', socket => {
    if(wss.clients.size>1000){socket.close(1013,'Capacity');return;}
    socket.budget={at:now(),count:0};socket.connectedAt=now();
    socket.on('error',()=>{});
    socket.on('message',(raw,isBinary)=> {
      let message;
      try {if(isBinary)throw Error();message=JSON.parse(raw.toString());if(!message||typeof message!=='object')throw Error();}catch {send(socket,{type:'error',error:'잘못된 메시지입니다.'});return;}
      const id = typeof message.id === 'number' ? message.id : null;
      const reply = data => send(socket,{type:'reply',id,...data});
      const fail = error => reply({error});
      const time=now();
      if(time-socket.budget.at>1000)socket.budget={at:time,count:0};
      if(++socket.budget.count>40){socket.close(1008,'Rate limit');return;}
      if(socket.member)socket.member.lastSeen=time;
      if(message.type==='ping')return reply({serverReceived:time,serverSent:now()});
      if(['create','join','resume'].includes(message.type)) {
        if(socket.room)return fail('이미 방에 연결되어 있습니다.');
        let room,member;
        if(message.type==='create') {
          if(rooms.size>=500)return fail('서버가 가득 찼습니다.');
          let code;do{code=randomBytes(3).toString('hex').toUpperCase();}while(rooms.has(code));
          room={code,hostId:null,members:new Map(),state:{bpm:120,beats:4,playing:false,startAt:0,revision:0},createdAt:time};
          rooms.set(code,room);
        } else {
          room=rooms.get(message.code);
          if(!room)return fail('방이 종료되었거나 코드를 찾을 수 없습니다.');
        }
        if(message.type==='resume') {
          member=[...room.members.values()].find(m=>m.token===message.token);
          if(!member)return fail('재접속 정보가 만료되었습니다.');
        } else {
          if(room.members.size>=12)return fail('한 방에는 최대 12명이 참여할 수 있습니다.');
          member={id:randomBytes(8).toString('hex'),token:randomBytes(24).toString('hex'),name:typeof message.name==='string'?(message.name.trim().slice(0,20)||'멤버'):'멤버',ready:false,rtt:0};
          room.members.set(member.id,member);
          if(message.type==='create')room.hostId=member.id;
        }
        attach(socket,room,member);
        reply({session:{code:room.code,token:member.token,memberId:member.id,isHost:room.hostId===member.id},...snapshot(room)});
        broadcast(room);return;
      }
      const room=socket.room,member=socket.member;
      if(!room||!member)return fail('먼저 방에 연결하세요.');
      if(message.type==='ready') {
        if(!Number.isFinite(message.rtt)||message.rtt<0||message.rtt>2000)return fail('네트워크 지연이 너무 큽니다.');
        member.ready=true;member.rtt=message.rtt;reply({ok:true});broadcast(room);return;
      }
      if(message.type==='set') {
        if(room.hostId!==member.id)return fail('방장 휴대폰만 제어할 수 있습니다.');
        if(message.revision!==room.state.revision)return fail('설정이 바뀌었습니다. 다시 눌러주세요.');
        if(typeof message.playing!=='boolean'||!Number.isInteger(message.bpm)||message.bpm<30||message.bpm>240||![2,3,4,5,6,7].includes(message.beats))return fail('잘못된 박자 설정입니다.');
        if(room.state.playing && message.playing && (message.bpm!==room.state.bpm||message.beats!==room.state.beats))return fail('정지 후 템포를 변경하세요.');
        const online=[...room.members.values()].filter(m=>m.socket);
        if(message.playing && online.some(m=>!m.ready))return fail('모든 멤버의 동기화가 끝나면 시작할 수 있습니다.');
        const lead=Math.min(3000,Math.max(600,...online.map(m=>m.rtt*3+200)));
        room.state={bpm:message.bpm,beats:message.beats,playing:message.playing,startAt:message.playing?time+lead:0,revision:room.state.revision+1};
        reply({state:room.state,leadMs:lead});broadcast(room);return;
      }
      if(message.type==='leave') {
        reply({ok:true});socket.room=null;socket.member=null;
        if(member.id===room.hostId)closeRoom(room);
        else {room.members.delete(member.id);broadcast(room);}
        return;
      }
      fail('지원하지 않는 요청입니다.');
    });
    socket.on('close',()=> {
      const room=socket.room,m=socket.member;
      if(!room||!m||m.socket!==socket)return;
      m.socket=null;m.ready=false;m.disconnectedAt=now();
      if(m.id===room.hostId)stop(room);
      broadcast(room);
    });
  });
  const sweep=setInterval(()=> {
    const time=now();
    for(const socket of wss.clients) {
      if(time-(socket.member?.lastSeen||socket.connectedAt)>heartbeatMs)socket.terminate();
    }
    for(const room of rooms.values()) {
      const host=room.members.get(room.hostId);
      if((!host.socket&&time-host.disconnectedAt>graceMs)||time-room.createdAt>86400000){closeRoom(room);continue;}
      let changed=false;
      for(const m of room.members.values())if(m.id!==room.hostId&&!m.socket&&time-m.disconnectedAt>graceMs){room.members.delete(m.id);changed=true;}
      if(changed)broadcast(room);
    }
  },sweepMs);
  sweep.unref();
  const close=async()=>{clearInterval(sweep);for(const s of wss.clients)s.terminate();await new Promise(resolve=>wss.close(resolve));await new Promise(resolve=>server.close(resolve));};
  return {server,close};
}
if(require.main===module) {
  const relay=createRelay();
  relay.server.listen(Number(process.env.PORT)||8788,'0.0.0.0',()=>console.log('Pulse Band relay ready'));
  const shutdown=()=>relay.close().then(()=>process.exit(0));process.on('SIGINT',shutdown);process.on('SIGTERM',shutdown);
}
module.exports={createRelay};

