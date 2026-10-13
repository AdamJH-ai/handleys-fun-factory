// Non-visual checks of the actual inline browser handlers; run with node.
const fs = require('fs'), vm = require('vm'), assert = require('node:assert/strict');
function harness(file, end) {
 const html=fs.readFileSync(file,'utf8');
 const code=html.slice(html.indexOf('        // Five short clock samples;'),html.indexOf(end));
 const handlers={}, emitted=[], events={}, ramps=[], frames=[];
 const context={currentTime:0,state:'running',destination:{},resume:async()=>{},
  decodeAudioData:async()=>({duration:240}),
  createBufferSource:()=>({connect(){},disconnect(){},stop(){},start(at){this.at=at;},buffer:null}),
  createGain:()=>({connect(){},gain:{setValueAtTime:(...v)=>ramps.push(['set',...v]),linearRampToValueAtTime:(...v)=>ramps.push(['ramp',...v])}})};
 const cls={add(){},remove(){}};
 const area={classList:cls,innerHTML:''};
 const document={body:{classList:cls},addEventListener:(e,f)=>events[e]=f,
  querySelectorAll:()=>[],getElementById:id=>id==='wwts-player-area'?area:null};
 const socket={on:(e,f)=>handlers[e]=f,emit:(e,d)=>emitted.push([e,d]),
  timeout:()=>({emit:(e,cb)=>cb(null,{now:5000})})};
 const sandbox={document,socket,performance:{timeOrigin:0,now:()=>5000},window:{AudioContext:function(){return context}},
  fetch:async()=>({ok:true,arrayBuffer:async()=>new ArrayBuffer(0)}),
  requestAnimationFrame:f=>{frames.push(f);return frames.length},cancelAnimationFrame:()=>{},
  configureInputArea:()=>{},showArea:()=>{},guessLabel:{},currentClientRoundType:null,console};
 vm.createContext(sandbox);vm.runInContext(code,sandbox);
 return {handlers,emitted,events,ramps,frames,context};
}
(async()=>{
 const main=harness('templates/main_screen.html',"        socket.on('hlb_main_phase'");
 main.events.click();
 await main.handlers.wwts_media({phase:'prepare',token:'a',url:'/static/audio/test.mp3'});
 assert(main.emitted.some(([e])=>e==='wwts_ready'));
 await main.handlers.wwts_media({phase:'starting',token:'b',fade:10.8,fade_duration:.5});
 main.context.currentTime=.2;main.frames.pop()();
 assert.equal(main.emitted.at(-1)[0],'wwts_started');
 assert.equal(main.emitted.at(-1)[1].start_ms,4900);
 assert(Math.abs(main.ramps.find(r=>r[0]==='ramp')[2]-11.4)<1e-8);
 await main.handlers.wwts_media({phase:'replay',token:'c',url:'/static/audio/test.mp3',word:18.21,tail:5});
 main.context.currentTime=24;main.frames.pop()();
 assert.equal(main.emitted.at(-1)[0],'wwts_replay_done');
 const phone=harness('templates/index.html',"        socket.on('hlb_player_prompt'");
 await phone.handlers.wwts_prompt({phase:'prepare',token:'a',html:''});
 const button={disabled:false,dataset:{token:'attempt'}};
 const event={target:{closest:()=>button},preventDefault(){}};
 phone.events.pointerdown(event);phone.events.pointerdown(event);
 assert.equal(phone.emitted.filter(([e])=>e==='wwts_tap').length,1);
 assert.equal(phone.emitted.at(-1)[1].tap_ms,5000);
 console.log('PASS: audio clock anchor, fade schedule, replay completion, timestamp at tap and double-tap lock');
})().catch(e=>{console.error(e);process.exit(1)});
