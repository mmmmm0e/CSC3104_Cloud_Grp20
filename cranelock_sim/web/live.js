"use strict";
const $=id=>document.getElementById(id);
let state=null,busy=false,token="",version=0;
const name=id=>id?id.replace("crane-","Crane "):"none";
const node=(tag,text,cls)=>{const n=document.createElement(tag);if(text!==undefined)n.textContent=text;if(cls)n.className=cls;return n;};
const shape='<svg viewBox="0 0 90 76" fill="none" aria-hidden="true"><path d="M13 64V12h64v52M13 20h64" stroke="#d79639" stroke-width="7"/><path d="M6 10h78M22 22v35M68 22v35M23 33h44" stroke="#e6af57" stroke-width="3"/><path d="M45 9v26l6 7H39l6-7" stroke="#405c70" stroke-width="2"/><rect x="27" y="45" width="38" height="20" rx="2" fill="#55949c"/><path d="M34 48v14m7-14v14m7-14v14m7-14v14" stroke="#87b5b9"/><rect x="65" y="23" width="12" height="15" rx="2" fill="#235a7e"/><circle cx="13" cy="67" r="5" fill="#344f60"/><circle cx="77" cy="67" r="5" fill="#344f60"/></svg>';
function actionButton(text,action,crane,disabled=false){const b=node("button",text);b.dataset.action=action;b.dataset.crane=crane;b.disabled=disabled||busy;b.addEventListener("click",()=>send(action,crane));return b;}
function render(s){
  const focused=document.activeElement,focusedAction=focused?.dataset.action,focusedCrane=focused?.dataset.crane;
  state=s;token=s.session_token;
  document.querySelector(".faults .section-head .pill").textContent=s.automatic?"Automatic recovery and return":"Manual experiment controls";
  $("recover").disabled=busy||s.automatic;
  $("proposer").disabled=s.automatic;
  $("connection").textContent="● Connected to actual local services";
  $("epoch").textContent=`Epoch ${s.gate.epoch}`;$("revision").textContent=`Revision ${s.gate.revision} · ${name(s.gate.holder)}`;
  $("mode").textContent=s.gate.mode;$("controller-state").textContent=s.controller_online?"Controller service running":"Controller stopped";
  $("lane-state").textContent=s.gate_online?(s.gate_fresh?s.gate.lane_state:"RECONNECTING"):"OFFLINE";$("sensor-state").textContent=s.observations?"Position updates enabled":"Position updates paused";
  $("completed").textContent=s.completed;$("holder").textContent=`Holder: ${name(s.gate.holder)}`;
  $("physical-state").textContent=`Physical lane: ${s.occupant?name(s.occupant)+" inside":"empty"}`;
  $("gate-light").classList.toggle("clear",s.gate_fresh&&s.gate.lane_state==="CLEAR");
  $("network-state").textContent=new Set(Object.values(s.groups)).size>1?"Peer network partitioned":"Peer network connected";
  $("decision").textContent=s.decision;$("evidence").textContent=s.evidence;
  $("controller-toggle").textContent=s.controller_online?"Stop controller":"Restore controller";
  $("controller-toggle").dataset.action=s.controller_online?"stop_controller":"restore_controller";
  $("sensor-toggle").textContent=s.observations?"Pause position updates":"Resume position updates";
  $("gate-toggle").textContent=s.gate_online?"Stop gate":"Restore gate";
  $("leave").disabled=busy||!s.occupant||!s.agents.find(a=>a.id===s.occupant)?.running;$("leave").dataset.crane=s.occupant||"crane-1";
  $("vote-count").textContent=`${s.recovery.votes} / ${s.recovery.required}`;$("vote-status").textContent=s.recovery.status;
  $("vote-signers").textContent=s.recovery.signers.length?`Verified signers: ${s.recovery.signers.map(name).join(", ")}`:"No majority certificate installed by this attempt.";
  const fleet=$("fleet");fleet.replaceChildren();
  for(const a of s.agents){
    const card=node("article",undefined,`fleet-card${a.id===s.gate.holder?" current":""}${a.running?"":" stopped"}`);
    const head=node("div",undefined,"fleet-title");head.append(node("span",name(a.id),"crane-name"),node("span",a.running?a.mode:"STOPPED","fleet-status"));
    const info=a.id===s.occupant?"Inside shared lane":s.reservation===a.id?"Reserved · ready for entry":a.has_token&&(a.epoch!==s.gate.epoch||a.revision!==s.gate.revision)?"Old token · fenced":s.waiting.includes(a.id)?"Request waiting":a.id===s.gate.holder?"Current gate holder":"Waiting / idle";
    card.append(head,node("div",info,"fleet-detail"));
    if(a.controller_suspected)card.append(node("div","Controller suspected","hint"));
    if(a.queue.length)card.append(node("div",`Token queue: ${a.queue.map(name).join(", ")}`,"hint"));
    const actions=node("div",undefined,"fleet-actions");actions.append(actionButton("Request turn","request",a.id,!a.running),actionButton("Enter lane","enter",a.id,!a.running||!s.gate_fresh||s.reservation!==a.id),actionButton(a.running?"Stop":"Restart",a.running?"stop_agent":"restart_agent",a.id));card.append(actions);fleet.append(card);
    let art=document.getElementById(`art-${a.id}`);
    if(!art){art=node("div",undefined,"gantry");art.id=`art-${a.id}`;art.innerHTML=shape;art.append(node("span",name(a.id),"tag"));$("crane-art").append(art);}
    art.style.setProperty("--x",`${[...s.agents].findIndex(x=>x.id===a.id)*30+20}%`);
    art.classList.toggle("inside",s.occupant===a.id);art.classList.toggle("holder",s.gate.holder===a.id);art.classList.toggle("stopped",!a.running);
  }
  const events=$("events");events.replaceChildren();
  for(const e of [...s.events].reverse()){
    const raw=e.agent_event||e.controller_event||e,label=raw.event.replaceAll("_"," ").toLowerCase();
    const row=node("div",undefined,"event"),body=node("div");body.append(node("strong",label));
    const details=[raw.agent_id||raw.crane,raw.reason,raw.holder?`holder ${name(raw.holder)}`:"",raw.epoch!==undefined?`epoch ${raw.epoch}`:""] .filter(Boolean).join(" · ");
    if(details)body.append(node("small",details));row.append(node("time",`${e.time.toFixed(2)}s`),body);events.append(row);
  }
  if(focusedAction&&!document.contains(focused)){
    const replacement=[...document.querySelectorAll("button")].find(b=>b.dataset.action===focusedAction&&b.dataset.crane===focusedCrane&&!b.disabled);
    replacement?.focus({preventScroll:true});
  }
}
async function send(action,crane){
  if(busy)return;let failure=null;busy=true;version++;document.body.classList.add("busy");document.querySelectorAll("button").forEach(b=>b.disabled=true);
  try{const response=await fetch("/api/live/action",{method:"POST",headers:{"Content-Type":"application/json","X-CraneLock-Session":token},body:JSON.stringify({action,crane:crane||$("proposer").value})});const data=await response.json();if(!response.ok)throw Error(data.error);state=data;}
  catch(error){failure=error.message;}
  finally{busy=false;document.body.classList.remove("busy");document.querySelectorAll("button").forEach(b=>b.disabled=false);if(state)render(state);if(failure)$("decision").textContent=failure;}
}
document.querySelectorAll("button[data-action]").forEach(b=>b.addEventListener("click",()=>send(b.dataset.action,b.dataset.crane)));
$("recover").addEventListener("click",()=>send("recover",$("proposer").value));
async function poll(){if(!busy){const current=version;try{const response=await fetch("/api/live/state");if(!response.ok)throw Error("Yard unavailable");const data=await response.json();if(!busy&&version===current)render(data);}catch(error){$("connection").textContent="Connection lost · no movement authorized";}}setTimeout(poll,700);}
poll();
