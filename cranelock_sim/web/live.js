"use strict";

const $ = id => document.getElementById(id);
const name = id => id ? id.replace("crane-", "Crane ") : "none";
const shape = '<svg viewBox="0 0 90 76" fill="none" aria-hidden="true"><path d="M13 64V12h64v52M13 20h64" stroke="#d79639" stroke-width="7"/><path d="M6 10h78M22 22v35M68 22v35M23 33h44" stroke="#e6af57" stroke-width="3"/><path d="M45 9v26l6 7H39l6-7" stroke="#405c70" stroke-width="2"/><rect x="27" y="45" width="38" height="20" rx="2" fill="#55949c"/><path d="M34 48v14m7-14v14m7-14v14m7-14v14" stroke="#87b5b9"/><rect x="65" y="23" width="12" height="15" rx="2" fill="#235a7e"/><circle cx="13" cy="67" r="5" fill="#344f60"/><circle cx="77" cy="67" r="5" fill="#344f60"/></svg>';
let state = null;
let busy = false;
let version = 0;
let fleetKey = "";
let latestEvent;
let guideIndex = 0;
let guideActive = false;
let guideFailed = false;
let guideMessage = "";
let guideRun;

function element(tag, text = "", className = "") {
  const result = document.createElement(tag);
  result.textContent = text;
  result.className = className;
  return result;
}

function actionButton(text, action, crane, disabled = false) {
  const button = element("button", text);
  button.dataset.action = action;
  button.dataset.crane = crane;
  button.disabled = disabled;
  button.addEventListener("click", () => send(action, crane));
  return button;
}

function fleetDetail(yard, agent) {
  if (agent.id === yard.occupant) return "Inside shared lane";
  if (yard.gate_fresh && agent.id === yard.reservation) return "Reserved · ready for entry";
  if (yard.gate_fresh && agent.has_token &&
      (agent.epoch !== yard.gate.epoch || agent.revision !== yard.gate.revision)) {
    return "Old token · fenced";
  }
  if (yard.waiting.includes(agent.id)) return "Request waiting";
  return yard.gate_fresh && agent.id === yard.gate.holder ? "Current Gate holder" : "Waiting / idle";
}

function renderFleet(yard) {
  const key = JSON.stringify([yard.agents, yard.gate_fresh, yard.gate.holder,
    yard.gate.epoch, yard.gate.revision, yard.occupant, yard.reservation, yard.waiting]);
  if (key === fleetKey) return;
  fleetKey = key;
  const fleet = $("fleet");
  const focused = document.activeElement?.dataset;
  fleet.replaceChildren();
  yard.agents.forEach((agent, index) => {
    const holder = yard.gate_fresh && agent.id === yard.gate.holder;
    const card = element("article", "", "fleet-card" + (holder ? " current" : "") +
        (agent.running ? "" : " stopped"));
    const head = element("div", "", "fleet-title");
    head.append(element("span", name(agent.id), "crane-name"),
      element("span", agent.running ? agent.mode : "STOPPED", "fleet-status"));
    card.append(head, element("div", fleetDetail(yard, agent), "fleet-detail"));
    if (agent.controller_suspected) card.append(element("div", "Controller suspected", "hint"));
    if (agent.queue.length) {
      card.append(element("div", "Token queue: " + agent.queue.map(name).join(", "), "hint"));
    }
    const actions = element("div", "", "fleet-actions");
    actions.append(
      actionButton("Request turn", "request", agent.id, !agent.running),
      actionButton("Enter lane", "enter", agent.id,
        !agent.running || !yard.gate_fresh || yard.reservation !== agent.id),
      actionButton(agent.running ? "Stop" : "Restart", agent.running ? "stop_agent" : "restart_agent", agent.id),
    );
    card.append(actions);
    fleet.append(card);

    let art = $("art-" + agent.id);
    if (!art) {
      art = element("div", "", "gantry");
      art.id = "art-" + agent.id;
      art.innerHTML = shape;
      art.append(element("span", name(agent.id), "tag"));
      art.style.setProperty("--x", (index * 30 + 20) + "%");
      $("crane-art").append(art);
    }
    art.classList.toggle("inside", yard.occupant === agent.id);
    art.classList.toggle("holder", holder);
    art.classList.toggle("stopped", !agent.running);
  });
  if (focused?.crane) {
    const replacement = [...fleet.querySelectorAll("button")].find(button =>
      button.dataset.action === focused.action && button.dataset.crane === focused.crane && !button.disabled);
    replacement?.focus({preventScroll: true});
  }
}

function renderEvents(events) {
  const stamp = events.at(-1)?.time;
  if (stamp === latestEvent) return;
  latestEvent = stamp;
  $("events").replaceChildren(...events.toReversed().map(event => {
    const raw = event.agent_event || event;
    const row = element("div", "", "event");
    const body = element("div");
    body.append(element("strong", raw.event.replaceAll("_", " ").toLowerCase()));
    const description = [raw.agent_id || raw.crane, raw.reason,
      raw.holder && "holder " + name(raw.holder), raw.epoch !== undefined && "epoch " + raw.epoch]
        .filter(Boolean).join(" · ");
    if (description) body.append(element("small", description));
    row.append(element("time", event.time.toFixed(2) + " s"), body);
    return row;
  }));
}

function render(yard) {
  state = yard;
  restoreGuide(yard.evidence);
  $("connection").textContent = "Connected to local services";
  $("connection").dataset.connected = "true";
  $("epoch").textContent = (yard.gate_fresh ? "Epoch " : "Last epoch ") + yard.gate.epoch;
  $("revision").textContent = "Revision " + yard.gate.revision + " · " + name(yard.gate.holder);
  $("mode").textContent = yard.gate_fresh ? yard.gate.mode : "UNCONFIRMED";
  $("mode").dataset.state = $("mode").textContent;
  $("controller-state").textContent = yard.controller_online ? "Controller running" : "Controller stopped";
  $("lane-state").textContent = yard.gate_fresh ? yard.gate.lane_state : yard.gate_online ? "RECONNECTING" : "OFFLINE";
  $("lane-state").dataset.state = $("lane-state").textContent;
  $("sensor-state").textContent = yard.observations ? "Position updates enabled" : "Position updates paused";
  $("completed").textContent = yard.completed;
  $("holder").textContent = (yard.gate_fresh ? "Holder: " : "Last holder: ") + name(yard.gate.holder);
  $("physical-state").textContent = "Physical lane: " + (yard.occupant ? name(yard.occupant) + " inside" : "empty");
  $("physical-state").dataset.occupied = Boolean(yard.occupant);
  $("gate-light").classList.toggle("clear", yard.gate_fresh && yard.gate.lane_state === "CLEAR");
  $("network-state").textContent = new Set(Object.values(yard.groups)).size > 1 ?
    "Peer network partitioned" : "Peer network connected";
  $("decision").textContent = yard.decision;
  $("evidence").textContent = yard.evidence;
  $("controller-toggle").textContent = yard.controller_online ? "Stop controller" : "Restore controller";
  $("controller-toggle").dataset.action = yard.controller_online ? "stop_controller" : "restore_controller";
  $("sensor-toggle").textContent = yard.observations ? "Pause position updates" : "Resume position updates";
  $("gate-toggle").textContent = yard.gate_online ? "Stop gate" : "Restore gate";
  $("leave").disabled = !yard.agents.find(agent => agent.id === yard.occupant)?.running;
  $("leave").dataset.crane = yard.occupant || "";
  $("earlier-grant").disabled = !yard.gate_fresh || !yard.earlier_grant;
  $("clear-lane").disabled = !yard.gate_fresh || (!yard.occupant && yard.gate.lane_state === "CLEAR");
  $("vote-count").textContent = yard.recovery.votes + " / " + yard.recovery.required;
  $("vote-status").textContent = yard.recovery.status;
  renderFleet(yard);
  renderEvents(yard.events);
  renderGuide();
}

async function readYard() {
  const response = await fetch("/api/live/state");
  if (!response.ok) throw Error("Yard unavailable");
  return response.json();
}

async function yardAction(action, crane) {
  const response = await fetch("/api/live/action", {
    method: "POST",
    headers: {"Content-Type": "application/json", "X-CraneLock-Session": state.session_token},
    body: JSON.stringify({action, crane: crane || undefined}),
  });
  const result = await response.json();
  if (!response.ok) throw Error(result.error);
  render(result);
  return result;
}

const pauseFor = milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds));

async function waitForYard(condition, expected) {
  const deadline = performance.now() + 12000;
  while (performance.now() < deadline) {
    const yard = await readYard();
    render(yard);
    if (yard.gate_fresh && condition(yard)) return yard;
    await pauseFor(250);
  }
  throw Error("Still waiting for " + expected + ". Check the live event trail.");
}

async function guideEnter(crane) {
  await waitForYard(yard => yard.reservation === crane, name(crane) + "'s reservation");
  await yardAction("enter", crane);
  if (state.occupant !== crane) throw Error(name(crane) + " was not admitted.");
}

const guideSteps = [
  {title: "Take turns", button: "Start guided demo", pending: "Crane 1 enters while Crane 2 waits.",
    async run() {
      await yardAction("request", "crane-1");
      await guideEnter("crane-1");
      await yardAction("request", "crane-2");
      if (!state.waiting.includes("crane-2")) throw Error("Crane 2 did not queue.");
      await pauseFor(1100);
      await yardAction("exit", "crane-1");
      await guideEnter("crane-2");
      await pauseFor(1000);
      await yardAction("exit", "crane-2");
      if (state.completed !== 2 || state.gate.epoch !== 0) throw Error("Normal crossings did not complete.");
      return "Two cranes took turns. The Gate changed the holder and revision, and the epoch stayed at 0.";
    }},
  {title: "Lose quorum", button: "Isolate peers and stop controller",
    pending: "Crane 1 enters again. Crane 3 queues before the controller stops.",
    async run() {
      await yardAction("request", "crane-1");
      await guideEnter("crane-1");
      await yardAction("request", "crane-3");
      await yardAction("isolate");
      await yardAction("stop_controller");
      const yard = await waitForYard(yard => !yard.controller_online && yard.recovery.status === "No quorum" &&
        yard.gate.epoch === 0 && yard.occupant === "crane-1", "a recorded no-quorum refusal");
      return "No quorum. The agents recorded " + yard.recovery.votes + " of " + yard.recovery.required +
        " required votes. The Gate stayed at epoch 0, and Crane 1 still occupies the lane.";
    }},
  {title: "Recover authority", button: "Restore peer links",
    pending: "The controller is still stopped. The agents can now collect a majority.",
    async run() {
      await yardAction("heal");
      const yard = await waitForYard(result => result.gate.mode === "LOCAL" && result.gate.epoch === 1 &&
        result.gate.holder === "crane-1" && result.occupant === "crane-1" && !result.controller_online,
        "LOCAL authority at epoch 1");
      return "LOCAL authority installed at epoch " + yard.gate.epoch + ". Crane 1 keeps the token and still occupies the lane.";
    }},
  {title: "Fence the old grant", button: "Try earlier permission",
    pending: "The simulator submits the earlier grant to the same Gate.",
    async run() {
      const yard = await yardAction("stale_entry");
      if (yard.decision !== "STALE_EPOCH" || yard.occupant !== "crane-1") {
        throw Error("Expected the Gate to reject the old epoch without changing occupancy.");
      }
      return "The Gate rejected the old grant with STALE_EPOCH. No crane moved.";
    }},
  {title: "Replace the holder", button: "Stop Crane 1",
    pending: "Stopping the token holder also leaves its simulated crane in the lane.",
    async run() {
      await yardAction("stop_agent", "crane-1");
      await waitForYard(yard => yard.gate.epoch === 2 && yard.gate.holder === "crane-2" &&
        yard.occupant === "crane-1" && !yard.reservation && yard.waiting.includes("crane-3"),
        "a replacement holder with the lane still occupied");
      return "Crane 2 holds the token at epoch 2. Crane 1 is still inside, so Crane 3 must wait.";
    }},
  {title: "Clear and resume", button: "Operator clear and cross",
    pending: "The operator confirms simulated clearance before Crane 3 tries to enter.",
    async run() {
      await yardAction("clear");
      await guideEnter("crane-3");
      await pauseFor(1100);
      await yardAction("exit", "crane-3");
      if (state.completed !== 3 || state.gate.lane_state !== "CLEAR") {
        throw Error("The crossing after clearance did not complete.");
      }
      return "Crane 3 crossed with a fresh grant after explicit clearance. Three crossings completed.";
    }},
  {title: "Restore central control", button: "Restore controller",
    pending: "The returning controller reads the Gate's current authority.",
    async run() {
      await yardAction("restore_controller");
      await waitForYard(yard => yard.gate.mode === "CENTRAL" && yard.gate.epoch === 2 &&
        yard.gate.holder === "crane-3", "CENTRAL authority at the current epoch");
      return "CENTRAL returned at epoch 2. The Gate retained the current holder. Guided demo complete.";
    }},
];

function freshYard() {
  return state?.gate_fresh && state.controller_online && state.observations &&
    state.gate.mode === "CENTRAL" && state.gate.epoch === 0 && state.gate.revision === 1 &&
    state.gate.lane_state === "CLEAR" && !state.occupant && !state.reservation &&
    !state.waiting.length && !state.completed && state.agents.every(agent => agent.running) &&
    new Set(Object.values(state.groups)).size === 1;
}

function renderGuide() {
  const step = guideSteps[guideIndex];
  $("guide-progress").textContent = guideIndex + " of " + guideSteps.length + " complete";
  $("guide-next").textContent = step?.button || "Demo complete";
  $("guide-next").disabled = busy || guideFailed || !step || (!guideActive && !freshYard());
  $("guide-exit").hidden = !guideActive;
  $("guide-exit").disabled = busy;
  $("guide-reset").disabled = busy || !state?.can_reset;
  $("guide-reset").title = state && !state.can_reset ? "Restart this server to load reset support." : "";
  $("guide-status").textContent = guideMessage || (freshYard()
    ? "Show turn-taking, quorum recovery, and a safe return to service. The guide runs real actions in the simulated yard."
    : "Select Reset demo to start a fresh guided run.");
  [...$("guide-steps").children].forEach((item, index) => {
    item.classList.toggle("done", index < guideIndex);
    if (index === guideIndex) item.setAttribute("aria-current", "step");
    else item.removeAttribute("aria-current");
  });
}

function restoreGuide(run) {
  if (run === guideRun) return;
  guideRun = run;
  guideIndex = 0;
  guideActive = guideFailed = false;
  guideMessage = "";
  try {
    const saved = JSON.parse(sessionStorage.getItem("cranelock-guide"));
    if (saved?.run === run) {
      guideIndex = saved.index;
      guideActive = saved.active;
      guideFailed = saved.failed || saved.running;
      guideMessage = saved.running ? "The page changed during a step. Check the event trail before using manual controls."
        : saved.message;
    }
  } catch {
    // keeps the guide usable if browser storage is unavailable
  }
  document.body.classList.toggle("guided", guideActive);
}

function saveGuide(running) {
  try {
    sessionStorage.setItem("cranelock-guide", JSON.stringify({run: guideRun, index: guideIndex,
      active: guideActive, failed: guideFailed, message: guideMessage, running}));
  } catch {
    // browser storage is optional for a single page walkthrough
  }
}

function setBusy(value) {
  busy = value;
  document.body.classList.toggle("busy", value);
  renderGuide();
}

async function runGuideStep() {
  const step = guideSteps[guideIndex];
  if (busy || guideFailed || !step || (!guideActive && !freshYard())) return;
  guideActive = true;
  document.body.classList.add("guided");
  guideMessage = step.pending;
  saveGuide(true);
  version++;
  setBusy(true);
  try {
    guideMessage = await step.run();
    guideIndex++;
  } catch (error) {
    guideFailed = true;
    guideMessage = error.message;
  } finally {
    saveGuide(false);
    setBusy(false);
  }
}

async function send(action, crane) {
  if (busy || !state || guideActive) return;
  setBusy(true);
  version++;
  try {
    await yardAction(action, crane);
  } catch (error) {
    $("decision").textContent = error.message;
  } finally {
    setBusy(false);
  }
}

$("guide-steps").append(...guideSteps.map(step => element("li", step.title)));
$("guide-next").addEventListener("click", runGuideStep);
$("guide-exit").addEventListener("click", () => {
  if (busy) return;
  guideActive = false;
  guideFailed = true;
  guideMessage = "Manual controls are available. Select Reset demo to start a fresh guided run.";
  document.body.classList.remove("guided");
  saveGuide(false);
  renderGuide();
});
$("guide-reset").addEventListener("click", async () => {
  if (busy || !state?.can_reset) return;
  version++;
  setBusy(true);
  $("guide-status").textContent = "Starting a fresh yard…";
  try {
    await yardAction("reset");
    saveGuide(false);
  } catch (error) {
    guideMessage = error.message;
  } finally {
    setBusy(false);
  }
});

for (const button of document.querySelectorAll("button[data-action]")) {
  button.addEventListener("click", () => send(button.dataset.action, button.dataset.crane));
}

async function poll() {
  if (!busy) {
    const current = version;
    try {
      const result = await readYard();
      if (!busy && version === current) render(result);
    } catch {
      $("connection").textContent = "Connection lost";
      $("connection").dataset.connected = "false";
    }
  }
  setTimeout(poll, 500);
}
poll();
