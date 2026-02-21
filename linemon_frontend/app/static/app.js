const roomsListEl = document.getElementById("roomsList");
const messagesEl = document.getElementById("messages");
const chatTitleEl = document.getElementById("chatTitle");
const chatMetaEl = document.getElementById("chatMeta");
const statusLineEl = document.getElementById("statusLine");
const sendFormEl = document.getElementById("sendForm");
const sendBtnEl = document.getElementById("sendBtn");
const messageInputEl = document.getElementById("messageInput");
const refreshRoomsBtnEl = document.getElementById("refreshRoomsBtn");
const loadOlderBtnEl = document.getElementById("loadOlderBtn");

const state = {
  rooms: [],
  selectedRoomId: null,
  lastMessageIdByRoom: new Map(),
  oldestMessageIdByRoom: new Map(),
  hasMoreOlderByRoom: new Map(),
  sending: false,
};

const fmt = new Intl.DateTimeFormat(undefined, {
  year: "numeric",
  month: "short",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
});

function escapeHtml(text) {
  const div = document.createElement("div");
  div.textContent = text ?? "";
  return div.innerHTML;
}

function shortPreview(msg) {
  if (!msg) return "";
  if (msg.is_image || msg.kind === "image") return "[Image]";
  const raw = (msg.content || "").trim();
  return raw.length > 70 ? `${raw.slice(0, 70)}...` : raw;
}

function setStatus(text, isError = false) {
  statusLineEl.textContent = text || "";
  statusLineEl.classList.toggle("error", Boolean(isError));
}

function setLoadOlderState() {
  if (!state.selectedRoomId) {
    loadOlderBtnEl.disabled = true;
    return;
  }
  const canLoad = Boolean(state.hasMoreOlderByRoom.get(state.selectedRoomId));
  loadOlderBtnEl.disabled = !canLoad;
}

function renderRooms() {
  roomsListEl.innerHTML = "";
  if (!state.rooms.length) {
    roomsListEl.innerHTML = '<p class="muted">No rooms yet.</p>';
    return;
  }

  for (const room of state.rooms) {
    const active = room.id === state.selectedRoomId;
    const item = document.createElement("button");
    item.type = "button";
    item.className = `room-item ${active ? "active" : ""}`;
    const lastMessage = room.last_message || {};
    const updatedRaw = lastMessage.captured_at || room.updated_at;
    const updatedLabel = updatedRaw ? fmt.format(new Date(updatedRaw)) : "-";
    item.innerHTML = `
      <span class="room-name">${escapeHtml(room.canonical_name || room.raw_name || "(unnamed)")}</span>
      <span class="room-preview">${escapeHtml(shortPreview(lastMessage))}</span>
      <span class="room-time">${escapeHtml(updatedLabel)}</span>
    `;
    item.addEventListener("click", () => openRoom(room.id));
    roomsListEl.appendChild(item);
  }
}

function createMessageElement(msg) {
  const wrap = document.createElement("article");
  wrap.className = "msg-row";
  wrap.dataset.mid = String(msg.id);
  const sender = msg.sender || "(unknown)";
  const at = msg.captured_at ? fmt.format(new Date(msg.captured_at)) : "";
  const isImage = msg.is_image || msg.kind === "image";
  const body = isImage ? "[Image message]" : msg.content || "";
  const attachmentUrl = msg.attachment?.url || "";
  wrap.innerHTML = `
    <div class="msg-meta">
      <span>${escapeHtml(sender)}</span>
      <span>${escapeHtml(at)}</span>
    </div>
    <div class="msg-body">${escapeHtml(body)}</div>
    ${attachmentUrl ? `<a class="msg-attachment" href="${escapeHtml(attachmentUrl)}" target="_blank" rel="noreferrer">Open attachment</a>` : ""}
  `;
  return wrap;
}

function syncMessageBounds(roomId) {
  if (!roomId) return;
  const first = messagesEl.querySelector(".msg-row[data-mid]");
  const all = messagesEl.querySelectorAll(".msg-row[data-mid]");
  const last = all.length ? all[all.length - 1] : null;
  if (first) {
    state.oldestMessageIdByRoom.set(roomId, Number(first.dataset.mid));
  }
  if (last) {
    state.lastMessageIdByRoom.set(roomId, Number(last.dataset.mid));
  }
}

function renderMessages(messages, mode = "replace") {
  if (mode === "replace") {
    messagesEl.innerHTML = "";
  }

  const previousHeight = messagesEl.scrollHeight;
  const fragment = document.createDocumentFragment();
  for (const msg of messages) {
    if (messagesEl.querySelector(`.msg-row[data-mid="${msg.id}"]`)) {
      continue;
    }
    fragment.appendChild(createMessageElement(msg));
  }

  if (mode === "prepend") {
    messagesEl.prepend(fragment);
  } else {
    messagesEl.appendChild(fragment);
  }

  syncMessageBounds(state.selectedRoomId);

  if (mode === "prepend") {
    const newHeight = messagesEl.scrollHeight;
    messagesEl.scrollTop = newHeight - previousHeight;
  } else if (mode === "append" || mode === "replace") {
    messagesEl.scrollTop = messagesEl.scrollHeight;
  }
}

function setRoomHistoryFlag(hasMoreOlder) {
  if (!state.selectedRoomId) return;
  state.hasMoreOlderByRoom.set(state.selectedRoomId, Boolean(hasMoreOlder));
  setLoadOlderState();
}

function setSelectedRoomMeta() {
  const room = state.rooms.find((r) => r.id === state.selectedRoomId);
  if (!room) {
    chatTitleEl.textContent = "Select a room";
    chatMetaEl.textContent = "Waiting for room selection";
    setLoadOlderState();
    return;
  }
  chatTitleEl.textContent = room.canonical_name || room.raw_name || "(unnamed)";
  chatMetaEl.textContent = `Room ID ${room.id}`;
  setLoadOlderState();
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const message = payload?.detail || payload?.error || `Request failed (${response.status})`;
    throw new Error(message);
  }
  return payload;
}

async function loadRooms({ preserveSelection = true } = {}) {
  try {
    const payload = await fetchJson("/api/rooms?limit=150");
    state.rooms = payload.rooms || [];
    if (!state.rooms.length) {
      state.selectedRoomId = null;
      renderRooms();
      setSelectedRoomMeta();
      return;
    }
    if (!preserveSelection || !state.selectedRoomId || !state.rooms.some((x) => x.id === state.selectedRoomId)) {
      state.selectedRoomId = state.rooms[0].id;
      await loadMessagesForSelectedRoom();
    }
    renderRooms();
    setSelectedRoomMeta();
  } catch (err) {
    setStatus(String(err.message || err), true);
  }
}

async function openRoom(roomId) {
  if (state.selectedRoomId === roomId) return;
  state.selectedRoomId = roomId;
  renderRooms();
  setSelectedRoomMeta();
  await loadMessagesForSelectedRoom();
}

async function loadMessagesForSelectedRoom() {
  if (!state.selectedRoomId) {
    messagesEl.innerHTML = "";
    setRoomHistoryFlag(false);
    return;
  }
  try {
    const payload = await fetchJson(`/api/rooms/${state.selectedRoomId}/messages?limit=200`);
    const messages = payload.messages || [];
    renderMessages(messages, "replace");
    setRoomHistoryFlag(messages.length >= 200);
  } catch (err) {
    setStatus(String(err.message || err), true);
  }
}

async function loadOlderMessages() {
  if (!state.selectedRoomId) return;
  const oldest = state.oldestMessageIdByRoom.get(state.selectedRoomId);
  if (!oldest) return;

  loadOlderBtnEl.disabled = true;
  try {
    const payload = await fetchJson(
      `/api/rooms/${state.selectedRoomId}/messages?limit=200&before_id=${oldest}`
    );
    const messages = payload.messages || [];
    if (messages.length) {
      renderMessages(messages, "prepend");
    }
    setRoomHistoryFlag(messages.length >= 200);
  } catch (err) {
    setStatus(String(err.message || err), true);
  } finally {
    setLoadOlderState();
  }
}

async function pollNewMessages() {
  if (!state.selectedRoomId) return;
  const lastId = state.lastMessageIdByRoom.get(state.selectedRoomId);
  if (!lastId) {
    await loadMessagesForSelectedRoom();
    return;
  }
  try {
    const payload = await fetchJson(
      `/api/rooms/${state.selectedRoomId}/messages?limit=120&after_id=${lastId}`
    );
    const messages = payload.messages || [];
    if (messages.length) {
      renderMessages(messages, "append");
    }
  } catch (err) {
    setStatus(String(err.message || err), true);
  }
}

async function sendMessage(event) {
  event.preventDefault();
  if (!state.selectedRoomId) {
    setStatus("Select a room first.", true);
    return;
  }
  if (state.sending) return;
  const room = state.rooms.find((x) => x.id === state.selectedRoomId);
  const text = (messageInputEl.value || "").trim();
  if (!room || !text) return;
  const chatName = (room.canonical_name || room.raw_name || "").trim();
  if (!chatName) {
    setStatus("Selected room has no usable chat name.", true);
    return;
  }

  state.sending = true;
  sendBtnEl.disabled = true;
  setStatus("Sending...");

  try {
    const payload = await fetchJson("/api/send-chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ chat: chatName, text }),
    });
    if (!payload.ok) {
      const err = payload.error || payload.result?.stderr || "Message send failed";
      throw new Error(err);
    }
    messageInputEl.value = "";
    setStatus("Sent.");
    await loadMessagesForSelectedRoom();
    await loadRooms({ preserveSelection: true });
  } catch (err) {
    setStatus(String(err.message || err), true);
  } finally {
    state.sending = false;
    sendBtnEl.disabled = false;
  }
}

refreshRoomsBtnEl.addEventListener("click", async () => {
  await loadRooms({ preserveSelection: true });
});
loadOlderBtnEl.addEventListener("click", loadOlderMessages);

sendFormEl.addEventListener("submit", sendMessage);

async function boot() {
  await loadRooms({ preserveSelection: false });
  setInterval(() => loadRooms({ preserveSelection: true }), 3500);
  setInterval(() => pollNewMessages(), 1400);
}

boot();
