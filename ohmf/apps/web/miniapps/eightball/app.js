import { createMiniAppClientFromLocation } from "../../miniapp-sdk.js";

const runtimeParams = new URLSearchParams(window.location.search);
const isPreviewMode = runtimeParams.get("preview") === "1";
const hasBridgeContext = Boolean(runtimeParams.get("channel") && runtimeParams.get("parent_origin"));
const bridge = hasBridgeContext ? createMiniAppClientFromLocation() : null;
const permissionHelpers = window.OHMFEightballPermissions || {};

const ANSWERS = Object.freeze([
  "It is certain.",
  "Most likely.",
  "Signs point to yes.",
  "Outlook good.",
  "Reply hazy, try again.",
  "Ask again later.",
  "Cannot predict now.",
  "Do not count on it.",
  "Very doubtful.",
  "Concentrate and ask again.",
]);

const state = {
  launchContext: null,
  history: [],
  recentMessages: [],
  currentAnswer: "Ask a question",
  answerCount: 0,
  blockedActions: null,
};

const el = {
  appShell: document.getElementById("app-shell"),
  previewShell: document.getElementById("preview-shell"),
  status: document.getElementById("status-pill"),
  answerText: document.getElementById("answer-text"),
  questionInput: document.getElementById("question-input"),
  viewerLine: document.getElementById("viewer-line"),
  participantsLine: document.getElementById("participants-line"),
  sharedLine: document.getElementById("shared-line"),
  countLine: document.getElementById("count-line"),
  historyList: document.getElementById("history-list"),
  messageList: document.getElementById("message-list"),
  askBtn: document.getElementById("ask-btn"),
  sendBtn: document.getElementById("send-btn"),
  refreshBtn: document.getElementById("refresh-btn"),
  loadDraftBtn: document.getElementById("load-draft-btn"),
  saveDraftBtn: document.getElementById("save-draft-btn"),
  previewAnswerText: document.getElementById("preview-answer-text"),
  previewCaption: document.getElementById("preview-caption"),
};

function sanitizeText(value, limit = 220) {
  return String(value || "").replace(/[\u0000-\u001f\u007f]/g, "").trim().slice(0, limit);
}

function randomAnswer(question) {
  const clean = sanitizeText(question, 220);
  let total = 0;
  for (const char of clean) total += char.charCodeAt(0);
  return ANSWERS[total % ANSWERS.length];
}

function setStatus(message, isError = false) {
  el.status.textContent = sanitizeText(message, 180);
  el.status.classList.toggle("error", isError);
}

function requireBridge() {
  if (bridge) return bridge;
  throw new Error(isPreviewMode ? "Preview mode has no host bridge." : "Missing host bridge.");
}

function renderAnswer() {
  el.answerText.textContent = state.currentAnswer;
  if (el.previewAnswerText) {
    el.previewAnswerText.textContent = state.currentAnswer;
  }
}

function renderContext() {
  const viewer = state.launchContext?.viewer;
  const participants = Array.isArray(state.launchContext?.participants) ? state.launchContext.participants : [];
  const lastQuestion = sanitizeText(state.launchContext?.state_snapshot?.last_question, 160);
  const lastAnswer = sanitizeText(state.launchContext?.state_snapshot?.last_answer, 160);
  const askedBy = sanitizeText(state.launchContext?.state_snapshot?.asked_by, 120);
  el.viewerLine.textContent = viewer ? `Viewer: ${viewer.display_name || viewer.user_id}` : "Viewer: unavailable";
  el.participantsLine.textContent = participants.length
    ? `Participants: ${participants.map((item) => item.display_name || item.user_id).join(", ")}`
    : "Participants: unavailable";
  el.sharedLine.textContent = lastAnswer
    ? `Latest: "${lastQuestion}" → ${lastAnswer}${askedBy ? ` · by ${askedBy}` : ""}`
    : "No shared answer yet.";
  el.countLine.textContent = `Questions asked: ${state.answerCount}`;
  if (el.previewCaption) {
    el.previewCaption.textContent = lastAnswer ? `Latest shared answer: ${lastAnswer}` : "Shared answer ready for the thread.";
  }
}

function renderHistory() {
  el.historyList.replaceChildren();
  const history = Array.isArray(state.launchContext?.state_snapshot?.history) ? state.launchContext.state_snapshot.history : [];
  if (!history.length) {
    const item = document.createElement("li");
    item.textContent = "No answers yet.";
    el.historyList.append(item);
    return;
  }
  for (const entry of history.slice(-6).reverse()) {
    const item = document.createElement("li");
    const title = document.createElement("strong");
    title.textContent = sanitizeText(entry.question, 160) || "Question";
    const body = document.createElement("span");
    body.textContent = sanitizeText(entry.answer, 160);
    item.append(title, body);
    el.historyList.append(item);
  }
}

function renderRecentMessages() {
  el.messageList.replaceChildren();
  if (!state.recentMessages.length) {
    const item = document.createElement("li");
    item.textContent = "No recent thread messages available.";
    el.messageList.append(item);
    return;
  }
  for (const entry of state.recentMessages) {
    const item = document.createElement("li");
    const title = document.createElement("strong");
    title.textContent = sanitizeText(entry.author, 80) || "Unknown";
    const body = document.createElement("span");
    body.textContent = sanitizeText(entry.text, 180);
    item.append(title, body);
    el.messageList.append(item);
  }
}

function blockedActionsForLaunchContext() {
  const granted = Array.isArray(state.launchContext?.capabilities_granted) ? state.launchContext.capabilities_granted : [];
  if (typeof permissionHelpers.describeBlockedActions === "function") {
    return permissionHelpers.describeBlockedActions(granted);
  }
  const grantedSet = new Set(granted);
  const askDisabled = !grantedSet.has("realtime.session");
  const sendDisabled = !grantedSet.has("conversation.send_message");
  const draftDisabled = !grantedSet.has("storage.session");
  const refreshDisabled = !grantedSet.has("conversation.read_context");
  const missing = [];
  if (askDisabled) missing.push("realtime.session");
  if (sendDisabled) missing.push("conversation.send_message");
  if (draftDisabled) missing.push("storage.session");
  if (refreshDisabled) missing.push("conversation.read_context");
  return {
    askDisabled,
    sendDisabled,
    draftDisabled,
    refreshDisabled,
    missing,
    blockedSummary: missing.length ? `Blocked: host denied ${missing.join(", ")}.` : "",
  };
}

function permissionDeniedMessage(error) {
  if (typeof permissionHelpers.permissionErrorMessage === "function") {
    return permissionHelpers.permissionErrorMessage(error);
  }
  return sanitizeText(error?.message, 180) || "Blocked: required permission was denied by the host.";
}

function syncActionAvailability() {
  state.blockedActions = blockedActionsForLaunchContext();
  el.askBtn.disabled = Boolean(state.blockedActions.askDisabled);
  el.sendBtn.disabled = Boolean(state.blockedActions.sendDisabled);
  el.loadDraftBtn.disabled = Boolean(state.blockedActions.draftDisabled);
  el.saveDraftBtn.disabled = Boolean(state.blockedActions.draftDisabled);
  el.refreshBtn.disabled = Boolean(state.blockedActions.refreshDisabled);
  el.questionInput.disabled = Boolean(state.blockedActions.askDisabled);
}

function applyLaunchContext(launchContext) {
  state.launchContext = launchContext || {};
  state.answerCount = Number(state.launchContext?.state_snapshot?.answer_count || 0) || 0;
  state.currentAnswer = sanitizeText(state.launchContext?.state_snapshot?.last_answer, 160) || "Ask a question";
  renderAnswer();
  renderContext();
  renderHistory();
  syncActionAvailability();
}

async function refreshLaunchContext() {
  applyLaunchContext(await requireBridge().getLaunchContext());
  setStatus("Bridge context updated.");
}

async function refreshConversationContext() {
  const context = await requireBridge().readConversationContext();
  state.recentMessages = Array.isArray(context?.recent_messages) ? context.recent_messages : [];
  renderRecentMessages();
}

async function loadDraft() {
  const result = await requireBridge().getSessionStorage("eightball_draft_question");
  el.questionInput.value = typeof result?.value === "string" ? result.value : "";
  setStatus("Draft question loaded.");
}

async function saveDraft() {
  const question = sanitizeText(el.questionInput.value, 220);
  await requireBridge().setSessionStorage("eightball_draft_question", question);
  setStatus("Draft question saved.");
}

async function askQuestion() {
  const question = sanitizeText(el.questionInput.value, 220);
  if (!question) {
    setStatus("Enter a question first.", true);
    return;
  }
  const viewerName = sanitizeText(
    state.launchContext?.viewer?.display_name || state.launchContext?.viewer?.user_id || "Someone",
    80
  );
  const answer = randomAnswer(question);
  const history = Array.isArray(state.launchContext?.state_snapshot?.history) ? state.launchContext.state_snapshot.history.slice(-7) : [];
  history.push({ question, answer, asked_by: viewerName, asked_at: new Date().toISOString() });
  const payload = await requireBridge().updateSessionState({
    last_question: question,
    last_answer: answer,
    asked_by: viewerName,
    answer_count: Number(state.launchContext?.state_snapshot?.answer_count || 0) + 1,
    history,
  });
  state.currentAnswer = sanitizeText(payload?.state_snapshot?.last_answer, 160) || answer;
  applyLaunchContext({
    ...(state.launchContext || {}),
    state_snapshot: payload?.state_snapshot || {},
    state_version: payload?.state_version || state.launchContext?.state_version,
  });
  renderAnswer();
  setStatus(`The 8-ball says: ${state.currentAnswer}`);
}

async function sendSummary() {
  const question = sanitizeText(state.launchContext?.state_snapshot?.last_question, 220);
  const answer = sanitizeText(state.launchContext?.state_snapshot?.last_answer, 220);
  if (!answer) {
    setStatus("Ask the 8-ball first.", true);
    return;
  }
  await requireBridge().sendConversationMessage({
    content_type: "app_event",
    content: {
      event_name: "EIGHTBALL_ANSWERED",
      body: {
        question,
        answer,
      },
    },
    text: question ? `Mystic 8-Ball: "${question}" → ${answer}` : `Mystic 8-Ball says: ${answer}`,
  });
  await refreshConversationContext();
  setStatus("Projected the latest answer into the thread.");
}

async function bootstrapPreview() {
  document.body.classList.add("preview-mode");
  el.appShell.hidden = true;
  el.previewShell.hidden = false;
  renderAnswer();
  if (!bridge) return;
  try {
    await refreshLaunchContext();
  } catch (error) {
    setStatus(error.message || "Preview bridge unavailable.", true);
  }
}

async function bootstrap() {
  if (isPreviewMode) {
    await bootstrapPreview();
    return;
  }
  try {
    await refreshLaunchContext();
    await refreshConversationContext();
    try {
      await loadDraft();
    } catch (error) {
      console.error(error);
    }
    if (state.blockedActions?.blockedSummary) {
      setStatus(state.blockedActions.blockedSummary, true);
    } else {
      setStatus("Mini-app ready.");
    }
  } catch (error) {
    console.error(error);
    setStatus(error.message || "Mini-app failed to boot.", true);
  }
}

if (bridge) {
  bridge.on("session.stateUpdated", (payload) => {
    if (!payload?.state_snapshot) return;
    applyLaunchContext({
      ...(state.launchContext || {}),
      state_snapshot: payload.state_snapshot,
      state_version: payload.state_version || state.launchContext?.state_version,
    });
    setStatus("Shared 8-ball state updated.");
  });
}

el.askBtn?.addEventListener("click", async () => {
  try {
    await askQuestion();
  } catch (error) {
    console.error(error);
    setStatus(error?.code === "permission_denied" ? permissionDeniedMessage(error) : (error.message || "Unable to update shared state."), true);
  }
});

el.sendBtn?.addEventListener("click", async () => {
  try {
    await sendSummary();
  } catch (error) {
    console.error(error);
    setStatus(error?.code === "permission_denied" ? permissionDeniedMessage(error) : (error.message || "Unable to send to thread."), true);
  }
});

el.refreshBtn?.addEventListener("click", async () => {
  try {
    await refreshLaunchContext();
    await refreshConversationContext();
  } catch (error) {
    console.error(error);
    setStatus(error?.code === "permission_denied" ? permissionDeniedMessage(error) : (error.message || "Unable to refresh context."), true);
  }
});

el.loadDraftBtn?.addEventListener("click", async () => {
  try {
    await loadDraft();
  } catch (error) {
    console.error(error);
    setStatus(error?.code === "permission_denied" ? permissionDeniedMessage(error) : (error.message || "Unable to load draft."), true);
  }
});

el.saveDraftBtn?.addEventListener("click", async () => {
  try {
    await saveDraft();
  } catch (error) {
    console.error(error);
    setStatus(error?.code === "permission_denied" ? permissionDeniedMessage(error) : (error.message || "Unable to save draft."), true);
  }
});

bootstrap();
