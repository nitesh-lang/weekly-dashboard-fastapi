/**
 * AI Sales Assistant Widget
 * - Page-aware (dashboard / ams-trend / sales-trend / inventory)
 * - Persistent chat via localStorage (survives refresh)
 * - Full conversation history sent to backend
 * - Clear chat button
 */

(function () {
  "use strict";

  const STORAGE_KEY = "ai_chat_history_v2";
  const MAX_HISTORY = 40; // max messages to store

  // ── Page detection ──────────────────────────────────────────────────────
  function getPageContext() {
    const path = window.location.pathname;
    if (path.includes("ams"))                                      return "ams-trend";
    if (path.includes("inventory"))                                return "inventory";
    if (path.includes("sales-trend") || path.includes("amazon"))  return "sales-trend";
    return "dashboard";
  }

  function getDashboardFilters() {
    const params = new URLSearchParams(window.location.search);
    return {
      week:  params.get("weeks") || "All Weeks",
      brand: params.get("brand") || "All",
      view:  params.get("view")  || "all",
      page:  getPageContext(),
    };
  }

  // ── Suggestions per page ────────────────────────────────────────────────
  const SUGGESTIONS = {
    "ams-trend": [
      "Give me a full AMS analysis for last 4 weeks",
      "Which model has the best ROAS?",
      "Which ASINs have ACOS above 50%?",
      "How has ad spend trended week over week?",
    ],
    "inventory": [
      "Which SKUs are at risk of going out of stock?",
      "What is the overall sell-through rate?",
      "Which models have the highest inventory value?",
      "Show inventory health by category",
    ],
    "sales-trend": [
      "Which model is trending up in last 4 weeks?",
      "Give me a full sales analysis",
      "Which SKU has the highest sales contribution?",
      "Compare this week vs last week",
    ],
    "dashboard": [
      "Give me a full business overview",
      "Which channel is driving the most revenue?",
      "Top 5 models by GMV in last 4 weeks",
      "What is the NLC vs GMV ratio?",
    ],
  };

  const PAGE_LABELS = {
    "ams-trend":   "AMS Intelligence",
    "inventory":   "Inventory Intelligence",
    "sales-trend": "Sales Trend Intelligence",
    "dashboard":   "Sales Intelligence",
  };

  const PAGE_WELCOME = {
    "ams-trend":   "Hi! I have full AMS data — ask me about spend, ROAS, ACOS, model trends, or ASIN performance.",
    "inventory":   "Hi! I have inventory data — ask me about stock levels, sell-through, or risk SKUs.",
    "sales-trend": "Hi! I have full sales data — ask me about model trends, GMV, channels, or weekly comparisons.",
    "dashboard":   "Hi! I have complete data across sales, AMS, and inventory. Ask me anything — or try 'Give me a full business overview'.",
  };

  // ── localStorage helpers ────────────────────────────────────────────────
  function loadHistory() {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      return raw ? JSON.parse(raw) : [];
    } catch (_) { return []; }
  }

  function saveHistory(history) {
    try {
      const trimmed = history.slice(-MAX_HISTORY);
      localStorage.setItem(STORAGE_KEY, JSON.stringify(trimmed));
    } catch (_) {}
  }

  function clearHistory() {
    try { localStorage.removeItem(STORAGE_KEY); } catch (_) {}
  }

  // ── Build widget HTML ───────────────────────────────────────────────────
  function buildWidget() {
    const wrapper = document.createElement("div");
    wrapper.id = "ai-sales-widget";
    wrapper.innerHTML = `
      <button id="ai-widget-trigger" title="Ask AI" aria-label="Open AI Assistant">
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
          <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>
        </svg>
        <span class="ai-trigger-label">Ask AI</span>
      </button>

      <div id="ai-widget-panel" role="dialog" aria-label="AI Assistant" style="display:none;">
        <div id="ai-panel-header">
          <div class="ai-header-info">
            <div class="ai-avatar">AI</div>
            <div>
              <div class="ai-title" id="ai-page-title">Sales Intelligence</div>
              <div class="ai-subtitle" id="ai-filter-badge">Loading...</div>
            </div>
          </div>
          <div style="display:flex;align-items:center;gap:6px;">
            <button id="ai-clear-btn" title="Clear chat history" aria-label="Clear chat">
              <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14H6L5 6"/><path d="M10 11v6M14 11v6"/><path d="M9 6V4h6v2"/></svg>
            </button>
            <button id="ai-close-btn" aria-label="Close">
              <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
            </button>
          </div>
        </div>

        <div id="ai-messages" role="log" aria-live="polite">
          <div class="ai-msg ai-msg-bot" id="ai-welcome-msg">
            <p id="ai-welcome-text"></p>
            <p class="ai-suggestions-label">Try asking:</p>
            <div class="ai-suggestions" id="ai-suggestions"></div>
          </div>
        </div>

        <div id="ai-input-area">
          <textarea id="ai-input" placeholder="Ask about sales, AMS, models, trends..." rows="1" aria-label="Your question"></textarea>
          <button id="ai-send-btn" aria-label="Send">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg>
          </button>
        </div>
      </div>
    `;
    document.body.appendChild(wrapper);
  }

  // ── Render a message bubble (no streaming) ──────────────────────────────
  function renderBubble(role, text) {
    const messages = document.getElementById("ai-messages");
    const div = document.createElement("div");
    div.className = `ai-msg ai-msg-${role}`;
    if (role === "bot" || role === "assistant") {
      const p = document.createElement("p");
      p.innerHTML = formatText(text);
      div.appendChild(p);
    } else {
      div.textContent = text;
    }
    messages.appendChild(div);
    messages.scrollTop = messages.scrollHeight;
    return div;
  }

  function formatText(text) {
    return text
      .replace(/\*\*(.*?)\*\*/g, "<strong>$1</strong>")
      .replace(/\n/g, "<br>");
  }

  // ── Restore saved history into the chat UI ──────────────────────────────
  function restoreHistory() {
    const history = loadHistory();
    if (!history.length) return;
    history.forEach(msg => {
      const role = msg.role === "assistant" ? "bot" : "user";
      renderBubble(role, msg.content);
    });
  }

  // ── Streaming bubble ────────────────────────────────────────────────────
  function appendStreamingBubble() {
    const messages = document.getElementById("ai-messages");
    const div = document.createElement("div");
    div.className = "ai-msg ai-msg-bot ai-streaming";
    const p = document.createElement("p");
    p.textContent = "";
    div.appendChild(p);
    const dots = document.createElement("span");
    dots.className = "ai-typing-dots";
    dots.innerHTML = "<span></span><span></span><span></span>";
    div.appendChild(dots);
    messages.appendChild(div);
    messages.scrollTop = messages.scrollHeight;
    return { bubble: div, p, dots };
  }

  // ── Submit question ─────────────────────────────────────────────────────
  async function submitQuestion(question) {
    if (!question.trim()) return;

    const filters = getDashboardFilters();
    const sendBtn = document.getElementById("ai-send-btn");
    const inputEl = document.getElementById("ai-input");

    // Add user message to UI
    renderBubble("user", question);
    inputEl.value = "";
    inputEl.style.height = "auto";
    sendBtn.disabled = true;

    // Load history, add new user message
    const history = loadHistory();
    history.push({ role: "user", content: question });

    const { bubble, p, dots } = appendStreamingBubble();
    let fullText = "";

    try {
      const response = await fetch("/api/ai/ai-chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          question,
          history: history.slice(-10), // last 10 for context
          week:  filters.week,
          brand: filters.brand,
          view:  filters.view,
          page:  filters.page,
        }),
      });

      if (!response.ok) throw new Error(`Server error: ${response.status}`);

      const reader  = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { value, done } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop();

        for (const line of lines) {
          if (!line.startsWith("data: ")) continue;
          const raw = line.slice(6).trim();
          if (raw === "[DONE]") break;
          try {
            const parsed = JSON.parse(raw);
            if (parsed.error) {
              p.textContent = parsed.error;
              bubble.classList.add("ai-msg-error");
            } else if (parsed.text) {
              fullText += parsed.text;
              p.innerHTML = formatText(fullText);
              document.getElementById("ai-messages").scrollTop = 99999;
            }
          } catch (_) {}
        }
      }

      // Save assistant response to history
      if (fullText) {
        history.push({ role: "assistant", content: fullText });
        saveHistory(history);
      }

    } catch (err) {
      p.textContent = "Something went wrong. Please try again.";
      bubble.classList.add("ai-msg-error");
    } finally {
      dots.remove();
      bubble.classList.remove("ai-streaming");
      sendBtn.disabled = false;
      inputEl.focus();
    }
  }

  // ── Update header ───────────────────────────────────────────────────────
  function updateHeader() {
    const page = getPageContext();
    const f = getDashboardFilters();

    const title = document.getElementById("ai-page-title");
    if (title) title.textContent = PAGE_LABELS[page] || "Sales Intelligence";

    const badge = document.getElementById("ai-filter-badge");
    if (badge) {
      const brandLabel = f.brand && f.brand !== "All" ? f.brand : "All Brands";
      const weekLabel  = f.week  && f.week  !== "All Weeks" ? f.week : "All Weeks";
      badge.textContent = `${weekLabel} · ${brandLabel}`;
    }

    const welcome = document.getElementById("ai-welcome-text");
    if (welcome) welcome.textContent = PAGE_WELCOME[page] || PAGE_WELCOME["dashboard"];
  }

  // ── Render suggestion chips ─────────────────────────────────────────────
  function renderSuggestions() {
    const container = document.getElementById("ai-suggestions");
    if (!container) return;
    container.innerHTML = "";
    (SUGGESTIONS[getPageContext()] || SUGGESTIONS["dashboard"]).forEach((text) => {
      const btn = document.createElement("button");
      btn.className = "ai-suggestion-chip";
      btn.textContent = text;
      btn.addEventListener("click", () => submitQuestion(text));
      container.appendChild(btn);
    });
  }

  // ── Open / close ────────────────────────────────────────────────────────
  function openPanel() {
    document.getElementById("ai-widget-panel").style.display = "flex";
    updateHeader();
    renderSuggestions();
    document.getElementById("ai-input").focus();
    // scroll to bottom of restored history
    const msgs = document.getElementById("ai-messages");
    msgs.scrollTop = msgs.scrollHeight;
  }

  function closePanel() {
    document.getElementById("ai-widget-panel").style.display = "none";
  }

  function doClearChat() {
    if (!confirm("Clear all chat history?")) return;
    clearHistory();
    const messages = document.getElementById("ai-messages");
    // Remove all bubbles except the welcome message
    Array.from(messages.children).forEach(el => {
      if (el.id !== "ai-welcome-msg") el.remove();
    });
  }

  function autoGrow(el) {
    el.style.height = "auto";
    el.style.height = Math.min(el.scrollHeight, 120) + "px";
  }

  // ── Init ────────────────────────────────────────────────────────────────
  function init() {
    buildWidget();
    updateHeader();
    renderSuggestions();
    restoreHistory();   // restore saved chat

    document.getElementById("ai-widget-trigger").addEventListener("click", openPanel);
    document.getElementById("ai-close-btn").addEventListener("click", closePanel);
    document.getElementById("ai-clear-btn").addEventListener("click", doClearChat);

    const inputEl = document.getElementById("ai-input");
    const sendBtn = document.getElementById("ai-send-btn");

    inputEl.addEventListener("input", () => autoGrow(inputEl));
    inputEl.addEventListener("keydown", (e) => {
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        submitQuestion(inputEl.value.trim());
      }
    });
    sendBtn.addEventListener("click", () => submitQuestion(inputEl.value.trim()));
    window.addEventListener("popstate", () => { updateHeader(); renderSuggestions(); });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
