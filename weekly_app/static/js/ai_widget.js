/**
 * AI Sales Assistant Widget
 * Floating chat bubble for the Weekly Unified Dashboard.
 * Reads the current week/brand/view from the URL and page,
 * then streams answers from /api/ai-chat.
 */

(function () {
  "use strict";

  // ── Read current dashboard filters from the URL ─────────────────────────
  function getDashboardFilters() {
    const params = new URLSearchParams(window.location.search);
    return {
      week: params.get("weeks") || "Week 13",
      brand: params.get("brand") || "All",
      view: params.get("view") || "all",
    };
  }

  // ── Suggested questions ─────────────────────────────────────────────────
  const SUGGESTIONS = [
    "What is the total GMV this week?",
    "Which SKU has the highest sales contribution?",
    "How is AMS performing vs total GMV?",
    "Show me the top 5 SKUs by units sold",
    "What is the NLC this week?",
    "Which channel is driving the most revenue?",
    "Are there any zero-sales SKUs I should know about?",
    "How does this week compare to last week?",
  ];

  // ── Build widget HTML ───────────────────────────────────────────────────
  function buildWidget() {
    const wrapper = document.createElement("div");
    wrapper.id = "ai-sales-widget";
    wrapper.innerHTML = `
      <button id="ai-widget-trigger" title="Ask AI about your sales data" aria-label="Open AI Sales Assistant">
        <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
          <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>
        </svg>
        <span class="ai-trigger-label">Ask AI</span>
      </button>

      <div id="ai-widget-panel" role="dialog" aria-label="AI Sales Assistant" style="display:none;">
        <div id="ai-panel-header">
          <div class="ai-header-info">
            <div class="ai-avatar">AI</div>
            <div>
              <div class="ai-title">Sales Assistant</div>
              <div class="ai-subtitle" id="ai-filter-badge">Loading...</div>
            </div>
          </div>
          <button id="ai-close-btn" aria-label="Close">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
          </button>
        </div>

        <div id="ai-messages" role="log" aria-live="polite">
          <div class="ai-msg ai-msg-bot">
            <p>Hi! I can answer questions about your sales, AMS performance, and SKU data for the current week and brand filter.</p>
            <p class="ai-suggestions-label">Try asking:</p>
            <div class="ai-suggestions" id="ai-suggestions"></div>
          </div>
        </div>

        <div id="ai-input-area">
          <textarea
            id="ai-input"
            placeholder="Ask about sales, AMS, SKUs..."
            rows="1"
            aria-label="Your question"
          ></textarea>
          <button id="ai-send-btn" aria-label="Send question">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg>
          </button>
        </div>
      </div>
    `;
    document.body.appendChild(wrapper);
  }

  // ── Render suggestion chips ─────────────────────────────────────────────
  function renderSuggestions() {
    const container = document.getElementById("ai-suggestions");
    if (!container) return;
    SUGGESTIONS.slice(0, 4).forEach((text) => {
      const btn = document.createElement("button");
      btn.className = "ai-suggestion-chip";
      btn.textContent = text;
      btn.addEventListener("click", () => submitQuestion(text));
      container.appendChild(btn);
    });
  }

  // ── Update filter badge ─────────────────────────────────────────────────
  function updateFilterBadge() {
    const badge = document.getElementById("ai-filter-badge");
    if (!badge) return;
    const f = getDashboardFilters();
    badge.textContent = `${f.week} · ${f.brand === "All" ? "All Brands" : f.brand}`;
  }

  // ── Append a message bubble ─────────────────────────────────────────────
  function appendMessage(role, text) {
    const messages = document.getElementById("ai-messages");
    const div = document.createElement("div");
    div.className = `ai-msg ai-msg-${role}`;
    if (role === "bot") {
      div.innerHTML = `<p>${text}</p>`;
    } else {
      div.textContent = text;
    }
    messages.appendChild(div);
    messages.scrollTop = messages.scrollHeight;
    return div;
  }

  // ── Streaming: append bot bubble and stream into it ─────────────────────
  function appendStreamingBubble() {
    const messages = document.getElementById("ai-messages");
    const div = document.createElement("div");
    div.className = "ai-msg ai-msg-bot ai-streaming";
    const p = document.createElement("p");
    p.textContent = "";
    div.appendChild(p);
    // Loading dots
    const dots = document.createElement("span");
    dots.className = "ai-typing-dots";
    dots.innerHTML = "<span></span><span></span><span></span>";
    div.appendChild(dots);
    messages.appendChild(div);
    messages.scrollTop = messages.scrollHeight;
    return { bubble: div, p, dots };
  }

  // ── Send question to /api/ai-chat ───────────────────────────────────────
  async function submitQuestion(question) {
    if (!question.trim()) return;

    const sendBtn = document.getElementById("ai-send-btn");
    const inputEl = document.getElementById("ai-input");
    const filters = getDashboardFilters();

    // Show user message
    appendMessage("user", question);
    inputEl.value = "";
    inputEl.style.height = "auto";
    sendBtn.disabled = true;

    // Show streaming bubble
    const { bubble, p, dots } = appendStreamingBubble();

    try {
      const response = await fetch("/api/ai-chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          question,
          week: filters.week,
          brand: filters.brand,
          view: filters.view,
        }),
      });

      if (!response.ok) throw new Error(`Server error: ${response.status}`);

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      let fullText = "";

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
              // Simple markdown: bold, newlines
              p.innerHTML = fullText
                .replace(/\*\*(.*?)\*\*/g, "<strong>$1</strong>")
                .replace(/\n/g, "<br>");
              document.getElementById("ai-messages").scrollTop = 99999;
            }
          } catch (_) {}
        }
      }
    } catch (err) {
      p.textContent = "Sorry, something went wrong. Please try again.";
      bubble.classList.add("ai-msg-error");
    } finally {
      dots.remove();
      bubble.classList.remove("ai-streaming");
      sendBtn.disabled = false;
      inputEl.focus();
    }
  }

  // ── Toggle panel ────────────────────────────────────────────────────────
  function openPanel() {
    const panel = document.getElementById("ai-widget-panel");
    panel.style.display = "flex";
    updateFilterBadge();
    document.getElementById("ai-input").focus();
  }

  function closePanel() {
    document.getElementById("ai-widget-panel").style.display = "none";
  }

  // ── Auto-grow textarea ──────────────────────────────────────────────────
  function autoGrow(el) {
    el.style.height = "auto";
    el.style.height = Math.min(el.scrollHeight, 100) + "px";
  }

  // ── Init ────────────────────────────────────────────────────────────────
  function init() {
    buildWidget();
    renderSuggestions();

    document.getElementById("ai-widget-trigger").addEventListener("click", openPanel);
    document.getElementById("ai-close-btn").addEventListener("click", closePanel);

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

    // Update badge when URL changes (SPA navigation)
    window.addEventListener("popstate", updateFilterBadge);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
