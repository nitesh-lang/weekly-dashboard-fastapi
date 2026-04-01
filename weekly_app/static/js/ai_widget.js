/**
 * AI Sales Assistant Widget — page-aware version.
 * Detects current page and sends it with every question
 * so the backend loads the right data (sales / AMS / inventory).
 */

(function () {
  "use strict";

  // ── Detect current page ─────────────────────────────────────────────────
  function getPageContext() {
    const path = window.location.pathname;
    if (path.includes("ams"))        return "ams-trend";
    if (path.includes("inventory"))  return "inventory";
    if (path.includes("sales-trend") || path.includes("amazon-sales")) return "sales-trend";
    return "dashboard";
  }

  // ── Read URL filters ────────────────────────────────────────────────────
  function getDashboardFilters() {
    const params = new URLSearchParams(window.location.search);
    return {
      week:  params.get("weeks") || "All Weeks",
      brand: params.get("brand") || "All",
      view:  params.get("view")  || "all",
      page:  getPageContext(),
    };
  }

  // ── Page-specific suggestions ───────────────────────────────────────────
  const SUGGESTIONS = {
    "ams-trend": [
      "Which model has the best ROAS in the last 4 weeks?",
      "Show me AMS spend vs attributed sales trend",
      "Which ASINs have ACOS above 50%?",
      "How has ad spend changed week over week?",
    ],
    "inventory": [
      "Which SKUs are at risk of going out of stock?",
      "What is the overall sell-through rate?",
      "Which models have the highest inventory value?",
      "Show me inventory health by category",
    ],
    "sales-trend": [
      "Which model is trending up in the last 4 weeks?",
      "What is the GMV trend week over week?",
      "Which SKU has the highest sales contribution?",
      "Compare this week vs last week sales",
    ],
    "dashboard": [
      "What is the total GMV this week?",
      "Which channel is driving the most revenue?",
      "Show me the top 5 SKUs by units sold",
      "What is the NLC vs GMV ratio?",
    ],
  };

  function getSuggestions() {
    return SUGGESTIONS[getPageContext()] || SUGGESTIONS["dashboard"];
  }

  // ── Page label for header ───────────────────────────────────────────────
  const PAGE_LABELS = {
    "ams-trend":   "AMS / Ads Intelligence",
    "inventory":   "Inventory Intelligence",
    "sales-trend": "Sales Trend Intelligence",
    "dashboard":   "Sales Intelligence",
  };

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
          <button id="ai-close-btn" aria-label="Close">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
          </button>
        </div>

        <div id="ai-messages" role="log" aria-live="polite">
          <div class="ai-msg ai-msg-bot">
            <p id="ai-welcome-text">Hi! Ask me anything about your data.</p>
            <p class="ai-suggestions-label">Try asking:</p>
            <div class="ai-suggestions" id="ai-suggestions"></div>
          </div>
        </div>

        <div id="ai-input-area">
          <textarea id="ai-input" placeholder="Ask about sales, AMS, SKUs..." rows="1" aria-label="Your question"></textarea>
          <button id="ai-send-btn" aria-label="Send">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg>
          </button>
        </div>
      </div>
    `;
    document.body.appendChild(wrapper);
  }

  // ── Render suggestions for current page ────────────────────────────────
  function renderSuggestions() {
    const container = document.getElementById("ai-suggestions");
    if (!container) return;
    container.innerHTML = "";
    getSuggestions().forEach((text) => {
      const btn = document.createElement("button");
      btn.className = "ai-suggestion-chip";
      btn.textContent = text;
      btn.addEventListener("click", () => submitQuestion(text));
      container.appendChild(btn);
    });
  }

  // ── Update header for current page ─────────────────────────────────────
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
    if (welcome) {
      const msgs = {
        "ams-trend":   "Hi! Ask me about ad spend, ROAS, ACOS, or model-level AMS trends.",
        "inventory":   "Hi! Ask me about stock levels, sell-through, or inventory risks.",
        "sales-trend": "Hi! Ask me about sales trends, model performance, or week-over-week changes.",
        "dashboard":   "Hi! Ask me about GMV, units, NLC, channels, or top SKUs.",
      };
      welcome.textContent = msgs[page] || msgs["dashboard"];
    }
  }

  // ── Append message bubble ───────────────────────────────────────────────
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

    appendMessage("user", question);
    inputEl.value = "";
    inputEl.style.height = "auto";
    sendBtn.disabled = true;

    const { bubble, p, dots } = appendStreamingBubble();

    try {
      const response = await fetch("/api/ai/ai-chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          question,
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
              p.innerHTML = fullText
                .replace(/\*\*(.*?)\*\*/g, "<strong>$1</strong>")
                .replace(/\n/g, "<br>");
              document.getElementById("ai-messages").scrollTop = 99999;
            }
          } catch (_) {}
        }
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

  // ── Toggle panel ────────────────────────────────────────────────────────
  function openPanel() {
    const panel = document.getElementById("ai-widget-panel");
    panel.style.display = "flex";
    updateHeader();
    renderSuggestions();
    document.getElementById("ai-input").focus();
  }

  function closePanel() {
    document.getElementById("ai-widget-panel").style.display = "none";
  }

  function autoGrow(el) {
    el.style.height = "auto";
    el.style.height = Math.min(el.scrollHeight, 100) + "px";
  }

  // ── Init ────────────────────────────────────────────────────────────────
  function init() {
    buildWidget();

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
    window.addEventListener("popstate", () => { updateHeader(); renderSuggestions(); });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
