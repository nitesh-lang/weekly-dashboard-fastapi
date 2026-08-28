/**
 * Usage tracking.
 *
 * Records what the signed-in user actually does — pages opened, reports
 * exported, and a heartbeat that lets time-in-tool be measured. The server
 * already records syncs and data fetches on its own; this covers the things
 * that never reach the server, chiefly the CSV exports that are built inside
 * the browser from data already on screen.
 *
 * Three things this deliberately does NOT do:
 *   - throw. A failed tracking call must never surface to the user.
 *   - block. Every call is fire-and-forget.
 *   - beat while the tab is hidden, so a dashboard left open in a background
 *     tab overnight does not report as eight hours of use.
 */
import { apiUrl } from "./api";

const KEY = "sd_session_id";

/** Per-tab session id, created on first use and reused for the tab's life. */
export function sessionId(): string {
  try {
    let id = sessionStorage.getItem(KEY);
    if (!id) {
      id =
        (crypto?.randomUUID?.() as string | undefined) ??
        `s_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
      sessionStorage.setItem(KEY, id);
    }
    return id;
  } catch {
    // Private browsing with storage disabled — fall back to a per-load id.
    return `s_nostore_${Math.random().toString(36).slice(2, 10)}`;
  }
}

export function newSession(): string {
  try {
    sessionStorage.removeItem(KEY);
  } catch {
    /* ignore */
  }
  return sessionId();
}

type Event = "page_view" | "export" | "heartbeat";

export function track(
  event: Event,
  opts: { page?: string; brand?: string; detail?: Record<string, unknown> } = {}
): void {
  try {
    void fetch(apiUrl("/api/activity/track"), {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      keepalive: true, // still sent if the tab is closing
      body: JSON.stringify({
        event,
        session_id: sessionId(),
        page: opts.page ?? null,
        brand: opts.brand ?? null,
        detail: opts.detail ?? null,
      }),
    }).catch(() => {
      /* tracking must never surface an error */
    });
  } catch {
    /* ignore */
  }
}

/**
 * Start the heartbeat. Returns a stop function.
 *
 * Beats every `intervalMs` but only while the tab is visible, so the figure
 * reflects time the tool was actually in front of someone.
 */
export function startHeartbeat(
  getBrand: () => string | undefined,
  intervalMs = 60_000
): () => void {
  const beat = () => {
    if (document.visibilityState === "visible") {
      track("heartbeat", { page: location.pathname, brand: getBrand() });
    }
  };
  beat();
  const timer = window.setInterval(beat, intervalMs);
  // A beat as the tab goes away closes off the session tail accurately.
  const onHide = () => {
    if (document.visibilityState === "hidden") {
      track("heartbeat", { page: location.pathname, brand: getBrand() });
    }
  };
  document.addEventListener("visibilitychange", onHide);
  return () => {
    window.clearInterval(timer);
    document.removeEventListener("visibilitychange", onHide);
  };
}
