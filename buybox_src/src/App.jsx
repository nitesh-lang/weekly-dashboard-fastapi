import React, { Suspense, lazy, useEffect, useState } from "react";
import {
  UnlockError,
  base64ToBytes,
  bytesToBase64,
  lock,
  unlockWithKey,
  unlockWithPassword,
} from "./secureData";

// The dashboard is loaded on demand, never at startup. That is deliberate: its
// module-scope constants read the decrypted datasets, so the module must not be
// evaluated until after a successful sign-in.
const Dashboard = lazy(() => import("./nexlev_ads_dashboard"));

// We keep the PBKDF2-derived key (not the password) so a refresh does not force
// a re-login. It cannot be reversed into the password, and nothing here reveals
// the credentials in DevTools.
//
// It lives in localStorage, NOT sessionStorage: the dashboard opens the BSR
// tracker, ASIN and model pages with window.open(..., "_blank"), and a new tab
// starts with an empty sessionStorage — which forced a second sign-in every
// time. localStorage is shared across tabs, so one sign-in now covers them all.
// The trade-off is that the key survives on disk, so it carries an expiry that
// is renewed on each use: an idle browser locks itself after a week.
const SESSION_KEY = "buybox-session-key";
const SESSION_TTL_MS = 7 * 24 * 60 * 60 * 1000;
// Left over from the old hardcoded-password gate; removed on sight.
const LEGACY_AUTH_KEY = "buybox-authenticated";

function readStoredKey() {
  try {
    // Anything written by the old per-tab build is migrated on first read.
    const legacyTabKey = window.sessionStorage.getItem(SESSION_KEY);
    const raw = window.localStorage.getItem(SESSION_KEY);
    if (!raw) return legacyTabKey || null;

    const parsed = JSON.parse(raw);
    if (typeof parsed?.key !== "string" || typeof parsed?.expiresAt !== "number") return null;
    if (Date.now() > parsed.expiresAt) {
      window.localStorage.removeItem(SESSION_KEY);
      return null;
    }
    return parsed.key;
  } catch {
    // Corrupt or unreadable entry: drop it and fall back to the login screen.
    window.localStorage.removeItem(SESSION_KEY);
    return null;
  }
}

function writeStoredKey(encodedKey) {
  window.sessionStorage.removeItem(SESSION_KEY);
  window.localStorage.setItem(
    SESSION_KEY,
    JSON.stringify({ key: encodedKey, expiresAt: Date.now() + SESSION_TTL_MS })
  );
}

function clearStoredKey() {
  window.localStorage.removeItem(SESSION_KEY);
  window.sessionStorage.removeItem(SESSION_KEY);
}

const HELP_ITEMS = [
  {
    question: "Questions about login access?",
    answer: "Contact your admin or team lead to confirm your username and password.",
  },
  {
    question: "Dashboard not opening after sign in?",
    answer: "Refresh once and try again. If it still fails, ask the ops team to verify access.",
  },
  {
    question: "Need a new report or brand added?",
    answer: "Share the brand name and report type with the analytics team for setup.",
  },
];

function Splash({ message }) {
  return (
    <div
      style={{
        minHeight: "100vh",
        background:
          "radial-gradient(circle at top left, rgba(59,130,246,0.22), transparent 28%), linear-gradient(135deg, #050816 0%, #0B1024 48%, #111827 100%)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        color: "#E5EEF8",
        fontFamily: "'Inter', 'Segoe UI', sans-serif",
        fontSize: 15,
        letterSpacing: 0.4,
      }}
    >
      {message}
    </div>
  );
}

function LoginScreen({ onLogin }) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [activePill, setActivePill] = useState("Live Data");
  // Collapsed by default so the card fits a laptop screen without scrolling.
  const [helpOpen, setHelpOpen] = useState(false);

  const handleSubmit = async (event) => {
    event.preventDefault();
    if (busy) return;

    if (!username.trim() || !password.trim()) {
      setError("Enter username and password.");
      return;
    }

    setBusy(true);
    setError("");

    try {
      const { payload, keyBytes } = await unlockWithPassword(password);

      // The expected email lives inside the encrypted payload, so it is only
      // knowable once the password has already checked out.
      if (username.trim().toLowerCase() !== String(payload.username || "").toLowerCase()) {
        lock();
        setError("Invalid username or password. Contact your admin if you need access.");
        return;
      }

      onLogin(keyBytes);
    } catch (err) {
      setError(
        err instanceof UnlockError
          ? err.message
          : "Something went wrong signing in. Refresh and try again."
      );
    } finally {
      setBusy(false);
    }
  };

  const fieldStyle = {
    width: "100%",
    height: 40,
    borderRadius: 10,
    border: "1px solid #D7DFEA",
    padding: "0 12px",
    fontSize: 13,
    color: "#0F172A",
    outline: "none",
    boxSizing: "border-box",
  };

  const labelStyle = {
    display: "block",
    fontSize: 10,
    fontWeight: 700,
    letterSpacing: 1,
    marginBottom: 5,
  };

  return (
    <div
      style={{
        minHeight: "100vh",
        background:
          "radial-gradient(circle at top left, rgba(59,130,246,0.22), transparent 28%), linear-gradient(135deg, #050816 0%, #0B1024 48%, #111827 100%)",
        padding: "20px 16px",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        color: "#E5EEF8",
        fontFamily: "'Inter', 'Segoe UI', sans-serif",
      }}
    >
      <div style={{ width: "100%", maxWidth: 360 }}>
        <div
          style={{
            borderRadius: 16,
            overflow: "hidden",
            background: "#FFFFFF",
            boxShadow: "0 20px 56px rgba(15,23,42,0.42)",
          }}
        >
          <div
            style={{
              padding: "16px 20px 14px",
              background: "linear-gradient(135deg, #1E3A8A 0%, #2563EB 100%)",
              color: "#FFFFFF",
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
              <div
                style={{
                  width: 32,
                  height: 32,
                  borderRadius: 10,
                  background: "rgba(255,255,255,0.14)",
                  border: "1px solid rgba(255,255,255,0.18)",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  fontWeight: 800,
                  fontSize: 16,
                  flexShrink: 0,
                }}
              >
                B
              </div>
              <div>
                <div style={{ fontSize: 15, fontWeight: 700, letterSpacing: -0.3 }}>BrandIQ Hub</div>
                <div style={{ fontSize: 8, opacity: 0.75, letterSpacing: 1.2, marginTop: 2 }}>
                  AMAZON PERFORMANCE INTELLIGENCE
                </div>
              </div>
            </div>

            <div style={{ marginTop: 12, fontSize: 20, fontWeight: 800, letterSpacing: -0.6 }}>
              Welcome back
            </div>
            <div style={{ marginTop: 2, fontSize: 12, color: "rgba(255,255,255,0.8)" }}>
              Sign in to access your dashboard
            </div>

            <div style={{ display: "flex", gap: 6, flexWrap: "wrap", marginTop: 11 }}>
              {["Live Data", "Buy Box Analytics", "BSR Tracker"].map((item, index) => (
                <button
                  type="button"
                  key={item}
                  onClick={() => setActivePill(item)}
                  style={{
                    padding: "4px 9px",
                    borderRadius: 999,
                    fontSize: 9.5,
                    fontWeight: 600,
                    border: "1px solid rgba(255,255,255,0.2)",
                    background:
                      activePill === item
                        ? index === 0
                          ? "rgba(34,197,94,0.18)"
                          : "rgba(255,255,255,0.18)"
                        : "rgba(255,255,255,0.08)",
                    color: index === 0 && activePill === item ? "#B7F7CD" : "#E5EEFF",
                    cursor: "pointer",
                    outline: "none",
                  }}
                >
                  {index === 0 ? "• " : ""}
                  {item}
                </button>
              ))}
            </div>
          </div>

          <div style={{ padding: "16px 20px 18px", color: "#0F172A" }}>
            <form onSubmit={handleSubmit}>
              <label style={labelStyle}>USERNAME</label>
              <input
                value={username}
                onChange={(event) => setUsername(event.target.value)}
                placeholder="Enter your username"
                disabled={busy}
                style={{ ...fieldStyle, marginBottom: 11 }}
              />

              <label style={labelStyle}>PASSWORD</label>
              <input
                type="password"
                value={password}
                onChange={(event) => setPassword(event.target.value)}
                placeholder="Enter your password"
                disabled={busy}
                style={fieldStyle}
              />

              {error ? (
                <div style={{ marginTop: 8, color: "#DC2626", fontSize: 11.5, fontWeight: 600 }}>{error}</div>
              ) : null}

              <button
                type="submit"
                disabled={busy}
                style={{
                  width: "100%",
                  marginTop: 13,
                  height: 42,
                  borderRadius: 10,
                  border: "none",
                  background: busy
                    ? "linear-gradient(135deg, #64748B 0%, #94A3B8 100%)"
                    : "linear-gradient(135deg, #2563EB 0%, #3B82F6 100%)",
                  color: "#FFFFFF",
                  fontSize: 14,
                  fontWeight: 700,
                  cursor: busy ? "wait" : "pointer",
                  boxShadow: "0 10px 18px rgba(37,99,235,0.22)",
                }}
              >
                {busy ? "Unlocking…" : "Sign In →"}
              </button>
            </form>

            <div style={{ marginTop: 13, display: "flex", alignItems: "center", gap: 10 }}>
              <div style={{ flex: 1, height: 1, background: "#E2E8F0" }} />
              <div style={{ fontSize: 9, color: "#94A3B8", letterSpacing: 1.6 }}>CAMBIUM RETAIL</div>
              <div style={{ flex: 1, height: 1, background: "#E2E8F0" }} />
            </div>

            <div style={{ marginTop: 9, textAlign: "center", color: "#94A3B8", fontSize: 11, lineHeight: 1.45 }}>
              Restricted access. Authorised team members only.
            </div>

            <button
              type="button"
              onClick={() => setHelpOpen((open) => !open)}
              style={{
                width: "100%",
                marginTop: 11,
                padding: "8px 11px",
                borderRadius: 10,
                border: "1px solid #E2E8F0",
                background: "#F8FAFC",
                color: "#475569",
                fontSize: 11.5,
                fontWeight: 700,
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                cursor: "pointer",
                outline: "none",
              }}
            >
              <span>Questions? Quick help</span>
              <span style={{ fontSize: 9, color: "#94A3B8" }}>{helpOpen ? "▲" : "▼"}</span>
            </button>

            {helpOpen ? (
              <div style={{ display: "grid", gap: 7, marginTop: 7 }}>
                {HELP_ITEMS.map((item) => (
                  <div
                    key={item.question}
                    style={{
                      borderRadius: 9,
                      background: "#FFFFFF",
                      border: "1px solid #E2E8F0",
                      padding: "9px 11px",
                    }}
                  >
                    <div style={{ fontSize: 11.5, fontWeight: 700, color: "#1E293B", marginBottom: 3 }}>
                      {item.question}
                    </div>
                    <div style={{ fontSize: 11, lineHeight: 1.45, color: "#64748B" }}>{item.answer}</div>
                  </div>
                ))}
              </div>
            ) : null}
          </div>
        </div>
      </div>
    </div>
  );
}

export default function App() {
  // "checking" -> trying to restore a session, "locked" -> login, "unlocked" -> dashboard.
  const [status, setStatus] = useState("checking");

  useEffect(() => {
    let cancelled = false;

    // The old gate stored a plain "authenticated" flag; it grants nothing now.
    window.localStorage.removeItem(LEGACY_AUTH_KEY);

    // Single sign-on: the server only serves /buybox to a logged-in Weekly
    // user, and /api/buybox-sso returns the unlock credentials to that same
    // session — so normally nobody ever sees the login form.  Any failure
    // (endpoint missing, password mismatch, network) falls back to it.
    const trySso = async () => {
      try {
        const r = await fetch("/api/buybox-sso", { credentials: "same-origin" });
        if (!r.ok) throw new UnlockError("no sso");
        const { password } = await r.json();
        const { keyBytes } = await unlockWithPassword(password);
        writeStoredKey(bytesToBase64(keyBytes));
        if (!cancelled) setStatus("unlocked");
      } catch {
        if (!cancelled) setStatus("locked");
      }
    };

    const stored = readStoredKey();
    if (!stored) {
      trySso();
      return () => {
        cancelled = true;
      };
    }

    (async () => {
      try {
        await unlockWithKey(base64ToBytes(stored));
        // Sliding expiry: someone who uses the dashboard never gets logged out.
        writeStoredKey(stored);
        if (!cancelled) setStatus("unlocked");
      } catch {
        clearStoredKey();
        await trySso();
      }
    })();

    return () => {
      cancelled = true;
    };
  }, []);

  const handleLogin = (keyBytes) => {
    writeStoredKey(bytesToBase64(keyBytes));
    setStatus("unlocked");
  };

  const handleLogout = () => {
    clearStoredKey();
    lock();
    setStatus("locked");
    // Datasets live in module state; a reload guarantees nothing is left behind.
    window.location.reload();
  };

  if (status === "checking") {
    return <Splash message="Unlocking dashboard…" />;
  }

  if (status === "locked") {
    return <LoginScreen onLogin={handleLogin} />;
  }

  return (
    <Suspense fallback={<Splash message="Loading dashboard…" />}>
      <Dashboard onLogout={handleLogout} />
    </Suspense>
  );
}
