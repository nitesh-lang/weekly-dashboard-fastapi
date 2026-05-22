import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import App from "./App";
import "./index.css";

// ──────────────────────────────────────────────────────────────────────────
// Self-heal mid-deploy chunk races.
//
// Vite hashes each lazy-loaded chunk's filename.  When a new build deploys
// while a user's tab is open, the old main bundle in memory still
// references the OLD chunk URLs — and those files no longer exist on the
// server.  Any subsequent navigation that triggers a lazy import throws:
//   "TypeError: Failed to fetch dynamically imported module: …/Dashboard-XYZ.js"
//
// The recovery is dead simple: reload the page once.  That fetches a fresh
// index.html (which is `no-cache`), which references the new chunk hashes.
// The sessionStorage guard prevents infinite reload loops if the failure
// is real (e.g., network down) rather than a stale-cache race.
// ──────────────────────────────────────────────────────────────────────────
const CHUNK_RELOAD_FLAG = "chunk-reload-attempted";
function handleChunkLoadError(message: string) {
    if (!/Failed to fetch dynamically imported module|Importing a module script failed/.test(message)) return;
    if (sessionStorage.getItem(CHUNK_RELOAD_FLAG)) return;     // already tried — don't loop
    sessionStorage.setItem(CHUNK_RELOAD_FLAG, "1");
    // Clear the flag if the next load is successful — handled at the bottom.
    window.location.reload();
}
window.addEventListener("error", (e) => handleChunkLoadError(e.message || ""));
window.addEventListener("unhandledrejection", (e) =>
    handleChunkLoadError(String((e.reason && (e.reason.message || e.reason)) || ""))
);
// Page made it to here without dying → clear the guard so a future race
// gets a fresh single-shot reload chance.
setTimeout(() => sessionStorage.removeItem(CHUNK_RELOAD_FLAG), 5_000);

// Weekly data — staleness window of 5 minutes is plenty.  Tab switches
// inside that window hit the cache and feel instant; background refetch
// happens silently when the operator triggers a query that's gone stale.
const queryClient = new QueryClient({
    defaultOptions: {
        queries: {
            refetchOnWindowFocus: false,
            refetchOnReconnect: false,
            staleTime: 5 * 60_000,
            gcTime: 30 * 60_000,
            retry: 1,
        },
    },
});

ReactDOM.createRoot(document.getElementById("root")!).render(
    <React.StrictMode>
        <BrowserRouter>
            <QueryClientProvider client={queryClient}>
                <App />
            </QueryClientProvider>
        </BrowserRouter>
    </React.StrictMode>
);
