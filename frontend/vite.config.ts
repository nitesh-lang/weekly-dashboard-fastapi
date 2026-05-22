import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "path";

// Vite dev server proxies API + auth calls to FastAPI on :8000 so the
// browser sees one origin (no CORS / cookie issues during local dev).
// Build output goes to ../weekly_app/static/spa so FastAPI can serve it
// directly without copying files.
export default defineConfig({
    plugins: [react()],
    // Built bundle lives at weekly_app/static/spa and is served via the
    // FastAPI /static mount, so all asset URLs in the built HTML must be
    // prefixed with /static/spa/.  Dev server isn't affected (it uses the
    // root path because Vite serves from memory).
    base: process.env.NODE_ENV === "production" ? "/static/spa/" : "/",
    resolve: {
        alias: {
            "@": path.resolve(__dirname, "./src"),
        },
    },
    build: {
        outDir: "../weekly_app/static/spa",
        emptyOutDir: true,
    },
    server: {
        port: 5173,
        proxy: {
            // Everything that should hit FastAPI during dev
            "/api":      { target: "http://127.0.0.1:8000", changeOrigin: true },
            "/login":    { target: "http://127.0.0.1:8000", changeOrigin: true },
            "/logout":   { target: "http://127.0.0.1:8000", changeOrigin: true },
            "/static":   { target: "http://127.0.0.1:8000", changeOrigin: true },
        },
    },
});
