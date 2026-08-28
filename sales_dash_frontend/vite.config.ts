import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "node:path";

export default defineConfig({
  // Env-driven base so ONE codebase serves both deployments:
  //   standalone static site  → default "/"           (unchanged)
  //   mounted under Weekly    → VITE_BASE=/sales/     (assets + router base)
  base: process.env.VITE_BASE || "/",
  plugins: [react()],
  resolve: {
    alias: { "@": path.resolve(__dirname, "./src") },
  },
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: process.env.VITE_API_BASE || "http://127.0.0.1:8001",
        changeOrigin: true,
      },
    },
  },
});
