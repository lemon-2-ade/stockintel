/// <reference types="vitest/config" />
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig, loadEnv } from "vite";

// In development the Vite server proxies API and WebSocket calls to the
// FastAPI backend, so the browser talks to one origin and CORS never applies.
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const api = env.VITE_API_PROXY_TARGET ?? "http://localhost:8000";
  return {
    plugins: [react(), tailwindcss()],
    server: {
      port: 5173,
      proxy: {
        "/api": { target: api, changeOrigin: true },
        "/ws": { target: api.replace(/^http/, "ws"), ws: true },
        "/ready": { target: api, changeOrigin: true },
      },
    },
    build: { sourcemap: true },
    test: {
      environment: "jsdom",
      setupFiles: ["./src/test/setup.ts"],
      css: false,
      restoreMocks: true,
    },
  };
});
