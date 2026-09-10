import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// Dev proxy → the FastAPI console server (uvicorn on :8080).
// In production the same server serves this app's build from /.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": "http://127.0.0.1:8080",
      "/chat": "http://127.0.0.1:8080",
      "/approve": "http://127.0.0.1:8080",
      "/health": "http://127.0.0.1:8080",
    },
  },
});
