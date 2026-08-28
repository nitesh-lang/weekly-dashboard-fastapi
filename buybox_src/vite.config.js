import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  // Env-driven base so one codebase serves both deployments:
  //   standalone static site      -> default "/"        (unchanged)
  //   embedded under Weekly       -> VITE_BASE=/buybox/ (assets sub-path)
  // Hash routing means no router changes are needed for the sub-path.
  base: process.env.VITE_BASE || '/',
  plugins: [react()]
})
