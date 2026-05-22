# Weekly Brief — Deploy Guide

A single FastAPI service that serves both the React SPA and the JSON API.
Same repo, same Render service, same domain.

## Architecture

```
Browser
   │
   ▼
Render (single web service, uvicorn)
   ├── /                         → 303 /dashboard
   ├── /dashboard                → SPA shell (React Router takes over)
   ├── /sales-trend              → SPA shell
   ├── /amazon-sales-trend       → SPA shell
   ├── /category-sales           → SPA shell
   ├── /inventory-dashboard      → SPA shell
   ├── /ams-trend                → SPA shell
   ├── /ams-poor-performers      → SPA shell
   ├── /no-sales-last-week       → SPA shell
   ├── /dead-stock               → SPA shell
   ├── /drilldown                → SPA shell
   ├── /login                    → SPA shell (React Login page)
   ├── /api/*                    → JSON endpoints
   ├── /static/*                 → SPA bundle assets + legacy /static files
   ├── /upload                   → Jinja (kept; auth-gated)
   ├── /viewer/*                 → Jinja viewers (kept; auth-gated)
   ├── /analytics                → Jinja Claude page (kept; auth-gated)
   └── /health, /ping            → public health checks
```

## Local dev

Two terminals from repo root (`FastAPI/`):

```bash
# Terminal 1 — FastAPI
PYTHONIOENCODING=utf-8 uvicorn weekly_app.main:app --port 8000

# Terminal 2 — Vite dev server (live reload)
cd frontend
npm run dev        # serves at http://localhost:5173, proxies /api → :8000
```

Open `http://localhost:5173/` in a browser.

## Local production smoke test

```bash
# 1. Build the SPA bundle
cd frontend
npm ci
npm run build                # writes to ../weekly_app/static/spa/

# 2. Boot uvicorn (no Vite)
cd ..
PYTHONIOENCODING=utf-8 uvicorn weekly_app.main:app --port 8000

# 3. Visit http://localhost:8000/ — should serve the SPA shell
```

If `npm run build` fails due to Google-Drive `EBADF`, use the helper:
```powershell
pwsh scripts/build-spa-local.ps1
```
This builds from `C:\Users\Admin\weekly-frontend\` (local-disk mirror with
node_modules) and copies the bundle into `weekly_app/static/spa/`.

## Render deploy

`render.yaml` is committed at repo root. Two paths to use it:

### Option A — Fresh Blueprint service (clean)
1. Push `render.yaml` + all the route changes to your GitHub branch.
2. Render Dashboard → **New** → **Blueprint** → select the repo.
3. Render reads `render.yaml` and proposes a service named `weekly-dashboard`.
4. Set the `SESSION_SECRET` env var when prompted (it's marked `sync: false`).
5. Deploy.

### Option B — Update existing service via Render UI
1. Push the route changes (with or without `render.yaml`).
2. In Render Dashboard → your existing service → **Settings**.
3. **Build Command**:
   ```
   npm --prefix frontend ci && npm --prefix frontend run build && pip install -r requirements.txt
   ```
4. **Start Command** (unchanged):
   ```
   uvicorn weekly_app.main:app --host 0.0.0.0 --port $PORT
   ```
5. Env vars:
   - `SESSION_SECRET` — long random string (you should already have this)
   - `COOKIE_SECURE=1` — secure cookies over HTTPS
6. Trigger a manual redeploy.

## Required env vars on Render

| Var | Required | Notes |
|---|---|---|
| `SESSION_SECRET` | ✅ | 64+ random chars. Cookie session signing. |
| `COOKIE_SECURE` | ✅ | `1` in prod (HTTPS). `0` for local. |
| `NODE_VERSION` | optional | `20` (defaulted by render.yaml). |
| `PYTHON_VERSION` | optional | `3.11` (defaulted by render.yaml). |
| `ANTHROPIC_API_KEY` | conditional | Only if AI chat is enabled. |

## What changed (since the pure-Jinja version)

| File | Change |
|---|---|
| `weekly_app/main.py` | Added SPA catch-all at the end (serves index.html). Dropped `/ams-trend` redirect alias. |
| `weekly_app/middleware/auth_guard.py` | Browser navs to SPA paths now pass through (React handles auth). Only `/api/*` and listed Jinja paths still 401/303 on unauth. |
| `weekly_app/routes/{dashboard,sales_trend,AM_sales_trend,category_sales,inventory_dashboard,drilldown,ams_trend}.py` | Removed the Jinja HTML `@router.get(...)` decorators. JSON aliases via `add_api_route(...)` are unchanged. |
| `weekly_app/routes/auth.py` | Removed Jinja `GET /login` decorator (SPA owns it). `POST /login` + `/api/login` etc. unchanged. |
| `frontend/vite.config.ts` | `base: "/static/spa/"` in production so asset URLs work. |
| `render.yaml` | New — declarative build + start config. |
| `scripts/build-spa-local.ps1` | Helper for the Google-Drive workaround. |

## Smoke-test checklist (post-deploy)

After Render finishes building, hit the production URL and verify:

- [ ] `/` redirects → `/dashboard`
- [ ] `/dashboard` loads the React SPA, then React calls `/api/me` → 401 → navigates to `/login`
- [ ] `/login` shows the React login form
- [ ] Login with correct creds → lands back on `/dashboard` with data
- [ ] Sidebar nav reaches all 9 tabs: Dashboard, Sales Trend, Amazon + 1P, Category, Inventory, AMS Trend, Ad Underperformers, No Sales, Dead Stock
- [ ] Each page renders a KPI strip + table with data
- [ ] Table sort, filter input, CSV export work
- [ ] Drilldown link from Dashboard SKU table → `/drilldown?...` shows SKU rows
- [ ] Logout → back to `/login`
- [ ] `/health` returns `{"status":"healthy"}`
- [ ] `/ping` returns `{"status":"app running"}` (UptimeRobot anti-coldstart)
- [ ] Legacy: `/upload`, `/viewer/sales`, `/analytics` still render Jinja and require auth

## Rollback

If anything breaks in production:
1. Re-add the Jinja `@router.get(...)` decorators in the route files (one line per file, see git diff).
2. Re-add the original `AuthGuardMiddleware` logic (also a small diff).
3. The SPA catch-all in `main.py` is harmless — Jinja routes take precedence when registered.
4. No data migrations involved.

## Known issues / nice-to-have

- The SPA bundle is **891 KB** (gzipped 249 KB). Acceptable for an internal tool, but if you ever want to ship to public traffic, code-split per-route via `React.lazy(import('./pages/SalesTrend'))`.
- Recharts is most of the weight; could swap for a lighter chart lib if needed.
- The AI chat widget (`/static/js/ai_widget.js`) was bundled with the Jinja
  dashboard. It's still loaded by the legacy templates but isn't wired into
  the React app yet — that's a separate scoped task.
