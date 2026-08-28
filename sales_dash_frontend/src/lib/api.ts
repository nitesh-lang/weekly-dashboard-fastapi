export class ApiError extends Error {
  status: number;
  code?: string;
  detail?: unknown;
  constructor(status: number, message: string, code?: string, detail?: unknown) {
    super(message);
    this.status = status;
    this.code = code;
    this.detail = detail;
  }
}

/**
 * Base URL for API calls. In dev the Vite proxy forwards /api → :8000, so
 * this is empty. In prod (Render static site) it's set to the backend
 * service URL via VITE_API_BASE at build time.
 */
const API_BASE = (import.meta.env.VITE_API_BASE ?? "").replace(/\/$/, "");

/**
 * Build a full URL for API endpoints — required for browser-initiated
 * downloads via `window.open` or `<a href>`, since those don't route
 * through the fetch wrapper's API_BASE prefix. Passing a relative /api/…
 * URL to window.open hits the static-site origin (which SPA-rewrites to
 * index.html) instead of the API.
 */
export function apiUrl(path: string): string {
  if (path.startsWith("http")) return path;
  return `${API_BASE}${path.startsWith("/") ? "" : "/"}${path}`;
}

async function request<T>(
  path: string,
  init: RequestInit & { params?: Record<string, unknown> } = {}
): Promise<T> {
  const { params, ...rest } = init;
  let url = path.startsWith("http")
    ? path
    : `${API_BASE}${path.startsWith("/") ? "" : "/"}${path}`;
  if (params) {
    const search = new URLSearchParams();
    for (const [k, v] of Object.entries(params)) {
      if (v === undefined || v === null || v === "") continue;
      search.append(k, String(v));
    }
    const qs = search.toString();
    if (qs) url += (url.includes("?") ? "&" : "?") + qs;
  }
  const resp = await fetch(url, {
    ...rest,
    credentials: "include",
    headers: {
      ...(rest.body && !(rest.body instanceof FormData)
        ? { "Content-Type": "application/json" }
        : {}),
      ...(rest.headers ?? {}),
    },
  });
  if (!resp.ok) {
    let payload: unknown = null;
    try {
      payload = await resp.json();
    } catch {
      /* ignore */
    }
    const errObj = (payload as { error?: { code?: string; message?: string; detail?: unknown } })
      ?.error;
    throw new ApiError(
      resp.status,
      errObj?.message ?? `HTTP ${resp.status}`,
      errObj?.code,
      errObj?.detail
    );
  }
  const contentType = resp.headers.get("content-type") ?? "";
  if (contentType.includes("application/json")) return (await resp.json()) as T;
  return (await resp.text()) as unknown as T;
}

export const api = {
  get: <T>(path: string, params?: Record<string, unknown>) =>
    request<T>(path, { method: "GET", params }),
  post: <T>(path: string, body?: unknown, params?: Record<string, unknown>) =>
    request<T>(path, {
      method: "POST",
      body: body instanceof FormData ? body : body != null ? JSON.stringify(body) : undefined,
      params,
    }),
  patch: <T>(path: string, body?: unknown) =>
    request<T>(path, { method: "PATCH", body: body != null ? JSON.stringify(body) : undefined }),
  put: <T>(path: string, body?: unknown) =>
    request<T>(path, { method: "PUT", body: body != null ? JSON.stringify(body) : undefined }),
  delete: <T>(path: string) => request<T>(path, { method: "DELETE" }),
};
