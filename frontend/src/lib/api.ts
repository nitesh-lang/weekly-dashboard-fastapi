/** Thin fetch wrapper.
 *  - Always sends the session cookie ("credentials: include")
 *  - Throws ApiError on non-2xx so React Query exposes it
 *  - Treats 401/302→/login as "unauthenticated" so RequireAuth can redirect
 */

export class ApiError extends Error {
    status: number;
    body: unknown;
    constructor(status: number, message: string, body?: unknown) {
        super(message);
        this.status = status;
        this.body = body;
    }
}

const BASE = ""; // relative — Vite proxies in dev, same-origin in prod

async function request<T = unknown>(
    path: string,
    init: RequestInit = {}
): Promise<T> {
    const res = await fetch(BASE + path, {
        credentials: "include",
        ...init,
        headers: {
            Accept: "application/json",
            ...(init.body && !(init.body instanceof FormData)
                ? { "Content-Type": "application/json" }
                : {}),
            ...(init.headers || {}),
        },
        redirect: "manual",
    });

    // FastAPI's AuthGuardMiddleware returns 303 → /login when unauth.
    // "redirect: manual" gives us a status 0 (opaqueredirect) for those,
    // so we treat anything that's not 2xx as unauth/error.
    if (res.type === "opaqueredirect" || res.status === 401 || res.status === 303) {
        throw new ApiError(401, "Unauthorized");
    }

    if (!res.ok) {
        let body: unknown = null;
        try { body = await res.json(); } catch { /* not json */ }
        throw new ApiError(res.status, `${res.status} ${res.statusText}`, body);
    }
    const ct = res.headers.get("content-type") || "";
    if (ct.includes("application/json")) return (await res.json()) as T;
    return (await res.text()) as unknown as T;
}

export const api = {
    get:  <T = unknown>(p: string) => request<T>(p),
    post: <T = unknown>(p: string, body?: unknown) =>
        request<T>(p, {
            method: "POST",
            body: body instanceof FormData ? body : body == null ? undefined : JSON.stringify(body),
        }),
    patch: <T = unknown>(p: string, body?: unknown) =>
        request<T>(p, {
            method: "PATCH",
            body: body == null ? undefined : JSON.stringify(body),
        }),
    delete: <T = unknown>(p: string) =>
        request<T>(p, { method: "DELETE" }),
};
