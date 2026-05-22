import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import App from "./App";
import "./index.css";

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
