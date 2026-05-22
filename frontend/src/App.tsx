import { lazy, Suspense } from "react";
import { Routes, Route, Navigate } from "react-router-dom";
import { AuthProvider, RequireAuth } from "./lib/auth";

// Login is small and the first thing an unauthenticated visitor hits — eager-load it.
import Login from "./pages/Login";

// Every other page lazy-loads its own chunk so the initial bundle stays small.
// Recharts (~300 KB) only ships when the user opens Dashboard / inventory etc.
const Dashboard          = lazy(() => import("./pages/Dashboard"));
const SalesTrend         = lazy(() => import("./pages/SalesTrend"));
const AmazonSalesTrend   = lazy(() => import("./pages/AmazonSalesTrend"));
const CategorySales      = lazy(() => import("./pages/CategorySales"));
const InventoryDashboard = lazy(() => import("./pages/InventoryDashboard"));
const AmsTrend           = lazy(() => import("./pages/AmsTrend"));
const AmsPoorPerformers  = lazy(() => import("./pages/AmsPoorPerformers"));
const AmsPlanning        = lazy(() => import("./pages/AmsPlanning"));
const NoSalesLastWeek    = lazy(() => import("./pages/NoSalesLastWeek"));
const DeadStock          = lazy(() => import("./pages/DeadStock"));
const MarginSnapshot     = lazy(() => import("./pages/MarginSnapshot"));
const Returns            = lazy(() => import("./pages/Returns"));
const Drilldown          = lazy(() => import("./pages/Drilldown"));

function Protected({ children }: { children: React.ReactNode }) {
    return <RequireAuth>{children}</RequireAuth>;
}

/** Inline skeleton shown while a route chunk downloads — should appear for
 *  ~100ms on first hit, instantly from cache afterwards. */
function PageFallback() {
    return (
        <div className="min-h-screen flex items-center justify-center">
            <div className="flex flex-col gap-3 items-center" style={{ color: "#6b7280" }}>
                <div
                    className="h-8 w-8 rounded-full border-2 border-transparent"
                    style={{
                        borderTopColor: "#1e40af",
                        borderRightColor: "#1e40af",
                        animation: "spin 0.8s linear infinite",
                    }}
                />
                <span className="text-[12px]" style={{ letterSpacing: "0.04em" }}>Loading…</span>
            </div>
            <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
        </div>
    );
}

export default function App() {
    return (
        <AuthProvider>
            <Suspense fallback={<PageFallback />}>
                <Routes>
                    <Route path="/login" element={<Login />} />
                    <Route path="/dashboard"           element={<Protected><Dashboard /></Protected>} />
                    <Route path="/sales-trend"         element={<Protected><SalesTrend /></Protected>} />
                    <Route path="/amazon-sales-trend"  element={<Protected><AmazonSalesTrend /></Protected>} />
                    <Route path="/category-sales"      element={<Protected><CategorySales /></Protected>} />
                    <Route path="/inventory-dashboard" element={<Protected><InventoryDashboard /></Protected>} />
                    <Route path="/ams-trend"           element={<Protected><AmsTrend /></Protected>} />
                    <Route path="/ams-poor-performers" element={<Protected><AmsPoorPerformers /></Protected>} />
                    <Route path="/ams-planning"        element={<Protected><AmsPlanning /></Protected>} />
                    <Route path="/no-sales-last-week"  element={<Protected><NoSalesLastWeek /></Protected>} />
                    <Route path="/dead-stock"          element={<Protected><DeadStock /></Protected>} />
                    <Route path="/margin-snapshot"     element={<Protected><MarginSnapshot /></Protected>} />
                    <Route path="/returns"             element={<Protected><Returns /></Protected>} />
                    <Route path="/drilldown"           element={<Protected><Drilldown /></Protected>} />
                    <Route path="*" element={<Navigate to="/dashboard" replace />} />
                </Routes>
            </Suspense>
        </AuthProvider>
    );
}
