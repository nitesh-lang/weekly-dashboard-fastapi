import { lazy, Suspense } from "react";
import { Routes, Route, Navigate } from "react-router-dom";
import { AuthProvider, RequireAuth, RequireAdmin, RequireTab } from "./lib/auth";

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
const AdminUsers         = lazy(() => import("./pages/AdminUsers"));
const Insights           = lazy(() => import("./pages/Insights"));
const Price              = lazy(() => import("./pages/Price"));
const VariationPerformance = lazy(() => import("./pages/VariationPerformance"));
const KeepaUpload          = lazy(() => import("./pages/KeepaUpload"));

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
                    <Route path="/dashboard"           element={<RequireAuth><RequireTab tab="/dashboard"><Dashboard /></RequireTab></RequireAuth>} />
                    <Route path="/insights"            element={<RequireAuth><RequireTab tab="/insights"><Insights /></RequireTab></RequireAuth>} />
                    <Route path="/sales-trend"         element={<RequireAuth><RequireTab tab="/sales-trend"><SalesTrend /></RequireTab></RequireAuth>} />
                    <Route path="/amazon-sales-trend"  element={<RequireAuth><RequireTab tab="/amazon-sales-trend"><AmazonSalesTrend /></RequireTab></RequireAuth>} />
                    <Route path="/category-sales"      element={<RequireAuth><RequireTab tab="/category-sales"><CategorySales /></RequireTab></RequireAuth>} />
                    <Route path="/inventory-dashboard" element={<RequireAuth><RequireTab tab="/inventory-dashboard"><InventoryDashboard /></RequireTab></RequireAuth>} />
                    <Route path="/ams-trend"           element={<RequireAuth><RequireTab tab="/ams-trend"><AmsTrend /></RequireTab></RequireAuth>} />
                    <Route path="/ams-poor-performers" element={<RequireAuth><RequireTab tab="/ams-poor-performers"><AmsPoorPerformers /></RequireTab></RequireAuth>} />
                    <Route path="/ams-planning"        element={<RequireAuth><RequireTab tab="/ams-planning"><AmsPlanning /></RequireTab></RequireAuth>} />
                    <Route path="/variation-performance" element={<RequireAuth><RequireTab tab="/variation-performance"><VariationPerformance /></RequireTab></RequireAuth>} />
                    <Route path="/keepa-upload"        element={<RequireAuth><RequireTab tab="/keepa-upload"><KeepaUpload /></RequireTab></RequireAuth>} />
                    <Route path="/no-sales-last-week"  element={<RequireAuth><RequireTab tab="/no-sales-last-week"><NoSalesLastWeek /></RequireTab></RequireAuth>} />
                    <Route path="/dead-stock"          element={<RequireAuth><RequireTab tab="/dead-stock"><DeadStock /></RequireTab></RequireAuth>} />
                    <Route path="/margin-snapshot"     element={<RequireAuth><RequireTab tab="/margin-snapshot"><MarginSnapshot /></RequireTab></RequireAuth>} />
                    <Route path="/price"               element={<RequireAuth><RequireTab tab="/price"><Price /></RequireTab></RequireAuth>} />
                    <Route path="/returns"             element={<RequireAuth><RequireTab tab="/returns"><Returns /></RequireTab></RequireAuth>} />
                    <Route path="/drilldown"           element={<RequireAuth><Drilldown /></RequireAuth>} />
                    <Route path="/admin/users"         element={<RequireAuth><RequireAdmin><AdminUsers /></RequireAdmin></RequireAuth>} />
                    <Route path="*" element={<Navigate to="/dashboard" replace />} />
                </Routes>
            </Suspense>
        </AuthProvider>
    );
}
