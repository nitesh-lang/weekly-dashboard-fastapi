import { useEffect } from "react";
import { Navigate, Route, Routes, useLocation } from "react-router-dom";
import { useAuth } from "@/lib/auth";
import { AppLayout } from "@/components/AppLayout";
import { Login } from "@/pages/Login";
import { Dashboard } from "@/pages/Dashboard";
import { SyncPage } from "@/pages/SyncPage";
import { UploadPage } from "@/pages/UploadPage";
import { ResetPassword } from "@/pages/ResetPassword";
import { UsersPage } from "@/pages/UsersPage";
import { UsagePage } from "@/pages/UsagePage";

// Auth check without the must_reset_password redirect — the /reset-password
// page itself needs to render even when must_reset_password is true.
function RequireAuthPassthrough({ children }: { children: JSX.Element }) {
  const { me, loading } = useAuth();
  if (loading)
    return <div className="grid h-full min-h-screen place-items-center text-sm">Loading…</div>;
  if (!me?.user) return <Navigate to="/login" replace />;
  return children;
}

function RequireAuth({ children }: { children: JSX.Element }) {
  const { me, loading } = useAuth();
  const location = useLocation();
  if (loading) {
    return (
      <div className="grid h-full min-h-screen place-items-center text-sm text-muted-foreground">
        Loading…
      </div>
    );
  }
  if (!me?.user) return <Navigate to="/login" replace state={{ from: location }} />;
  if (me.user.must_reset_password && location.pathname !== "/reset-password") {
    return <Navigate to="/reset-password" replace />;
  }
  return children;
}

export default function App() {
  const { me, fetchMe } = useAuth();
  useEffect(() => {
    fetchMe();
  }, [fetchMe]);

  return (
    <Routes>
      <Route path="/login" element={me?.user ? <Navigate to="/" replace /> : <Login />} />
      <Route path="/reset-password" element={<RequireAuthPassthrough><ResetPassword /></RequireAuthPassthrough>} />
      <Route
        element={
          <RequireAuth>
            <AppLayout />
          </RequireAuth>
        }
      >
        <Route index element={<Dashboard />} />
        <Route path="/sync" element={<SyncPage />} />
        <Route path="/upload" element={<UploadPage />} />
        <Route path="/usage" element={<UsagePage />} />
        <Route path="/users" element={<UsersPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Route>
    </Routes>
  );
}
