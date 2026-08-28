import * as React from "react";
import { cn } from "@/lib/utils";

type ToastVariant = "default" | "success" | "error";
interface ToastItem {
  id: number;
  title: string;
  description?: string;
  variant: ToastVariant;
}

const ToastCtx = React.createContext<{
  toast: (opts: { title: string; description?: string; variant?: ToastVariant }) => void;
}>({ toast: () => undefined });

export function useToast() {
  return React.useContext(ToastCtx);
}

export function ToastProvider({ children }: { children: React.ReactNode }) {
  const [items, setItems] = React.useState<ToastItem[]>([]);
  const nextId = React.useRef(1);

  const toast = React.useCallback(
    (opts: { title: string; description?: string; variant?: ToastVariant }) => {
      const id = nextId.current++;
      setItems((prev) => [
        ...prev,
        { id, title: opts.title, description: opts.description, variant: opts.variant ?? "default" },
      ]);
      window.setTimeout(() => {
        setItems((prev) => prev.filter((t) => t.id !== id));
      }, 4000);
    },
    []
  );

  return (
    <ToastCtx.Provider value={{ toast }}>
      {children}
      <div className="pointer-events-none fixed bottom-4 right-4 z-[100] flex w-96 max-w-[calc(100vw-2rem)] flex-col gap-2">
        {items.map((t) => (
          <div
            key={t.id}
            className={cn(
              "pointer-events-auto rounded-md border bg-background px-4 py-3 shadow-md",
              t.variant === "success" && "border-emerald-200 bg-emerald-50",
              t.variant === "error" && "border-red-200 bg-red-50"
            )}
          >
            <div className="text-sm font-semibold">{t.title}</div>
            {t.description && (
              <div className="mt-0.5 text-xs text-muted-foreground">{t.description}</div>
            )}
          </div>
        ))}
      </div>
    </ToastCtx.Provider>
  );
}
