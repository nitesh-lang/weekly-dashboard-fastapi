import { useBrand } from "@/store/useBrand";
import * as Select from "@radix-ui/react-select";
import { Check, ChevronDown } from "lucide-react";
import { cn } from "@/lib/utils";

export function BrandSwitcher() {
  const brand = useBrand((s) => s.brand);
  const brands = useBrand((s) => s.brands);
  const setBrand = useBrand((s) => s.setBrand);

  const current = brands.find((b) => b.key === brand);

  return (
    <div className="flex items-center gap-3">
      <span className="text-xs font-medium uppercase tracking-widest text-muted-foreground">
        Brand
      </span>
      <Select.Root value={brand} onValueChange={setBrand}>
        <Select.Trigger
          className={cn(
            "inline-flex h-9 min-w-[180px] items-center justify-between gap-2 rounded-md border border-[hsl(var(--hairline))] bg-white px-3 text-sm font-medium",
            "focus:outline-none focus:ring-2 focus:ring-[hsl(var(--primary))] focus:ring-offset-1"
          )}
        >
          <Select.Value>{current?.label ?? "Choose brand"}</Select.Value>
          <Select.Icon>
            <ChevronDown className="h-4 w-4 text-muted-foreground" />
          </Select.Icon>
        </Select.Trigger>
        <Select.Portal>
          <Select.Content
            className="z-50 overflow-hidden rounded-md border border-[hsl(var(--hairline))] bg-white shadow-lg"
            position="popper"
            sideOffset={4}
          >
            <Select.Viewport className="p-1">
              {brands.map((b) => (
                <Select.Item
                  key={b.key}
                  value={b.key}
                  className="relative flex cursor-pointer select-none items-center rounded-sm py-1.5 pl-8 pr-2 text-sm outline-none data-[highlighted]:bg-[hsl(var(--hairline-soft))]"
                >
                  <Select.ItemIndicator className="absolute left-2 inline-flex items-center">
                    <Check className="h-4 w-4" />
                  </Select.ItemIndicator>
                  <Select.ItemText>{b.label}</Select.ItemText>
                </Select.Item>
              ))}
            </Select.Viewport>
          </Select.Content>
        </Select.Portal>
      </Select.Root>
    </div>
  );
}
