import type { ReactNode } from "react";
import type { LucideIcon } from "lucide-react";
import { cn } from "@/lib/utils";

interface Props {
    icon?: LucideIcon;
    /** Color identity for the icon chip — accent + tint derived from it. */
    iconColor?: string;
    title: ReactNode;
    /** Smaller grey subtitle text — e.g., "— 718 rows" or "Latest snapshot" */
    subtitle?: ReactNode;
    /** Right-aligned action slot (buttons, inputs, etc.) */
    action?: ReactNode;
    className?: string;
}

/**
 * Consistent section / card header used by every table card.  Replaces the
 * old emoji-prefix pattern (📦 Sales by SKU — N rows) with a proper
 * icon-chip + typography composition.
 */
export function SectionHeader({
    icon: Icon,
    iconColor = "#1e40af",
    title,
    subtitle,
    action,
    className,
}: Props) {
    return (
        <div className={cn("flex items-center justify-between border-b px-4 py-3 gap-3", className)}>
            <div className="flex items-center gap-2.5 min-w-0">
                {Icon && (
                    <span
                        className="inline-flex items-center justify-center shrink-0 h-7 w-7"
                        style={{
                            background: `linear-gradient(135deg, ${iconColor}18 0%, ${iconColor}08 100%)`,
                            border: `1px solid ${iconColor}33`,
                            borderRadius: 7,
                            color: iconColor,
                        }}
                    >
                        <Icon className="h-3.5 w-3.5" strokeWidth={2.2} />
                    </span>
                )}
                <div className="flex items-baseline gap-2 min-w-0">
                    <span className="text-[16px] font-semibold tracking-tight truncate" style={{ color: "#0a0a0a" }}>
                        {title}
                    </span>
                    {subtitle && (
                        <span className="text-[13px] truncate" style={{ color: "#6b7280" }}>
                            {subtitle}
                        </span>
                    )}
                </div>
            </div>
            {action && <div className="flex items-center gap-2 shrink-0">{action}</div>}
        </div>
    );
}
