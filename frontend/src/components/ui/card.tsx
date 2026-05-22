import { forwardRef, type HTMLAttributes } from "react";
import { cn } from "@/lib/utils";

export const Card = forwardRef<HTMLDivElement, HTMLAttributes<HTMLDivElement>>(
    ({ className, ...props }, ref) => (
        <div
            ref={ref}
            className={cn(
                "relative rounded-lg border bg-card text-card-foreground overflow-hidden",
                // 3px gradient top stripe (oxblood→deep oxblood) with a subtle glow halo
                "before:absolute before:inset-x-0 before:top-0 before:h-[3px] before:rounded-t-lg",
                "before:bg-[linear-gradient(90deg,#1e40af_0%,#3b82f6_50%,#1e40af_100%)]",
                "before:shadow-[0_0_8px_rgba(30,64,175,0.35)]",
                className,
            )}
            {...props}
        />
    )
);
Card.displayName = "Card";

export const CardHeader = forwardRef<HTMLDivElement, HTMLAttributes<HTMLDivElement>>(
    ({ className, ...props }, ref) => (
        <div ref={ref} className={cn("flex flex-col space-y-1.5 p-6", className)} {...props} />
    )
);
CardHeader.displayName = "CardHeader";

export const CardTitle = forwardRef<HTMLDivElement, HTMLAttributes<HTMLDivElement>>(
    ({ className, ...props }, ref) => (
        <div
            ref={ref}
            className={cn("text-base font-semibold leading-none tracking-tight", className)}
            {...props}
        />
    )
);
CardTitle.displayName = "CardTitle";

export const CardContent = forwardRef<HTMLDivElement, HTMLAttributes<HTMLDivElement>>(
    ({ className, ...props }, ref) => (
        <div ref={ref} className={cn("p-6 pt-0", className)} {...props} />
    )
);
CardContent.displayName = "CardContent";
