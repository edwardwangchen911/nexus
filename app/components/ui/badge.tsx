// Adapted from shadcn/ui (MIT); see NOTICE.md. Status meaning belongs to callers.
import type { ComponentProps } from "react";
import { cva, type VariantProps } from "class-variance-authority";

const badgeVariants = cva("nexus-badge", {
  variants: { variant: { outline: "nexus-badge--outline", secondary: "nexus-badge--secondary" } },
  defaultVariants: { variant: "outline" },
});

export function Badge({ className, variant, ...props }: ComponentProps<"span"> & VariantProps<typeof badgeVariants>): React.ReactElement {
  return <span data-slot="badge" className={badgeVariants({ variant, className })} {...props} />;
}
