// Adapted from shadcn/ui (MIT); see NOTICE.md. Native buttons only in this slice.
import type { ComponentProps } from "react";
import { cva, type VariantProps } from "class-variance-authority";

const buttonVariants = cva("nexus-button", {
  variants: {
    variant: { default: "nexus-button--primary", outline: "nexus-button--outline", ghost: "nexus-button--ghost" },
    size: { default: "nexus-button--default", sm: "nexus-button--sm", icon: "nexus-button--icon" },
  },
  defaultVariants: { variant: "default", size: "default" },
});

export function Button({ className, variant, size, type = "button", ...props }: ComponentProps<"button"> & VariantProps<typeof buttonVariants>): React.ReactElement {
  return <button data-slot="button" type={type} className={buttonVariants({ variant, size, className })} {...props} />;
}
