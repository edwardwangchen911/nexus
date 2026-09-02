"use client";

// Adapted from shadcn/ui (MIT); see NOTICE.md. Only the used right-hand sheet is exposed.
import * as SheetPrimitive from "@radix-ui/react-dialog";
import { clsx } from "clsx";
import { X } from "lucide-react";
import type { ComponentProps } from "react";

import { Button } from "./button";

export const Sheet = SheetPrimitive.Root;

export function SheetContent({ className, children, closeLabel = "关闭详情", ...props }: ComponentProps<typeof SheetPrimitive.Content> & { closeLabel?: string }): React.ReactElement {
  return <SheetPrimitive.Portal>
    <SheetPrimitive.Overlay data-slot="sheet-overlay" className="nexus-sheet-overlay" />
    <SheetPrimitive.Content data-slot="sheet-content" className={clsx("nexus-sheet", className)} {...props}>
      {children}
      <SheetPrimitive.Close asChild>
        <Button variant="ghost" size="icon" className="nexus-sheet__close" aria-label={closeLabel}>
          <X aria-hidden="true" size={18} />
        </Button>
      </SheetPrimitive.Close>
    </SheetPrimitive.Content>
  </SheetPrimitive.Portal>;
}

export function SheetHeader({ className, ...props }: ComponentProps<"div">): React.ReactElement {
  return <div data-slot="sheet-header" className={clsx("nexus-sheet__header", className)} {...props} />;
}

export function SheetTitle({ className, ...props }: ComponentProps<typeof SheetPrimitive.Title>): React.ReactElement {
  return <SheetPrimitive.Title data-slot="sheet-title" className={clsx("nexus-sheet__title", className)} {...props} />;
}

export function SheetDescription({ className, ...props }: ComponentProps<typeof SheetPrimitive.Description>): React.ReactElement {
  return <SheetPrimitive.Description data-slot="sheet-description" className={clsx("nexus-sheet__description", className)} {...props} />;
}
