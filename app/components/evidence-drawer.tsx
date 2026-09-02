"use client";

import type { RefObject } from "react";

import type { components } from "../lib/generated/schema";
import { Badge } from "./ui/badge";
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from "./ui/sheet";

type Evidence = components["schemas"]["EvidenceRef"];

type Props = {
  evidence: Evidence | null;
  excerpt: string | null;
  open: boolean;
  onClose: () => void;
  returnFocusRef: RefObject<HTMLElement | null>;
};

export function EvidenceDrawer({ evidence, excerpt, open, onClose, returnFocusRef }: Props): React.ReactElement | null {
  if (evidence === null) return null;

  return (
    <Sheet open={open} onOpenChange={(nextOpen) => { if (!nextOpen) onClose(); }}>
      <SheetContent
        closeLabel="关闭证据抽屉"
        onCloseAutoFocus={(event) => {
          // Evidence has multiple triggers outside the controlled Sheet root.
          event.preventDefault();
          if (returnFocusRef.current?.isConnected) returnFocusRef.current.focus();
        }}
      >
        <SheetHeader>
          <Badge variant="secondary">不可变证据</Badge>
          <SheetTitle>证据原文与定位</SheetTitle>
          <SheetDescription>核验来源、可用时间与内容标识。证据位置仅作文本展示。</SheetDescription>
        </SheetHeader>
        <div className="nexus-evidence-body">
          <h3>关联研究摘录</h3>
          <p className="nexus-evidence-excerpt">{excerpt ?? "当前研究摘要未附原文摘录；请按不可变位置与哈希核验源对象。"}</p>
          <h3>来源与定位</h3>
          <dl className="nexus-evidence-facts">
            <div><dt>来源</dt><dd>{evidence.source.toUpperCase()}</dd></div>
            <div><dt>对象 ID</dt><dd><code>{evidence.object_id}</code></dd></div>
            <div><dt>可用时间</dt><dd className="numeric-data">{formatUtc(evidence.available_at)}</dd></div>
            <div><dt>原文位置</dt><dd><code>{evidence.location}</code></dd></div>
            <div><dt>内容哈希</dt><dd><code>{evidence.content_hash}</code></dd></div>
          </dl>
        </div>
        <p className="nexus-evidence-note">页面不直接访问该位置，也不根据位置执行 URL、路径或查询。</p>
      </SheetContent>
    </Sheet>
  );
}

function formatUtc(value: string): string {
  return `${value.replace("T", " ").replace(/(?:\.\d+)?Z$/, "")} UTC`;
}
