"use client";

import { X } from "lucide-react";
import { useEffect, useRef, type RefObject } from "react";

import type { components } from "../lib/generated/schema";

type Evidence = components["schemas"]["EvidenceRef"];

type Props = {
  evidence: Evidence | null;
  excerpt: string | null;
  open: boolean;
  onClose: () => void;
  returnFocusRef: RefObject<HTMLElement | null>;
};

export function EvidenceDrawer({ evidence, excerpt, open, onClose, returnFocusRef }: Props): React.ReactElement | null {
  const closeRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    if (!open) return;
    closeRef.current?.focus();
  }, [open]);

  if (!open || evidence === null) return null;

  function close(): void {
    onClose();
    globalThis.queueMicrotask(() => returnFocusRef.current?.focus());
  }

  function onKeyDown(event: React.KeyboardEvent<HTMLDialogElement>): void {
    if (event.key === "Escape") {
      event.preventDefault();
      close();
      return;
    }
    if (event.key !== "Tab") return;
    const focusable = [...event.currentTarget.querySelectorAll<HTMLElement>("button:not([disabled]), [href], [tabindex]:not([tabindex='-1'])")];
    if (focusable.length === 0) return;
    const first = focusable[0];
    const last = focusable[focusable.length - 1];
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first.focus();
    }
  }

  return (
    <div className="evidence-drawer" role="presentation">
      <dialog
        open
        className="evidence-drawer__panel"
        aria-modal="true"
        aria-labelledby="evidence-drawer-title"
        onKeyDown={onKeyDown}
      >
        <header>
          <div>
            <p className="eyebrow">EVIDENCE / IMMUTABLE</p>
            <h2 id="evidence-drawer-title">证据原文与定位</h2>
          </div>
          <button ref={closeRef} className="button button--quiet" type="button" onClick={close} aria-label="关闭证据抽屉">
            <X aria-hidden="true" size={18} />关闭
          </button>
        </header>
        <p className="evidence-drawer__excerpt">{excerpt ?? "当前研究摘要未附原文摘录；请按不可变位置与哈希核验源对象。"}</p>
        <dl className="evidence-drawer__facts">
          <div><dt>来源</dt><dd>{evidence.source.toUpperCase()}</dd></div>
          <div><dt>对象 ID</dt><dd>{evidence.object_id}</dd></div>
          <div><dt>可用时间</dt><dd className="numeric-data">{formatUtc(evidence.available_at)}</dd></div>
          <div><dt>原文位置</dt><dd>{evidence.location}</dd></div>
          <div><dt>内容哈希</dt><dd className="numeric-data">{evidence.content_hash}</dd></div>
        </dl>
        <p className="evidence-drawer__note">页面不直接访问该位置，也不根据位置执行 URL、路径或查询。</p>
      </dialog>
    </div>
  );
}

function formatUtc(value: string): string {
  return `${value.replace("T", " ").replace(/(?:\.\d+)?Z$/, "")} UTC`;
}
