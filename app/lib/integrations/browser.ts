"use client";

import { createContext, createElement, useCallback, useContext, useEffect, useState, type ReactNode } from "react";
import type { DataStateKind } from "../../components/data-state";
import type { IntegrationListEnvelope, IntegrationStatus } from "./providers";

type LoadNotice = { kind: "error" | "success"; text: string } | null;
type IntegrationLoadResult = {
  state: DataStateKind;
  integrations: IntegrationStatus[] | null;
  notice: LoadNotice;
};

export type IntegrationStatusResource = {
  state: DataStateKind;
  integrations: IntegrationStatus[];
  refreshing: boolean;
  notice: LoadNotice;
  refresh: () => Promise<DataStateKind>;
  replaceStatus: (status: IntegrationStatus) => void;
};

const IntegrationStatusContext = createContext<IntegrationStatusResource | null>(null);

export function IntegrationStatusProvider({ children }: { children: ReactNode }): ReactNode {
  const resource = useIntegrationStatusResource();
  return createElement(IntegrationStatusContext.Provider, { value: resource }, children);
}

export function useIntegrationStatuses(): IntegrationStatusResource {
  const resource = useContext(IntegrationStatusContext);
  if (!resource) throw new Error("IntegrationStatusProvider is required");
  return resource;
}

function useIntegrationStatusResource(): IntegrationStatusResource {
  const [state, setState] = useState<DataStateKind>("loading");
  const [integrations, setIntegrations] = useState<IntegrationStatus[]>([]);
  const [refreshing, setRefreshing] = useState(false);
  const [notice, setNotice] = useState<LoadNotice>(null);

  const refresh = useCallback(async () => {
    setRefreshing(true);
    setNotice(null);
    const result = await loadIntegrationStatuses();
    setState(result.state);
    if (result.integrations !== null) setIntegrations(result.integrations);
    setNotice(result.notice);
    setRefreshing(false);
    return result.state;
  }, []);

  useEffect(() => {
    let active = true;
    void loadIntegrationStatuses().then((result) => {
      if (!active) return;
      setState(result.state);
      setIntegrations(result.integrations ?? []);
      setNotice(result.notice);
    });
    return () => {
      active = false;
    };
  }, []);

  const replaceStatus = useCallback((status: IntegrationStatus) => {
    setIntegrations((current) => {
      const next = current.map((item) => item.provider === status.provider ? status : item);
      setState(classifyIntegrationState(next));
      return next;
    });
  }, []);

  return { state, integrations, refreshing, notice, refresh, replaceStatus };
}

async function loadIntegrationStatuses(): Promise<IntegrationLoadResult> {
  const controller = new AbortController();
  const timeout = globalThis.setTimeout(() => controller.abort(), 10_000);
  try {
    const response = await fetch("/api/v1/integrations", {
      method: "GET",
      cache: "no-store",
      signal: controller.signal,
    });
    if (response.status === 401 || response.status === 403) {
      return { state: "forbidden", integrations: [], notice: null };
    }
    if (!response.ok) throw new Error("integration status unavailable");
    const envelope = (await response.json()) as IntegrationListEnvelope;
    const integrations = Array.isArray(envelope.data) ? envelope.data : [];
    return {
      state: classifyIntegrationState(integrations),
      integrations,
      notice: null,
    };
  } catch (error) {
    return {
      state: "error",
      integrations: null,
      notice: {
        kind: "error",
        text: error instanceof DOMException && error.name === "AbortError"
          ? "状态刷新超时。保留最后已知状态，请重试。"
          : "无法读取集成状态。保留安全默认值，请重试。",
      },
    };
  } finally {
    globalThis.clearTimeout(timeout);
  }
}

export function classifyIntegrationState(integrations: IntegrationStatus[], now = Date.now()): DataStateKind {
  if (integrations.length === 0) return "empty";
  const active = integrations.filter((integration) => integration.mode !== "disabled");
  if (active.some((integration) => integration.quality === "FAIL")) return "error";
  if (active.some((integration) => isStale(integration, now))) return "stale";
  if (active.length === 0 || integrations.some((integration) => integration.quality === "PARTIAL" || integration.mode === "disabled")) return "partial";
  return "ready";
}

function isStale(integration: IntegrationStatus, now: number): boolean {
  const expiry = integration.capability_expires_at
    ? Date.parse(integration.capability_expires_at)
    : Number.POSITIVE_INFINITY;
  const asOf = integration.as_of ? Date.parse(integration.as_of) : now;
  return expiry <= now || (!Number.isNaN(asOf) && now - asOf > 24 * 60 * 60 * 1000);
}
