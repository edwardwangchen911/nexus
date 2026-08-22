import { requireNexusPageUser } from "../lib/auth/nexus-user";
import { AppShell } from "../components/app-shell";
import { GlobalSystemStatus } from "../components/global-system-status";

export const dynamic = "force-dynamic";

export default async function ProtectedLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  await requireNexusPageUser("/overview");
  return <AppShell><GlobalSystemStatus />{children}</AppShell>;
}
