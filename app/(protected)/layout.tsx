import { requireNexusPageUser } from "../lib/auth/nexus-user";
import { AppShell } from "../components/app-shell";

export const dynamic = "force-dynamic";

export default async function ProtectedLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  await requireNexusPageUser("/overview");
  return <AppShell>{children}</AppShell>;
}
