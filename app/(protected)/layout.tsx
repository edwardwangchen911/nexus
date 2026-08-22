import { requireNexusUser } from "../chatgpt-auth";
import { AppShell } from "../components/app-shell";

export const dynamic = "force-dynamic";

export default async function ProtectedLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  await requireNexusUser("/overview");
  return <AppShell>{children}</AppShell>;
}
