import { permanentRedirect } from "next/navigation";

export default function LegacyFosunHoldingsPage(): never {
  permanentRedirect("/portfolio?source=fosun");
}
