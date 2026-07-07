import { useEffect, useState } from "react";
import { api } from "./lib/api";
import { BRAND, CONFIG, TABS, type TabId } from "./lib/copy";
import { useTheme } from "./lib/theme";
import type { AppConfig } from "./lib/types";
import { GraphTab } from "./tabs/Graph";
import { InquiriesTab } from "./tabs/Inquiries";
import { KnowledgeTab } from "./tabs/Knowledge";
import { MetricsTab } from "./tabs/Metrics";
import { ProductsTab } from "./tabs/Products";

export default function App() {
  const [tab, setTab] = useState<TabId>("inquiries");
  const [theme, toggleTheme] = useTheme();
  const [config, setConfig] = useState<AppConfig | null>(null);

  useEffect(() => {
    api.config().then(setConfig).catch(() => setConfig(null));
  }, []);

  const runnerLabel = config
    ? config.runner === "ADKRunner"
      ? `${CONFIG.live} · ${config.model}`
      : CONFIG.scripted
    : "…";

  return (
    <div className="shell">
      <header className="topbar">
        <div className="brand">
          <span className="brand-mark">{BRAND.name}</span>
          <span className="brand-sub">{BRAND.sub}</span>
        </div>
        <nav className="tabnav" role="tablist" aria-label="Sections">
          {TABS.map((t) => (
            <button
              key={t.id}
              role="tab"
              className="tab"
              aria-selected={tab === t.id}
              onClick={() => setTab(t.id)}
            >
              {t.label}
            </button>
          ))}
        </nav>
        <div className="topbar-right">
          <span className="env-badge" title={
            config
              ? `Graph: ${config.graph.live ? "live snapshot" : "sample"} · ${config.graph.path}`
              : "Connecting to the console server…"
          }>
            <span
              className={`live-dot ${config?.graph.live ? "" : "sample"}`}
              aria-hidden
            />
            {runnerLabel}
          </span>
          <button
            type="button"
            className="icon-btn"
            onClick={toggleTheme}
            aria-label={`Switch to ${theme === "dark" ? "light" : "dark"} mode`}
            title={`Switch to ${theme === "dark" ? "light" : "dark"} mode`}
          >
            {theme === "dark" ? "☀" : "☾"}
          </button>
        </div>
      </header>

      <main className="main">
        {tab === "inquiries" && <InquiriesTab />}
        {tab === "products" && <ProductsTab />}
        {tab === "metrics" && <MetricsTab />}
        {tab === "graph" && <GraphTab />}
        {tab === "knowledge" && <KnowledgeTab />}
      </main>
    </div>
  );
}
