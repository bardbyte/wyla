import { useState } from "react";
import { BRAND, TABS, type TabId } from "./lib/copy";
import { useTheme } from "./lib/theme";
import { GraphTab } from "./tabs/Graph";
import { InquiriesTab } from "./tabs/Inquiries";
import { KnowledgeTab } from "./tabs/Knowledge";
import { MetricsTab } from "./tabs/Metrics";
import { ProductsTab } from "./tabs/Products";

export default function App() {
  const [tab, setTab] = useState<TabId>("inquiries");
  const [theme, toggleTheme] = useTheme();

  return (
    <div className="shell">
      <header className="topbar">
        <div className="brand">
          <span className="brand-mark">{BRAND.name}</span>
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
              {t.preview && <sup className="nav-preview">Preview</sup>}
            </button>
          ))}
        </nav>
        <div className="topbar-right">
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
