import { WitnessDrawer } from "./components/WitnessDrawer";
import { BRAND, TABS } from "./lib/copy";
import { NavProvider, useNav } from "./lib/nav";
import { useTheme } from "./lib/theme";
import { AskTab } from "./tabs/Ask";
import { GraphTab } from "./tabs/Graph";
import { KnowledgeTab } from "./tabs/Knowledge";
import { ProductsTab } from "./tabs/Products";

export default function App() {
  return (
    <NavProvider>
      <Shell />
    </NavProvider>
  );
}

function Shell() {
  const nav = useNav();
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
              aria-selected={nav.tab === t.id}
              onClick={() => nav.go(t.id)}
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
        {/* Ask stays mounted so a conversation and any in-flight stream
            survive tab hops to Data products / Graph / Knowledge */}
        <div className="tab-panel"
          style={{ display: nav.tab === "ask" ? "flex" : "none" }}>
          <AskTab />
        </div>
        {nav.tab === "products" && <ProductsTab />}
        {nav.tab === "graph" && <GraphTab />}
        {nav.tab === "knowledge" && <KnowledgeTab />}
      </main>

      {nav.evidenceRef && (
        <WitnessDrawer refUri={nav.evidenceRef}
          onClose={nav.closeEvidence} />
      )}
    </div>
  );
}
