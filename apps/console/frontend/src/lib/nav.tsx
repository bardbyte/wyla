/** The interconnection spine: one navigation context every tab can
 * call, so a table on any surface is a door to every other surface —
 * evidence, the graph thread, or a pre-filled question in chat. */

import {
  createContext, useCallback, useContext, useMemo, useState,
} from "react";
import type { ReactNode } from "react";
import type { TabId } from "./copy";

export interface Handoff {
  text: string;
  send?: boolean;
  autoPin?: boolean;
}

interface Nav {
  tab: TabId;
  go(tab: TabId): void;
  askAbout(text: string, opts?: { send?: boolean; autoPin?: boolean }): void;
  goToGraph(table: string): void;
  openEvidence(ref: string): void;
  evidenceRef: string | null;
  closeEvidence(): void;
  handoff: Handoff | null;
  clearHandoff(): void;
  graphAnchor: string | null;
  clearGraphAnchor(): void;
  /* the live-traversal channel: Ask reports which graph objects the
   * agent touches (table names / synapse:// refs → last-touch ms);
   * any graph surface lights them up. */
  activity: Record<string, number>;
  reportActivity(keys: string[]): void;
  clearActivity(): void;
  agentBusy: boolean;
  setAgentBusy(busy: boolean): void;
  traversalVerb: string;
  setTraversalVerb(v: string): void;
  /* anticipation: while the user TYPES, nodes matching the draft
   * question glimmer — the space is listening (mockup treatment B/C) */
  listening: string[];
  setListening(keys: string[]): void;
}

const NavContext = createContext<Nav | null>(null);

export function NavProvider({ children }: { children: ReactNode }) {
  const [tab, setTab] = useState<TabId>("ask");
  const [handoff, setHandoff] = useState<Handoff | null>(null);
  const [graphAnchor, setGraphAnchor] = useState<string | null>(null);
  const [evidenceRef, setEvidenceRef] = useState<string | null>(null);
  const [activity, setActivity] = useState<Record<string, number>>({});
  const [agentBusy, setAgentBusy] = useState(false);
  const [traversalVerb, setTraversalVerb] = useState("");
  const [listening, setListening] = useState<string[]>([]);

  const go = useCallback((t: TabId) => setTab(t), []);
  const askAbout = useCallback(
    (text: string, opts?: { send?: boolean; autoPin?: boolean }) => {
      setHandoff({ text, send: opts?.send, autoPin: opts?.autoPin });
      setTab("ask");
    }, []);
  const goToGraph = useCallback((table: string) => {
    setGraphAnchor(table);
    setTab("graph");
  }, []);
  const openEvidence = useCallback((ref: string) => setEvidenceRef(ref), []);
  const closeEvidence = useCallback(() => setEvidenceRef(null), []);
  const clearHandoff = useCallback(() => setHandoff(null), []);
  const clearGraphAnchor = useCallback(() => setGraphAnchor(null), []);
  const reportActivity = useCallback((keys: string[]) => {
    if (!keys.length) return;
    const now = Date.now();
    setActivity((prev) => {
      const next = { ...prev };
      for (const k of keys) next[k.toLowerCase()] = now;
      return next;
    });
  }, []);
  const clearActivity = useCallback(() => setActivity({}), []);

  const value = useMemo<Nav>(() => ({
    tab, go, askAbout, goToGraph, openEvidence, evidenceRef,
    closeEvidence, handoff, clearHandoff, graphAnchor, clearGraphAnchor,
    activity, reportActivity, clearActivity, agentBusy, setAgentBusy,
    traversalVerb, setTraversalVerb, listening, setListening,
  }), [tab, go, askAbout, goToGraph, openEvidence, evidenceRef,
       closeEvidence, handoff, clearHandoff, graphAnchor,
       clearGraphAnchor, activity, reportActivity, clearActivity,
       agentBusy, traversalVerb, listening]);

  return <NavContext.Provider value={value}>{children}</NavContext.Provider>;
}

export function useNav(): Nav {
  const nav = useContext(NavContext);
  if (!nav) throw new Error("useNav outside NavProvider");
  return nav;
}
