/** POST /chat and yield each ConsoleEvent as it streams.
 * fetch + ReadableStream (EventSource cannot POST). */

import type { ConsoleEvent } from "./types";

export async function* streamChat(
  message: string,
  conversationId: string,
  signal?: AbortSignal,
): AsyncGenerator<ConsoleEvent> {
  const resp = await fetch("/chat", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ message, conversation_id: conversationId }),
    signal,
  });
  if (!resp.ok || !resp.body)
    throw new Error(`chat stream failed (${resp.status})`);

  const reader = resp.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    let idx: number;
    while ((idx = buffer.indexOf("\n\n")) >= 0) {
      const frame = buffer.slice(0, idx);
      buffer = buffer.slice(idx + 2);
      const line = frame.split("\n").find((l) => l.startsWith("data: "));
      if (!line) continue;
      try {
        yield JSON.parse(line.slice(6)) as ConsoleEvent;
      } catch {
        /* skip malformed frame — the stream goes on */
      }
    }
  }
}
