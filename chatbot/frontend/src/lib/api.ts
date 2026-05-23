import type { SemanticExample, SessionDetail, SessionSummary } from "./types";

const jsonHeaders = {
  "Content-Type": "application/json",
};

export async function createSession(): Promise<SessionSummary> {
  const response = await fetch("/api/chat/sessions", {
    method: "POST",
    credentials: "include",
  });
  const payload = await response.json();
  return payload.session as SessionSummary;
}

export async function listSessions(): Promise<SessionSummary[]> {
  const response = await fetch("/api/chat/sessions", { credentials: "include" });
  return response.json();
}

export async function getSession(sessionId: string): Promise<SessionDetail> {
  const response = await fetch(`/api/chat/sessions/${sessionId}`, { credentials: "include" });
  if (!response.ok) {
    throw new Error("Failed to load session");
  }
  return response.json();
}

export async function getExamples(): Promise<SemanticExample[]> {
  const response = await fetch("/api/semantic/examples", { credentials: "include" });
  return response.json();
}

type StreamHandlers = {
  onStatus: (value: string) => void;
  onSql: (sql: string) => void;
  onComplete: (message: unknown) => void;
};

export async function streamChatMessage(
  sessionId: string,
  message: string,
  handlers: StreamHandlers,
): Promise<void> {
  const response = await fetch(`/api/chat/sessions/${sessionId}/messages`, {
    method: "POST",
    headers: jsonHeaders,
    credentials: "include",
    body: JSON.stringify({ message }),
  });
  if (!response.ok || !response.body) {
    throw new Error("The assistant is unavailable.");
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const chunks = buffer.split("\n\n");
    buffer = chunks.pop() ?? "";
    for (const chunk of chunks) {
      const event = parseSseChunk(chunk);
      if (!event) continue;
      if (event.event === "status") {
        handlers.onStatus(JSON.parse(event.data).step);
      } else if (event.event === "sql") {
        handlers.onSql(JSON.parse(event.data).sql);
      } else if (event.event === "completed") {
        handlers.onComplete(JSON.parse(event.data));
      }
    }
  }
}

export async function sendFeedback(messageId: string, rating: "up" | "down"): Promise<void> {
  await fetch(`/api/chat/messages/${messageId}/feedback`, {
    method: "POST",
    headers: jsonHeaders,
    credentials: "include",
    body: JSON.stringify({ rating }),
  });
}

function parseSseChunk(chunk: string): { event: string; data: string } | null {
  const lines = chunk.split("\n");
  let event = "message";
  let data = "";
  for (const line of lines) {
    if (line.startsWith("event:")) event = line.slice(6).trim();
    if (line.startsWith("data:")) data += line.slice(5).trim();
  }
  return data ? { event, data } : null;
}
