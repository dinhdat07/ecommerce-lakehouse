import { useEffect, useState, useRef } from "react";
import { Composer } from "./features/chat/Composer";
import { MessageCard } from "./features/chat/MessageCard";
import { SessionRail } from "./components/SessionRail";
import { StarterPrompts } from "./components/StarterPrompts";
import { createSession, getExamples, getSession, listSessions, streamChatMessage } from "./lib/api";
import type { ChatMessage, SemanticExample, SessionDetail, SessionSummary } from "./lib/types";

function App() {
  const [sessions, setSessions] = useState<SessionSummary[]>([]);
  const [activeSessionId, setActiveSessionId] = useState<string | null>(null);
  const [sessionDetail, setSessionDetail] = useState<SessionDetail | null>(null);
  const [examples, setExamples] = useState<SemanticExample[]>([]);
  const [status, setStatus] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  };

  useEffect(() => {
    scrollToBottom();
  }, [sessionDetail?.messages, status]);

  useEffect(() => {
    void bootstrap();
  }, []);

  async function bootstrap() {
    try {
      const [loadedSessions, loadedExamples] = await Promise.all([listSessions(), getExamples()]);
      setSessions(loadedSessions);
      setExamples(loadedExamples);
      if (loadedSessions[0]) {
        await openSession(loadedSessions[0].id);
      } else {
        await handleCreateSession();
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to bootstrap the demo.");
    }
  }

  async function handleCreateSession() {
    const session = await createSession();
    setSessions((current) => [session, ...current]);
    setActiveSessionId(session.id);
    setSessionDetail({ ...session, messages: [] });
    setError(null);
  }

  async function openSession(sessionId: string) {
    const detail = await getSession(sessionId);
    setActiveSessionId(sessionId);
    setSessionDetail(detail);
    setError(null);
  }

  async function submitMessage(message: string) {
    if (!activeSessionId) return;
    const userMessage: ChatMessage = {
      id: `local-${Date.now()}`,
      role: "user",
      text: message,
      created_at: new Date().toISOString(),
      columns: [],
      rows: [],
      tables_used: [],
      warnings: [],
    };
    setSessionDetail((current) =>
      current ? { ...current, messages: [...current.messages, userMessage] } : current,
    );
    setBusy(true);
    setStatus("planning");
    setError(null);
    try {
      await streamChatMessage(activeSessionId, message, {
        onStatus: (nextStatus) => setStatus(nextStatus),
        onSql: () => {}, // Ignore SQL updates
        onComplete: (payload) => {
          const completed = payload as ChatMessage;
          setSessionDetail((current) =>
            current ? { ...current, messages: [...current.messages, completed] } : current,
          );
        },
      });
      const refreshedSessions = await listSessions();
      setSessions(refreshedSessions);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Message failed.");
    } finally {
      setBusy(false);
      setStatus(null);
    }
  }

  return (
    <div className="flex h-screen overflow-hidden bg-shell text-ink font-body">
      {/* Sidebar - fixed */}
      <div className="w-[280px] flex-shrink-0 h-full overflow-y-auto border-r border-ink/10 bg-white hidden md:block">
        <SessionRail
          sessions={sessions}
          activeSessionId={activeSessionId}
          onSelect={(id) => void openSession(id)}
          onCreate={() => void handleCreateSession()}
        />
      </div>

      {/* Main Content Area */}
      <main className="flex-1 flex flex-col h-full w-full bg-shell">
        {/* Scrollable messages container */}
        <div className="flex-1 overflow-y-auto p-4 md:p-8">
          <div className="max-w-3xl mx-auto space-y-6 pb-2">
            {!sessionDetail?.messages.length && (
              <div className="text-center py-12 px-4">
                <h1 className="font-display text-4xl mb-4 font-semibold text-ink">Data Insights AI</h1>
                <p className="text-ink/60 mb-8 max-w-lg mx-auto leading-relaxed">
                  Ask revenue, conversion, retention, product, category, or RFM questions.
                </p>
                <StarterPrompts examples={examples} onSelect={(question) => void submitMessage(question)} />
              </div>
            )}

            <div className="space-y-6">
              {sessionDetail?.messages.map((message) => (
                <MessageCard key={message.id} message={message} />
              ))}
              
              {/* Show loading indicator when busy */}
              {busy && status && (
                <div className="flex items-center space-x-2 text-ink/50 p-4 bg-white/50 rounded-2xl w-fit animate-pulse border border-ink/5">
                  <div className="w-2 h-2 bg-accent/70 rounded-full"></div>
                  <div className="w-2 h-2 bg-accent/70 rounded-full"></div>
                  <div className="w-2 h-2 bg-accent/70 rounded-full"></div>
                  <span className="ml-2 text-sm font-medium">{status.replace("_", " ")}...</span>
                </div>
              )}
              <div ref={messagesEndRef} className="h-1" />
            </div>

            {error && (
              <div className="rounded-2xl border border-red-500/30 bg-red-50 px-4 py-3 text-sm text-red-600 mt-4">
                {error}
              </div>
            )}
          </div>
        </div>

        {/* Composer section - now static in flex flow so it never overlaps */}
        <div className="shrink-0 p-4 bg-shell border-t border-ink/5">
          <div className="max-w-3xl mx-auto">
            <Composer onSubmit={submitMessage} disabled={busy} status={null} />
          </div>
        </div>
      </main>
    </div>
  );
}

export default App;
